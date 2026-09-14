"""Synthetic Paystack/Flutterwave-shaped payment events.

The point of this generator is NOT to make pretty data. It is to manufacture,
deterministically, the four things that make payments data genuinely hard and
that a clean synthetic dataset would hide:

  1. Multiple events per transaction. A charge emits pending, then a terminal
     status. Downstream must collapse them to one row, latest wins.
  2. Duplicates. At-least-once delivery means the same event_id shows up twice.
  3. Out-of-order arrival. The terminal event sometimes lands before the
     pending event it supersedes, so ordering by arrival is wrong.
  4. Missing settlements. A few percent of successful charges never settle.
     Finding those is the actual business question this pipeline answers.

Everything here is stdlib only and seeded, so the tests can assert on exact
counts without Kafka, pyarrow, or a network.
"""
from __future__ import annotations

import argparse
import json
import random
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

UTC = timezone.utc

from naijapay.schemas import (
    BANKS,
    FAILURE_REASONS,
    MERCHANT_CATEGORIES,
    Channel,
    Gateway,
    Status,
)

# Per-channel behaviour, loosely modelled on published Nigerian PSP figures:
# USSD and QR fail more than card, bank transfer is slowest to confirm.
CHANNEL_PROFILE: dict[str, dict[str, float]] = {
    Channel.CARD:          {"weight": 0.46, "success": 0.88, "confirm_secs": 12},
    Channel.BANK_TRANSFER: {"weight": 0.31, "success": 0.94, "confirm_secs": 90},
    Channel.USSD:          {"weight": 0.15, "success": 0.79, "confirm_secs": 45},
    Channel.QR:            {"weight": 0.08, "success": 0.83, "confirm_secs": 20},
}

DUPLICATE_RATE = 0.02       # at-least-once redelivery
OUT_OF_ORDER_RATE = 0.05    # terminal event overtakes its own pending event
REVERSAL_RATE = 0.012       # success later reversed (chargeback / failed payout)
UNSETTLED_RATE = 0.031      # successful but never settled: the reconciliation gap
USD_RATE = 0.04             # share of card charges denominated in USD

NGN_PER_USD = 1_615.0       # static on purpose; a real pipeline joins an fx table


@dataclass(frozen=True)
class Merchant:
    merchant_id: str
    name: str
    category: str
    settlement_lag_days: int


def build_merchants(rng: random.Random, count: int = 40) -> list[Merchant]:
    merchants = []
    for i in range(count):
        cat = rng.choice(MERCHANT_CATEGORIES)
        merchants.append(
            Merchant(
                merchant_id=f"MRC_{i:04d}",
                name=f"{cat.replace('_', ' ').title()} Merchant {i:03d}",
                category=cat,
                # T+1 for most, T+2 for a minority. This is what makes the
                # reconciliation aging buckets non-trivial.
                settlement_lag_days=1 if rng.random() < 0.78 else 2,
            )
        )
    return merchants


def _pick_channel(rng: random.Random) -> str:
    channels = list(CHANNEL_PROFILE)
    weights = [CHANNEL_PROFILE[c]["weight"] for c in channels]
    return rng.choices(channels, weights=weights, k=1)[0]


def _amount_kobo(rng: random.Random, category: str) -> int:
    """Log-normal-ish amounts, shifted by category.

    Naira amounts are heavily right-skewed: lots of small airtime-sized charges,
    a long tail of large ones. A uniform distribution would make every
    percentile metric downstream meaningless.
    """
    base = {
        "utilities": 8.4, "food_delivery": 8.2, "digital_services": 8.0,
        "ecommerce": 9.2, "logistics": 8.6, "education": 10.4,
        "travel": 10.8, "healthcare": 9.6,
    }.get(category, 9.0)
    naira = rng.lognormvariate(base, 0.85)
    return int(min(max(naira, 100.0), 8_000_000.0) * 100)


def _fee_kobo(amount_kobo: int, channel: str) -> int:
    """Paystack-shaped pricing: 1.5% capped at 2,000 NGN, plus 100 NGN over 2,500.

    Integer arithmetic throughout. Fees computed in floats and rounded later is
    a reliable way to be off by a few kobo per million transactions, which is
    exactly the kind of thing reconciliation is supposed to catch.
    """
    if channel == Channel.BANK_TRANSFER:
        return min(1_000_00, max(50_00, amount_kobo * 5 // 1000))
    fee = amount_kobo * 15 // 1000
    if amount_kobo > 2_500_00:
        fee += 100_00
    return min(fee, 2_000_00)


def generate_events(
    n_transactions: int,
    days: int,
    seed: int,
    end_date: date | None = None,
) -> tuple[list[dict], list[dict]]:
    """Return (transaction_events, settlement_events).

    Transaction events outnumber transactions: each charge emits at least two.
    """
    rng = random.Random(seed)
    merchants = build_merchants(rng)
    end = end_date or datetime.now(UTC).date()
    window_start = datetime.combine(end - timedelta(days=days), datetime.min.time(), tzinfo=UTC)

    tx_events: list[dict] = []
    settlements: list[dict] = []

    for _ in range(n_transactions):
        merchant = rng.choice(merchants)
        channel = _pick_channel(rng)
        profile = CHANNEL_PROFILE[channel]

        # Traffic is not uniform across the day: a broad afternoon/evening peak.
        day_offset = rng.uniform(0, days)
        hour_bias = rng.choices(
            range(24),
            weights=[1, 1, 1, 1, 1, 2, 4, 7, 9, 10, 11, 12, 13, 13, 12, 12, 13, 15, 16, 14, 11, 7, 4, 2],
            k=1,
        )[0]
        created = window_start + timedelta(
            days=int(day_offset),
            hours=hour_bias,
            minutes=rng.randint(0, 59),
            seconds=rng.randint(0, 59),
        )

        ref = f"NP_{uuid.UUID(int=rng.getrandbits(128)).hex[:18]}"
        amount = _amount_kobo(rng, merchant.category)
        currency = "USD" if (channel == Channel.CARD and rng.random() < USD_RATE) else "NGN"
        fx = NGN_PER_USD * rng.uniform(0.985, 1.015) if currency == "USD" else None
        if currency == "USD":
            amount = max(100, amount // int(NGN_PER_USD))
        fee = _fee_kobo(amount, channel)
        bank = rng.choice(BANKS)[0] if channel != Channel.CARD else None
        gateway = rng.choice(Gateway.ALL)
        customer = f"CUS_{rng.randrange(10**6):06d}"

        def base_event(status: str, ts: datetime, reason: str | None = None) -> dict:
            return {
                "event_id": str(uuid.UUID(int=rng.getrandbits(128))),
                "event_ts": ts.isoformat(),
                "transaction_ref": ref,
                "merchant_id": merchant.merchant_id,
                "customer_id": customer,
                "gateway": gateway,
                "channel": channel,
                "bank_code": bank,
                "status": status,
                "amount_kobo": amount,
                "fee_kobo": fee,
                "currency": currency,
                "fx_rate_to_ngn": fx,
                "failure_reason": reason,
                "created_at": created.isoformat(),
                "updated_at": ts.isoformat(),
            }

        # 1. pending
        pending = base_event(Status.PENDING, created)

        # 2. terminal
        settled_ok = rng.random() < profile["success"]
        confirm = created + timedelta(
            seconds=profile["confirm_secs"] * rng.uniform(0.4, 3.0)
        )
        if settled_ok:
            terminal = base_event(Status.SUCCESS, confirm)
        else:
            terminal = base_event(Status.FAILED, confirm, rng.choice(FAILURE_REASONS))

        pair = [pending, terminal]
        # 3. out-of-order arrival: emit terminal first
        if rng.random() < OUT_OF_ORDER_RATE:
            pair.reverse()
        tx_events.extend(pair)

        # 4. late reversal of an earlier success
        reversed_later = False
        if settled_ok and rng.random() < REVERSAL_RATE:
            rev_ts = confirm + timedelta(hours=rng.uniform(6, 96))
            tx_events.append(base_event(Status.REVERSED, rev_ts, "chargeback"))
            reversed_later = True

        # 5. duplicate redelivery of a random event in this transaction
        if rng.random() < DUPLICATE_RATE:
            tx_events.append(dict(rng.choice(pair)))

        # 6. settlement, for successes that were not reversed and did not fall
        #    into the unsettled gap
        if settled_ok and not reversed_later and rng.random() > UNSETTLED_RATE:
            sdate = (confirm + timedelta(days=merchant.settlement_lag_days)).date()
            settled_at = datetime.combine(
                sdate, datetime.min.time(), tzinfo=UTC
            ) + timedelta(hours=rng.uniform(9, 17))
            gross = amount if currency == "NGN" else int(amount * (fx or NGN_PER_USD))
            gross_fee = fee if currency == "NGN" else int(fee * (fx or NGN_PER_USD))
            settlements.append(
                {
                    "event_id": str(uuid.UUID(int=rng.getrandbits(128))),
                    "event_ts": settled_at.isoformat(),
                    "settlement_id": f"STL_{uuid.UUID(int=rng.getrandbits(128)).hex[:14]}",
                    "merchant_id": merchant.merchant_id,
                    "transaction_ref": ref,
                    "gross_kobo": gross,
                    "fee_kobo": gross_fee,
                    "net_kobo": gross - gross_fee,
                    "currency": "NGN",
                    "settlement_date": sdate.isoformat(),
                    "settled_at": settled_at.isoformat(),
                }
            )

    # Shuffle so consumers cannot rely on emission order.
    rng.shuffle(tx_events)
    rng.shuffle(settlements)
    return tx_events, settlements


def publish(events: list[dict], topic: str, bootstrap: str, key_field: str) -> int:
    """Publish to Kafka, keyed so all events for one reference share a partition.

    Keying matters: without it, two events for the same transaction can land on
    different partitions and any per-key ordering guarantee is gone.
    """
    from confluent_kafka import Producer

    producer = Producer(
        {
            "bootstrap.servers": bootstrap,
            "linger.ms": 50,
            "batch.size": 64 * 1024,
            "compression.type": "lz4",
            "enable.idempotence": True,
            # With idempotence enabled, librdkafka must complete a metadata
            # fetch AND allocate a Producer ID (PID) from the broker before
            # any message can be queued. Until that handshake is done the
            # internal queue capacity is effectively zero, so the very first
            # produce() call raises BufferError instantly. Pre-warming via
            # an initial poll() lets the handshake complete before we start
            # sending. 100 k messages is well above our 50 k batch ceiling.
            "queue.buffering.max.messages": 100_000,
        }
    )

    # Pre-warm: block up to 3 s so the broker connection, metadata fetch, and
    # idempotent PID registration all finish before we touch the produce loop.
    producer.poll(3)

    delivered = 0
    failures: list[str] = []

    def _cb(err, _msg):
        nonlocal delivered
        if err is not None:
            failures.append(str(err))
        else:
            delivered += 1

    for i, ev in enumerate(events):
        while True:
            try:
                producer.produce(
                    topic,
                    key=ev[key_field].encode(),
                    value=json.dumps(ev, separators=(",", ":")).encode(),
                    on_delivery=_cb,
                )
                break
            except BufferError:
                # poll() drains delivered callbacks and frees queue slots.
                # Guard with its own try/except: poll() itself can raise
                # BufferError when the queue is still saturated, which would
                # otherwise escape the retry loop.
                try:
                    producer.poll(0.5)
                except BufferError:
                    pass
        # Poll every 100 messages (not 1000) to keep the in-flight window
        # drained and avoid hitting queue limits mid-batch.
        if i % 100 == 0:
            producer.poll(0)
    producer.flush(120)

    if failures:
        raise RuntimeError(f"{len(failures)} deliveries failed, first: {failures[0]}")
    return delivered


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--count", type=int, default=50_000)
    ap.add_argument("--days", type=int, default=14)
    ap.add_argument("--seed", type=int, default=20260909)
    ap.add_argument("--bootstrap", default=None, help="Kafka bootstrap; omit to write JSONL")
    ap.add_argument("--out-dir", default=None, help="write JSONL here instead of Kafka")
    args = ap.parse_args()

    tx, stl = generate_events(args.count, args.days, args.seed)
    print(f"generated {len(tx)} transaction events and {len(stl)} settlement events")

    if args.out_dir:
        import pathlib

        d = pathlib.Path(args.out_dir)
        d.mkdir(parents=True, exist_ok=True)
        for name, rows in (("transactions", tx), ("settlements", stl)):
            path = d / f"{name}.jsonl"
            with path.open("w") as fh:
                for r in rows:
                    fh.write(json.dumps(r, separators=(",", ":")) + "\n")
            print(f"wrote {path} ({len(rows)} rows)")
        return

    from naijapay.config import get_settings

    s = get_settings()
    bootstrap = args.bootstrap or s.kafka_bootstrap
    n1 = publish(tx, s.topic_transactions, bootstrap, "transaction_ref")
    n2 = publish(stl, s.topic_settlements, bootstrap, "transaction_ref")
    print(f"published {n1} -> {s.topic_transactions}, {n2} -> {s.topic_settlements}")


if __name__ == "__main__":
    main()
