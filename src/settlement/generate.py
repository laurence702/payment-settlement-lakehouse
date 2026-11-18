"""Generate deterministic synthetic Paystack/Flutterwave-shaped payment events.

The dataset includes payment-data conditions that reconciliation must handle:

  1. Multiple events per transaction. A charge emits pending, then a terminal
     status. Downstream must collapse them to one row.
  2. Duplicates. At-least-once delivery means the same event_id shows up twice.
  3. Out-of-order arrival. The terminal event sometimes lands before the
     pending event it supersedes, so ordering by arrival is wrong.
  4. Missing settlements. A few percent of successful charges never settle.
     The reconciliation mart identifies their expected outstanding amount.

Everything here is stdlib only and seeded, so the tests can assert on exact
counts without Kafka, pyarrow, or a network.

v2 / 1M-transaction scale
--------------------------
At 1M transactions generate_events() would build ~2M dicts (~1.2 GB of Python
objects) in a single list before returning. iter_chunks() avoids that by
yielding (tx_batch, stl_batch) in configurable chunks so peak RSS stays
bounded regardless of total volume. Each chunk is seeded deterministically
(base_seed + chunk_start) so a full run is reproducible: running the same
--count with the same --seed always produces the same Kafka stream.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import random
import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

from settlement.schemas import (
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
    Channel.CARD: {"weight": 0.46, "success": 0.88, "confirm_secs": 12},
    Channel.BANK_TRANSFER: {"weight": 0.31, "success": 0.94, "confirm_secs": 90},
    Channel.USSD: {"weight": 0.15, "success": 0.79, "confirm_secs": 45},
    Channel.QR: {"weight": 0.08, "success": 0.83, "confirm_secs": 20},
}

DUPLICATE_RATE = 0.02  # at-least-once redelivery
OUT_OF_ORDER_RATE = 0.05  # terminal event overtakes its own pending event
REVERSAL_RATE = 0.012  # success later reversed (chargeback / failed payout)
UNSETTLED_RATE = 0.031  # successful but never settled: the reconciliation gap
USD_RATE = 0.04  # share of card charges denominated in USD
PARTIAL_SETTLEMENT_RATE = 0.006
SHORT_PAYMENT_RATE = 0.008
OVERPAYMENT_RATE = 0.004

NGN_PER_USD = 1_615.0  # static on purpose; a real pipeline joins an fx table


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
        "utilities": 8.4,
        "food_delivery": 8.2,
        "digital_services": 8.0,
        "ecommerce": 9.2,
        "logistics": 8.6,
        "education": 10.4,
        "travel": 10.8,
        "healthcare": 9.6,
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
    settlement_source_delay_days: int = 0,
) -> tuple[list[dict], list[dict]]:
    """Return (transaction_events, settlement_events).

    Transaction events outnumber transactions: each charge emits at least two.
    """
    rng = random.Random(seed)
    merchants = build_merchants(rng)
    end = end_date or datetime.now(UTC).date()
    source_watermark = datetime.combine(
        end + timedelta(days=2 - settlement_source_delay_days), datetime.max.time(), tzinfo=UTC
    )
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
            weights=[
                1,
                1,
                1,
                1,
                1,
                2,
                4,
                7,
                9,
                10,
                11,
                12,
                13,
                13,
                12,
                12,
                13,
                15,
                16,
                14,
                11,
                7,
                4,
                2,
            ],
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

        def base_event(
            status: str,
            ts: datetime,
            reason: str | None = None,
            _ref: str = ref,
            _merchant_id: str = merchant.merchant_id,
            _customer: str = customer,
            _gateway: str = gateway,
            _channel: str = channel,
            _bank: str | None = bank,
            _amount: int = amount,
            _fee: int = fee,
            _currency: str = currency,
            _fx: float | None = fx,
            _created: datetime = created,
            _settlement_lag_days: int = merchant.settlement_lag_days,
        ) -> dict:
            return {
                "event_id": str(uuid.UUID(int=rng.getrandbits(128))),
                "event_ts": ts.isoformat(),
                "transaction_ref": _ref,
                "merchant_id": _merchant_id,
                "customer_id": _customer,
                "gateway": _gateway,
                "channel": _channel,
                "bank_code": _bank,
                "status": status,
                "amount_kobo": _amount,
                "fee_kobo": _fee,
                "currency": _currency,
                "fx_rate_to_ngn": _fx,
                "failure_reason": reason,
                "settlement_lag_days": _settlement_lag_days,
                "settlement_source_watermark_at": source_watermark.isoformat(),
                "created_at": _created.isoformat(),
                "updated_at": ts.isoformat(),
            }

        # 1. pending
        pending = base_event(Status.PENDING, created)

        # 2. terminal
        settled_ok = rng.random() < profile["success"]
        confirm = created + timedelta(seconds=profile["confirm_secs"] * rng.uniform(0.4, 3.0))
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
        if settled_ok and rng.random() < REVERSAL_RATE:
            rev_ts = confirm + timedelta(hours=rng.uniform(6, 96))
            tx_events.append(base_event(Status.REVERSED, rev_ts, "chargeback"))

        # 5. duplicate redelivery of a random event in this transaction
        if rng.random() < DUPLICATE_RATE:
            tx_events.append(dict(rng.choice(pair)))

        # 6. Settlement lines are grouped into merchant/date payout batches.
        # A late reversal may follow a settlement and must remain visible to
        # reconciliation rather than being silently treated as an ordinary gap.
        if settled_ok and rng.random() > UNSETTLED_RATE:
            sdate = (confirm + timedelta(days=merchant.settlement_lag_days)).date()
            settled_at = datetime.combine(sdate, datetime.min.time(), tzinfo=UTC) + timedelta(
                hours=rng.uniform(9, 17)
            )
            # Re-sample the FX rate at settlement time rather than reusing the
            # charge-time `fx`. Settlement lands days after the charge, and a
            # real PSP settles at whatever rate is current then, not the rate
            # quoted at checkout. Reusing `fx` here made settlement_variance_kobo
            # provably zero for every row (both sides truncated the identical
            # amount * fx product) instead of the small, real drift the mart and
            # quality.py's soft check both expect and are built to tolerate.
            settlement_fx = NGN_PER_USD * rng.uniform(0.985, 1.015) if currency == "USD" else None
            gross = amount if currency == "NGN" else int(amount * settlement_fx)
            gross_fee = fee if currency == "NGN" else int(fee * settlement_fx)
            expected_net = gross - gross_fee
            adjustment = rng.random()
            if adjustment < PARTIAL_SETTLEMENT_RATE:
                settled_net = expected_net // 2
            elif adjustment < PARTIAL_SETTLEMENT_RATE + SHORT_PAYMENT_RATE:
                settled_net = max(0, expected_net - max(1, expected_net // 100))
            elif adjustment < PARTIAL_SETTLEMENT_RATE + SHORT_PAYMENT_RATE + OVERPAYMENT_RATE:
                settled_net = expected_net + max(1, expected_net // 100)
            else:
                settled_net = expected_net
            gross = settled_net + gross_fee
            settlements.append(
                {
                    "event_id": str(uuid.UUID(int=rng.getrandbits(128))),
                    "event_ts": settled_at.isoformat(),
                    "settlement_id": f"STL_{uuid.UUID(int=rng.getrandbits(128)).hex[:14]}",
                    "payout_id": f"PYO_{merchant.merchant_id}_{sdate:%Y%m%d}",
                    "merchant_id": merchant.merchant_id,
                    "transaction_ref": ref,
                    "gross_kobo": gross,
                    "fee_kobo": gross_fee,
                    "net_kobo": settled_net,
                    "currency": "NGN",
                    "settlement_date": sdate.isoformat(),
                    "settled_at": settled_at.isoformat(),
                }
            )

    # Shuffle so consumers cannot rely on emission order.
    rng.shuffle(tx_events)
    rng.shuffle(settlements)
    return tx_events, settlements


_DEFAULT_CHUNK_SIZE = 50_000


def _producer_config(bootstrap: str, transactional_id: str, transaction_timeout_ms: int) -> dict:
    """Producer settings for the transactional publisher.

    transactional.id: stable per run, so a retried task fences the failed
      attempt's producer and the broker aborts its open transaction. It also
      implies enable.idempotence, which is set explicitly for readability.
    transaction.timeout.ms 300 s: a 50k-transaction chunk is ~144k messages; the
      broker aborts a transaction left open longer than this, which bounds how
      long a crashed attempt can hold back read_committed consumers.
    linger.ms 100 / batch.size 128k: larger batches cut per-record overhead at
      1M scale; waiting 100 ms per batch costs nothing measurable.
    queue.buffering.max.messages 200k: room for a full chunk's messages without
      BufferError stalls in the produce loop.
    """
    return {
        "bootstrap.servers": bootstrap,
        "transactional.id": transactional_id,
        "transaction.timeout.ms": transaction_timeout_ms,
        "enable.idempotence": True,
        "linger.ms": 100,
        "batch.size": 128 * 1024,
        "compression.type": "lz4",
        "queue.buffering.max.messages": 200_000,
    }


class TransactionalPublisher:
    """Publishes one chunk per Kafka transaction, across both topics.

    A chunk's transaction and settlement events commit together or not at all,
    and read_committed consumers (ingest.py) never see an aborted chunk. Messages
    are keyed by transaction_ref so all events for one reference share a
    partition and keep their order.
    """

    def __init__(
        self,
        bootstrap: str,
        transactional_id: str,
        transaction_timeout_ms: int = 300_000,
        init_timeout_s: float = 60.0,
        flush_timeout_s: float = 300.0,
        producer_factory=None,
    ) -> None:
        if producer_factory is None:
            from confluent_kafka import Producer as producer_factory
        self._producer = producer_factory(
            _producer_config(bootstrap, transactional_id, transaction_timeout_ms)
        )
        self._flush_timeout_s = flush_timeout_s
        # Registers the transactional ID with the coordinator. For an ID used by
        # an earlier attempt, this fences that producer and aborts its open
        # transaction before anything new is sent.
        self._producer.init_transactions(init_timeout_s)

    def _produce_all(self, events: list[dict], topic: str, key_field: str, on_delivery) -> None:
        producer = self._producer
        for i, ev in enumerate(events):
            while True:
                try:
                    producer.produce(
                        topic,
                        key=ev[key_field].encode(),
                        value=json.dumps(ev, separators=(",", ":")).encode(),
                        on_delivery=on_delivery,
                    )
                    break
                except BufferError:
                    # poll() serves delivery callbacks and frees queue slots. It
                    # can itself raise BufferError while the queue is saturated.
                    with contextlib.suppress(BufferError):
                        producer.poll(0.5)
            if i % 500 == 0:
                producer.poll(0)

    def publish_chunk(self, batches: list[tuple[list[dict], str, str]]) -> list[int]:
        """Publish (events, topic, key_field) batches in one transaction.

        Returns the delivered count per batch. On any failure the transaction is
        aborted and the error re-raised, so nothing from this chunk is visible.
        """
        producer = self._producer
        delivered = [0] * len(batches)
        failures: list[str] = []

        def callback_for(idx: int):
            def _cb(err, _msg):
                if err is not None:
                    failures.append(str(err))
                else:
                    delivered[idx] += 1

            return _cb

        producer.begin_transaction()
        try:
            for idx, (events, topic, key_field) in enumerate(batches):
                self._produce_all(events, topic, key_field, callback_for(idx))
            remaining = producer.flush(self._flush_timeout_s)
            if remaining:
                raise RuntimeError(f"{remaining} messages still queued after flush")
            if failures:
                raise RuntimeError(f"{len(failures)} deliveries failed, first: {failures[0]}")
            producer.commit_transaction(self._flush_timeout_s)
        except BaseException:
            # The original error is the one worth reporting. An abort can fail
            # too (for example after fencing), and the broker then aborts the
            # transaction itself at transaction.timeout.ms.
            with contextlib.suppress(Exception):
                producer.abort_transaction(self._flush_timeout_s)
            raise
        return delivered


# ---------------------------------------------------------------------------
# v2: chunked generation for 1M+ transaction scale
# ---------------------------------------------------------------------------


def chunk_starts(n_transactions: int, chunk_size: int) -> range:
    """Offsets of each chunk, in transactions from the start of the run."""
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    return range(0, n_transactions, chunk_size)


def generate_chunk(
    chunk_start: int,
    n_transactions: int,
    days: int,
    seed: int,
    chunk_size: int = _DEFAULT_CHUNK_SIZE,
    **kwargs,
):
    """Generate the chunk that starts at `chunk_start`, seeded with seed + chunk_start.

    Each chunk depends only on its own start offset, so a retry that skips
    committed chunks regenerates the remaining ones exactly as before.
    """
    chunk_n = min(chunk_size, n_transactions - chunk_start)
    return generate_events(n_transactions=chunk_n, days=days, seed=seed + chunk_start, **kwargs)


def iter_chunks(
    n_transactions: int,
    days: int,
    seed: int,
    chunk_size: int = _DEFAULT_CHUNK_SIZE,
    **kwargs,
):
    """Yield (tx_events, settlement_events) in memory-safe chunks.

    Each chunk is a self-contained call to generate_events() seeded with
    `seed + chunk_start`. This makes the full run deterministic: running the
    same --count / --seed / --chunk-size always produces the same sequence of
    Kafka messages in the same order within each chunk, across every chunk.

    Peak RSS per chunk is bounded by chunk_size rather than n_transactions, so
    a 1M-transaction run with the default 50k chunk_size never holds more than
    ~60 MB of Python dicts in memory at once regardless of total volume.

    The `days` window is shared across all chunks so timestamps are
    spread over the same calendar period. kwargs are forwarded to
    generate_events() unchanged (e.g. settlement_source_delay_days, end_date).
    """
    for start in chunk_starts(n_transactions, chunk_size):
        yield generate_chunk(start, n_transactions, days, seed, chunk_size, **kwargs)


def generate_and_publish_chunked(
    n_transactions: int,
    days: int,
    seed: int,
    bootstrap: str,
    topic_transactions: str,
    topic_settlements: str,
    chunk_size: int = _DEFAULT_CHUNK_SIZE,
    run_key: str | None = None,
    ledger=None,
    publisher_factory=None,
    **kwargs,
) -> dict[str, int]:
    """Generate and publish n_transactions, one Kafka transaction per chunk.

    `run_key` names this request (see publish_ledger.run_key). Chunks already in
    `ledger` under that key are skipped, so a retried task publishes only what
    the failed attempt did not commit. Without a ledger (CLI runs) every chunk
    is published and the transactional ID is unique to this call.

    Returns {"transactions_delivered", "settlements_delivered", "chunks",
    "chunks_skipped"}; the delivered counts cover this call only.
    """
    from settlement.publish_ledger import InMemoryChunkLedger, transactional_id

    if run_key is None:
        run_key = f"adhoc-{uuid.uuid4().hex}"
    if ledger is None:
        ledger = InMemoryChunkLedger()
    if publisher_factory is None:
        publisher_factory = TransactionalPublisher

    done = ledger.committed(run_key)
    totals: dict[str, int] = {
        "transactions_delivered": 0,
        "settlements_delivered": 0,
        "chunks": 0,
        "chunks_skipped": 0,
    }
    publisher = None
    for start in chunk_starts(n_transactions, chunk_size):
        if start in done:
            totals["chunks_skipped"] += 1
            continue
        if publisher is None:
            publisher = publisher_factory(bootstrap, transactional_id(run_key))
        tx, stl = generate_chunk(start, n_transactions, days, seed, chunk_size, **kwargs)
        n_tx, n_stl = publisher.publish_chunk(
            [
                (tx, topic_transactions, "transaction_ref"),
                (stl, topic_settlements, "transaction_ref"),
            ]
        )
        ledger.record(run_key, start, n_tx, n_stl)
        totals["transactions_delivered"] += n_tx
        totals["settlements_delivered"] += n_stl
        totals["chunks"] += 1
    return totals


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--count", type=int, default=50_000)
    ap.add_argument("--days", type=int, default=14)
    ap.add_argument("--seed", type=int, default=20260909)
    ap.add_argument("--bootstrap", default=None, help="Kafka bootstrap; omit to write JSONL")
    ap.add_argument("--out-dir", default=None, help="write JSONL here instead of Kafka")
    ap.add_argument(
        "--chunk-size",
        type=int,
        default=_DEFAULT_CHUNK_SIZE,
        help=(
            "Generate and publish this many transactions at a time, one Kafka "
            "transaction per chunk. Keeps peak RSS bounded regardless of --count. "
            f"Default: {_DEFAULT_CHUNK_SIZE:,}."
        ),
    )
    args = ap.parse_args()

    if args.out_dir:
        # JSONL output: generate all at once (--out-dir is only used for
        # local testing at small --count values, not for 1M production runs).
        import pathlib

        tx, stl = generate_events(args.count, args.days, args.seed)
        print(f"generated {len(tx)} transaction events and {len(stl)} settlement events")
        d = pathlib.Path(args.out_dir)
        d.mkdir(parents=True, exist_ok=True)
        for name, rows in (("transactions", tx), ("settlements", stl)):
            path = d / f"{name}.jsonl"
            with path.open("w") as fh:
                for r in rows:
                    fh.write(json.dumps(r, separators=(",", ":")) + "\n")
            print(f"wrote {path} ({len(rows)} rows)")
        return

    from settlement.config import get_settings

    s = get_settings()
    bootstrap = args.bootstrap or s.kafka_bootstrap
    print(f"publishing {args.count:,} transactions in chunks of {args.chunk_size:,}")
    totals = generate_and_publish_chunked(
        n_transactions=args.count,
        days=args.days,
        seed=args.seed,
        bootstrap=bootstrap,
        topic_transactions=s.topic_transactions,
        topic_settlements=s.topic_settlements,
        chunk_size=args.chunk_size,
    )
    print(
        f"published {totals['transactions_delivered']:,} -> {s.topic_transactions}, "
        f"{totals['settlements_delivered']:,} -> {s.topic_settlements} "
        f"({totals['chunks']} chunks)"
    )


if __name__ == "__main__":
    main()
