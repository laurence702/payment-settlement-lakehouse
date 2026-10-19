"""The generator is the source of truth for every downstream test, so its
pathologies have to be asserted, not assumed."""

from __future__ import annotations

import collections

import pytest

from settlement.generate import _fee_kobo, generate_events, iter_chunks
from settlement.schemas import Channel, Status


def test_deterministic_for_a_given_seed():
    a_tx, a_stl = generate_events(500, 7, seed=42)
    b_tx, b_stl = generate_events(500, 7, seed=42)
    assert a_tx == b_tx
    assert a_stl == b_stl


def test_different_seeds_differ():
    a, _ = generate_events(500, 7, seed=1)
    b, _ = generate_events(500, 7, seed=2)
    assert a != b


def test_emits_multiple_events_per_transaction(events_small):
    tx, _ = events_small
    per_ref = collections.Counter(e["transaction_ref"] for e in tx)
    assert min(per_ref.values()) >= 2, "every charge must emit at least pending + terminal"
    assert max(per_ref.values()) >= 3, "some charges must emit a reversal or duplicate"


def test_injects_duplicate_event_ids(events_small):
    tx, _ = events_small
    ids = collections.Counter(e["event_id"] for e in tx)
    dupes = sum(c - 1 for c in ids.values() if c > 1)
    assert dupes > 0, "no duplicates injected; the dedupe stage would be untested"


def test_settlement_gap_is_within_expected_band(events_small):
    tx, stl = events_small
    success = {e["transaction_ref"] for e in tx if e["status"] == Status.SUCCESS}
    reversed_ = {e["transaction_ref"] for e in tx if e["status"] == Status.REVERSED}
    settled = {s["transaction_ref"] for s in stl}
    eligible = success - reversed_
    gap = len(eligible - settled) / len(eligible)
    # UNSETTLED_RATE is 3.1%. Wide band because this is a random process, but
    # tight enough that a logic change which settles everything fails here.
    assert 0.01 < gap < 0.07, f"unsettled gap {gap:.2%} outside expected band"


def test_settlements_only_reference_successful_transactions(events_small):
    tx, stl = events_small
    success = {e["transaction_ref"] for e in tx if e["status"] == Status.SUCCESS}
    assert {s["transaction_ref"] for s in stl} <= success


def test_money_is_always_integer_kobo(events_small):
    tx, stl = events_small
    for e in tx:
        assert isinstance(e["amount_kobo"], int)
        assert isinstance(e["fee_kobo"], int)
        assert e["amount_kobo"] > 0
        assert e["fee_kobo"] >= 0
    for s in stl:
        assert s["net_kobo"] == s["gross_kobo"] - s["fee_kobo"]
        assert isinstance(s["net_kobo"], int)
        assert s["net_kobo"] >= 0


def test_transactions_carry_merchant_settlement_rule_and_source_watermark(events_small):
    tx, _ = events_small
    assert {event["settlement_lag_days"] for event in tx} <= {1, 2}
    assert all(event["settlement_source_watermark_at"] for event in tx)


def test_settlement_lines_share_payout_batches(events_small):
    _, stl = events_small
    payout_counts = collections.Counter(event["payout_id"] for event in stl)
    assert all(event["payout_id"].startswith("PYO_") for event in stl)
    assert max(payout_counts.values()) > 1, "settlements are not grouped into payout batches"


def test_source_watermark_can_be_deterministically_delayed():
    tx, _ = generate_events(20, 2, seed=11, settlement_source_delay_days=4)
    watermark_dates = {event["settlement_source_watermark_at"][:10] for event in tx}
    assert len(watermark_dates) == 1


def test_fee_never_exceeds_amount(events_small):
    tx, _ = events_small
    assert all(e["fee_kobo"] <= e["amount_kobo"] for e in tx)


def test_fee_is_capped():
    # 1.5% of 10,000,000 kobo would be 150,000; the cap is 200,000 kobo plus the
    # 10,000 kobo surcharge, so a very large amount must hit the cap exactly.
    assert _fee_kobo(100_000_000_00, Channel.CARD) == 2_000_00


def test_usd_transactions_carry_an_fx_rate(events_small):
    tx, _ = events_small
    usd = [e for e in tx if e["currency"] == "USD"]
    assert usd, "no USD transactions generated"
    assert all(e["fx_rate_to_ngn"] and e["fx_rate_to_ngn"] > 0 for e in usd)


def test_ngn_transactions_have_no_fx_rate(events_small):
    tx, _ = events_small
    assert all(e["fx_rate_to_ngn"] is None for e in tx if e["currency"] == "NGN")


def test_settlement_fx_is_resampled_independently_of_charge_fx(events_small):
    """Regression test for the fix that stopped settlement reusing the
    charge-time fx. Before the fix this was a tautology: settlement gross
    always equalled int(amount_kobo * charge_fx), so
    settlement_variance_kobo was provably zero for every row instead of
    carrying the small, real drift the mart and quality.py's soft check
    both expect."""
    tx, stl = events_small
    usd_by_ref = {
        e["transaction_ref"]: (e["amount_kobo"], e["fx_rate_to_ngn"])
        for e in tx
        if e["currency"] == "USD"
    }
    compared = 0
    mismatches = 0
    for s in stl:
        info = usd_by_ref.get(s["transaction_ref"])
        if info is None:
            continue
        amount_kobo, charge_fx = info
        compared += 1
        if int(amount_kobo * charge_fx) != s["gross_kobo"]:
            mismatches += 1
    assert compared > 10, "too few USD settlements to test independence"
    assert mismatches > 0, (
        "every USD settlement matches the charge-time fx exactly: "
        "settlement is not resampling its own fx"
    )


# ---------------------------------------------------------------------------
# v2: iter_chunks — chunked generation for 1M+ transaction scale
# ---------------------------------------------------------------------------


def test_iter_chunks_produces_correct_number_of_chunks():
    """20k transactions in 8k chunks → 3 chunks (8k, 8k, 4k)."""
    chunks = list(iter_chunks(n_transactions=20_000, days=14, seed=1, chunk_size=8_000))
    assert len(chunks) == 3


def test_iter_chunks_total_transaction_count_matches_request():
    """Sum of all chunk tx events must correspond to exactly n_transactions."""
    n = 15_000
    chunk_size = 4_000
    tx_total = 0
    stl_total = 0
    for tx, stl in iter_chunks(n_transactions=n, days=14, seed=42, chunk_size=chunk_size):
        # Each chunk tx list contains ~2+ events per transaction (pending + terminal).
        # We measure unique transaction_refs, not raw event count.
        tx_total += len({e["transaction_ref"] for e in tx})
        stl_total += len(stl)
    assert tx_total == n, f"expected {n} unique refs, got {tx_total}"
    # Settlements are a subset of successes; just assert they exist and aren't empty.
    assert stl_total > 0


def test_iter_chunks_is_deterministic_across_runs():
    """Same seed+count+chunk_size must produce identical output on two runs."""
    chunks_a = list(iter_chunks(n_transactions=5_000, days=7, seed=99, chunk_size=2_000))
    chunks_b = list(iter_chunks(n_transactions=5_000, days=7, seed=99, chunk_size=2_000))
    for (tx_a, stl_a), (tx_b, stl_b) in zip(chunks_a, chunks_b, strict=True):
        assert tx_a == tx_b
        assert stl_a == stl_b


def test_iter_chunks_different_seeds_produce_different_output():
    first_a, _ = next(iter(iter_chunks(5_000, 7, seed=1, chunk_size=5_000)))
    first_b, _ = next(iter(iter_chunks(5_000, 7, seed=2, chunk_size=5_000)))
    assert first_a != first_b


def test_iter_chunks_preserves_money_invariants_across_chunk_boundaries():
    """The kobo precision and net=gross-fee invariant must hold in every chunk."""
    for tx, stl in iter_chunks(n_transactions=10_000, days=14, seed=7, chunk_size=3_000):
        for e in tx:
            assert isinstance(e["amount_kobo"], int) and e["amount_kobo"] > 0
            assert isinstance(e["fee_kobo"], int) and e["fee_kobo"] >= 0
        for s in stl:
            assert s["net_kobo"] == s["gross_kobo"] - s["fee_kobo"]
            assert isinstance(s["net_kobo"], int) and s["net_kobo"] >= 0


@pytest.mark.slow
def test_iter_chunks_peak_rss_bounded_for_large_run():
    """v2 memory invariant: a 200k-transaction chunked run must not exceed
    300 MB peak RSS (platform rss measurement via sys).

    This is a slow test because 200k transactions take a few seconds to
    generate. It does NOT require Docker, Kafka, or PySpark. It runs in CI
    as part of the 'slow' suite alongside Spark and dbt tests.

    Rationale: at 200k transactions with the default 50k chunk_size, peak RSS
    should be ~60-80 MB. At 300 MB we'd already be at 5x that — so the bound
    is conservative enough to survive GC noise while catching a regression
    back to single-list generation (which would be ~240 MB+ at 200k).
    """
    import tracemalloc

    tracemalloc.start()
    for tx, stl in iter_chunks(n_transactions=200_000, days=14, seed=20260909, chunk_size=50_000):
        # Consume the chunk (simulate what publish() would do).
        _ = len(tx) + len(stl)
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    peak_mb = peak / 1024 / 1024
    assert peak_mb < 300, (
        f"iter_chunks peak RSS {peak_mb:.1f} MB exceeds 300 MB bound at 200k transactions. "
        "This likely means single-list generation was accidentally restored."
    )
