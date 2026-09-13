"""The generator is the source of truth for every downstream test, so its
pathologies have to be asserted, not assumed."""
from __future__ import annotations

import collections

from naijapay.generate import _fee_kobo, generate_events
from naijapay.schemas import Channel, Status


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
        assert s["net_kobo"] >= 0


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
