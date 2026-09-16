"""Exercises the real Spark transform, not a stand-in.

Marked slow because it starts a JVM. Run with: pytest -m slow
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.slow


def test_dedupe_and_collapse_to_one_row_per_transaction(spark, raw_parquet_dir, tmp_path, events):
    from naijapay.transform_spark import transform_transactions

    tx, _ = events
    expected_refs = len({e["transaction_ref"] for e in tx})

    metrics = transform_transactions(spark, raw_parquet_dir / "transactions", tmp_path / "out")

    assert metrics["raw_events"] == len(tx)
    assert metrics["duplicates_removed"] > 0, "dedupe removed nothing"
    assert metrics["transactions_out"] == expected_refs, "grain is not one row per reference"
    assert metrics["events_collapsed"] > 0


def test_latest_status_wins_including_out_of_order(spark, raw_parquet_dir, tmp_path, events):
    """The critical correctness property.

    Events arrive shuffled, and ~5% of charges emit their terminal event before
    their pending event. Ordering by arrival would leave those stuck in pending.
    """
    from naijapay.transform_spark import transform_transactions

    tx, _ = events
    transform_transactions(spark, raw_parquet_dir / "transactions", tmp_path / "out")
    staged = spark.read.parquet((tmp_path / "out").as_posix())

    # Recompute the expected winner independently of the Spark code.
    rank = {"pending": 0, "failed": 1, "success": 2, "reversed": 3}
    best: dict[str, tuple] = {}
    for e in tx:
        key = (e["updated_at"], rank[e["status"]], e["event_id"])
        ref = e["transaction_ref"]
        if ref not in best or key > best[ref][0]:
            best[ref] = (key, e["status"])
    expected = {ref: status for ref, (_, status) in best.items()}

    actual = {
        r["transaction_ref"]: r["status"]
        for r in staged.select("transaction_ref", "status").collect()
    }

    assert actual == expected


def test_no_transaction_is_left_pending_when_a_terminal_event_exists(
    spark, raw_parquet_dir, tmp_path, events
):
    from naijapay.transform_spark import transform_transactions

    tx, _ = events
    has_terminal = {
        e["transaction_ref"] for e in tx if e["status"] in ("success", "failed", "reversed")
    }
    transform_transactions(spark, raw_parquet_dir / "transactions", tmp_path / "out")
    staged = spark.read.parquet((tmp_path / "out").as_posix())

    still_pending = {
        r["transaction_ref"]
        for r in staged.filter("status = 'pending'").select("transaction_ref").collect()
    }
    assert not (still_pending & has_terminal), (
        "a transaction with a terminal event was left pending; out-of-order handling is broken"
    )


def test_usd_amounts_are_converted_to_naira(spark, raw_parquet_dir, tmp_path):
    from naijapay.transform_spark import transform_transactions

    transform_transactions(spark, raw_parquet_dir / "transactions", tmp_path / "out")
    staged = spark.read.parquet((tmp_path / "out").as_posix())

    usd = (
        staged.filter("currency = 'USD'")
        .select("amount_kobo", "amount_ngn_kobo", "fx_rate_to_ngn")
        .collect()
    )
    assert usd, "no USD rows to check"
    for r in usd:
        assert r["amount_ngn_kobo"] > r["amount_kobo"]
        assert abs(r["amount_ngn_kobo"] - r["amount_kobo"] * r["fx_rate_to_ngn"]) <= 1

    ngn = (
        staged.filter("currency = 'NGN'")
        .select("amount_kobo", "amount_ngn_kobo")
        .limit(50)
        .collect()
    )
    assert all(r["amount_kobo"] == r["amount_ngn_kobo"] for r in ngn)


def test_settlements_are_deduplicated(spark, raw_parquet_dir, tmp_path, events):
    from naijapay.transform_spark import transform_settlements

    _, stl = events
    metrics = transform_settlements(spark, raw_parquet_dir / "settlements", tmp_path / "stl")
    assert metrics["settlements_out"] == len({s["settlement_id"] for s in stl})


def test_output_is_reproducible(spark, raw_parquet_dir, tmp_path):
    """Two runs over identical input must agree exactly.

    This is what the status_rank tiebreak buys. Without it, two events sharing
    an updated_at resolve non-deterministically and the marts change between
    runs for no reason.
    """
    from naijapay.transform_spark import transform_transactions

    transform_transactions(spark, raw_parquet_dir / "transactions", tmp_path / "a")
    transform_transactions(spark, raw_parquet_dir / "transactions", tmp_path / "b")

    def fingerprint(p):
        rows = (
            spark.read.parquet(p.as_posix())
            .select("transaction_ref", "status", "amount_ngn_kobo")
            .collect()
        )
        return sorted((r[0], r[1], r[2]) for r in rows)

    assert fingerprint(tmp_path / "a") == fingerprint(tmp_path / "b")
