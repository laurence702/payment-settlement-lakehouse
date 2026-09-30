"""Post-load checks that run against the SERVING layer.

dbt tests validate the marts as files. These validate what ClickHouse actually
serves, which is not the same thing: a load can succeed and still put the wrong
number in front of a dashboard, because of a type inference surprise or a
partial swap.

Each check returns a (passed, detail) pair. The task fails on any hard failure
and warns on soft ones, so a rounding drift does not page anyone but a missing
table does.
"""

from __future__ import annotations

from settlement.config import Settings, get_settings
from settlement.serve import MARTS, _client


def run_checks(settings: Settings) -> tuple[list[dict], list[dict]]:
    client = _client(settings)
    db = settings.clickhouse_db
    hard: list[dict] = []
    soft: list[dict] = []

    # 1. Every mart exists and is non-empty.
    for spec in MARTS:
        n = int(client.command(f"SELECT count() FROM {db}.{spec.name}"))
        if n == 0:
            hard.append({"check": f"{spec.name}_non_empty", "rows": n})

    # 2. Grain holds in the serving layer, not just in dbt.
    dupes = int(
        client.command(
            f"SELECT count() FROM (SELECT transaction_ref FROM {db}.fct_transactions "
            f"GROUP BY transaction_ref HAVING count() > 1)"
        )
    )
    if dupes:
        hard.append({"check": "fct_transactions_unique_ref", "duplicate_refs": dupes})

    # 3. A matched row must carry zero outstanding balance. A settlement line
    #    can be partial or short-paid, so presence alone does not reconcile it.
    bad = int(
        client.command(
            f"SELECT count() FROM {db}.mart_settlement_reconciliation "
            f"WHERE is_amount_matched AND outstanding_net_kobo != 0"
        )
    )
    if bad:
        hard.append({"check": "matched_rows_zero_outstanding", "violations": bad})

    # 4. Reconciliation totals must tie back to the transaction fact. The mart
    #    grain is: all settlement-eligible transactions, plus reversed
    #    transactions that carry a prior settlement (reversed_after_settlement).
    #    That second population is absent from is_settlement_eligible, so the
    #    query must mirror the mart's own WHERE predicate.
    recon_count = int(client.command(f"SELECT count() FROM {db}.mart_settlement_reconciliation"))
    expected_count = int(
        client.command(
            f"SELECT count() FROM {db}.fct_transactions "
            f"WHERE is_settlement_eligible "
            f"   OR (is_reversed AND transaction_ref IN "
            f"       (SELECT transaction_ref FROM {db}.fct_settlements))"
        )
    )
    if recon_count != expected_count:
        hard.append(
            {
                "check": "reconciliation_ties_to_fact",
                "reconciliation_rows": recon_count,
                "expected_rows": expected_count,
            }
        )

    # 5a. A matched row cannot carry a variance. Amount discrepancies are
    #     explicit v0.2 reconciliation outcomes, including for NGN lines.
    matched_bad = int(
        client.command(
            f"SELECT count() FROM {db}.mart_settlement_reconciliation "
            f"WHERE reconciliation_status = 'matched' "
            f"AND (settlement_variance_kobo != 0 OR outstanding_net_kobo != 0 "
            f"OR NOT is_amount_matched)"
        )
    )
    if matched_bad:
        hard.append({"check": "matched_reconciliation_consistency", "rows": matched_bad})

    # 5b. Preserve serving-layer visibility for expected v0.2 discrepancies
    #     without treating them as a pipeline failure. USD lines additionally
    #     include independently sampled settlement-time FX drift.
    variance_counts = client.command(
        f"SELECT "
        "  countIf(currency = 'NGN' AND is_settled AND NOT is_amount_matched), "
        "  countIf(currency = 'USD' AND is_settled AND settlement_variance_kobo != 0) "
        f"FROM {db}.mart_settlement_reconciliation "
    )
    ngn_discrepancies, usd_variances = (
        int(variance_counts[0] or 0),
        int(variance_counts[1] or 0),
    )
    if ngn_discrepancies or usd_variances:
        soft.append(
            {
                "check": "settlement_amount_discrepancies",
                "ngn_amount_discrepancies": ngn_discrepancies,
                "usd_rows_with_variance": usd_variances,
                "note": "review by reconciliation status before escalation",
            }
        )

    return hard, soft


def main() -> None:
    hard, soft = run_checks(get_settings())
    for s in soft:
        print(f"WARN {s}")
    if hard:
        for h in hard:
            print(f"FAIL {h}")
        raise SystemExit(f"{len(hard)} serving-layer quality check(s) failed")
    print(f"all serving-layer checks passed ({len(soft)} warning(s))")


if __name__ == "__main__":
    main()
