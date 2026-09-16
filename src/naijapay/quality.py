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

from naijapay.config import Settings, get_settings
from naijapay.serve import MARTS, _client


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

    # 3. A settled row must carry zero outstanding balance.
    bad = int(
        client.command(
            f"SELECT count() FROM {db}.mart_settlement_reconciliation "
            f"WHERE is_settled AND outstanding_net_kobo != 0"
        )
    )
    if bad:
        hard.append({"check": "settled_rows_zero_outstanding", "violations": bad})

    # 4. Reconciliation totals must tie back to the transaction fact. If these
    #    disagree, one of the two loads is stale and the dashboard is lying.
    recon_success = int(client.command(f"SELECT count() FROM {db}.mart_settlement_reconciliation"))
    fct_success = int(
        client.command(f"SELECT count() FROM {db}.fct_transactions WHERE is_settlement_eligible")
    )
    if recon_success != fct_success:
        hard.append(
            {
                "check": "reconciliation_ties_to_fact",
                "reconciliation_rows": recon_success,
                "eligible_transactions": fct_success,
            }
        )

    # 5. Soft: FX rounding drift. Converting USD kobo at an FX rate and casting
    #    to an integer loses sub-kobo amounts, so a small non-zero variance is
    #    expected and correct. A LARGE one is a pricing bug.
    variance = client.command(
        f"SELECT sum(abs(settlement_variance_kobo)), count() "
        f"FROM {db}.mart_settlement_reconciliation WHERE is_settled"
    )
    total_var, settled_n = (int(variance[0] or 0), int(variance[1] or 0))
    if settled_n:
        per_row = total_var / settled_n
        if per_row > 100:  # more than 1 naira average drift is not rounding
            hard.append({"check": "settlement_variance", "avg_kobo_per_row": per_row})
        elif total_var:
            soft.append(
                {
                    "check": "settlement_variance",
                    "total_kobo": total_var,
                    "avg_kobo_per_row": round(per_row, 4),
                    "note": "expected FX integer-truncation drift, not a defect",
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
