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

    # 5a. Hard: an NGN settlement never touches an FX rate, so its variance
    #     must be exactly zero. Any non-zero value here is a pricing bug, full
    #     stop, regardless of size.
    ngn_bad = int(
        client.command(
            f"SELECT count() FROM {db}.mart_settlement_reconciliation "
            f"WHERE is_settled AND currency = 'NGN' AND settlement_variance_kobo != 0"
        )
    )
    if ngn_bad:
        hard.append({"check": "ngn_settlement_variance_nonzero", "rows": ngn_bad})

    # 5b. USD settlement FX drift. generate.py samples both the charge-time
    #     and settlement-time rate independently from
    #     NGN_PER_USD * rng.uniform(0.985, 1.015), so a settled USD row is
    #     expected to carry real, amount-proportional variance now, not just
    #     sub-kobo rounding noise. Two independent draws from that range bound
    #     the theoretical worst case at 1.015/0.985 - 1 ~= 3.05% of the row's
    #     gross amount; 5% leaves headroom for legitimate noise while still
    #     catching an actual pricing or currency bug, which would blow well
    #     past that band.
    FX_DRIFT_HARD_LIMIT = 0.05
    usd_variance = client.command(
        f"SELECT "
        f"  countIf(abs(settlement_variance_kobo) / (amount_ngn_kobo - fee_ngn_kobo) > {FX_DRIFT_HARD_LIMIT}), "
        f"  max(abs(settlement_variance_kobo) / (amount_ngn_kobo - fee_ngn_kobo)), "
        f"  avg(abs(settlement_variance_kobo) / (amount_ngn_kobo - fee_ngn_kobo)), "
        f"  count() "
        f"FROM {db}.mart_settlement_reconciliation "
        f"WHERE is_settled AND currency = 'USD' AND (amount_ngn_kobo - fee_ngn_kobo) != 0"
    )
    over_limit, max_ratio, avg_ratio, usd_n = (
        int(usd_variance[0] or 0),
        float(usd_variance[1] or 0),
        float(usd_variance[2] or 0),
        int(usd_variance[3] or 0),
    )
    if usd_n:
        if over_limit:
            hard.append(
                {
                    "check": "usd_settlement_fx_drift",
                    "rows_over_limit": over_limit,
                    "max_ratio": round(max_ratio, 4),
                    "limit": FX_DRIFT_HARD_LIMIT,
                }
            )
        elif max_ratio:
            soft.append(
                {
                    "check": "usd_settlement_fx_drift",
                    "settled_usd_rows": usd_n,
                    "avg_ratio": round(avg_ratio, 4),
                    "max_ratio": round(max_ratio, 4),
                    "note": "expected settlement-time FX drift, not a defect",
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
