"""Reverse ETL: current breaks from the serving layer into the ops store.

Runs as an Airflow task after the serving-layer quality checks. It is
idempotent: running it twice on the same marts changes nothing, and it only
ever writes the pipeline-owned columns, so analyst decisions survive any number
of pipeline runs. See docs/adr/0007.
"""

from __future__ import annotations

import argparse
from collections.abc import Iterator

from settlement.config import Settings, get_settings
from settlement.ops.models import BreakRow
from settlement.ops.postgres_store import PostgresCaseStore
from settlement.ops.service import CaseStore
from settlement.serve import _client

_BREAKS_SQL = (
    "SELECT case_id, transaction_ref, merchant_id, channel, gateway, bank_code, currency, "
    "succeeded_date, amount_ngn_kobo, expected_net_kobo, settled_net_kobo, break_type, "
    "exposure_kobo FROM {db}.mart_settlement_reconciliation WHERE is_break"
)


def _to_row(r) -> BreakRow:
    return BreakRow(
        case_id=r[0],
        transaction_ref=r[1],
        merchant_id=r[2],
        channel=r[3],
        gateway=r[4],
        bank_code=r[5],
        currency=r[6],
        succeeded_date=r[7],
        amount_ngn_kobo=int(r[8]),
        expected_net_kobo=int(r[9]),
        settled_net_kobo=None if r[10] is None else int(r[10]),
        break_type=r[11],
        exposure_kobo=int(r[12]),
    )


def read_breaks(client, db: str) -> Iterator[BreakRow]:
    """Stream current breaks out of ClickHouse in blocks, never all at once."""
    with client.query_row_block_stream(_BREAKS_SQL.format(db=db)) as stream:
        for block in stream:
            for row in block:
                yield _to_row(row)


def run(settings: Settings, store: CaseStore | None = None) -> dict:
    client = _client(settings)
    db = settings.clickhouse_db

    # An empty mart means the load failed, not that every break was fixed.
    # Clearing every case on that signal would be wrong, so refuse.
    mart_rows = int(client.command(f"SELECT count() FROM {db}.mart_settlement_reconciliation"))
    if mart_rows == 0:
        raise RuntimeError("mart_settlement_reconciliation is empty; refusing to clear cases")

    owns_store = store is None
    store = store or PostgresCaseStore(settings)
    try:
        open_cases, cleared = store.sync_breaks(read_breaks(client, db))
    finally:
        if owns_store:
            store.close()
    return {"open": open_cases, "cleared": cleared, "mart_rows": mart_rows}


def main() -> None:
    argparse.ArgumentParser(description=__doc__.splitlines()[0]).parse_args()
    print(run(get_settings()))


if __name__ == "__main__":
    main()
