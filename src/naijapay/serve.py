"""Marts -> ClickHouse, the serving layer.

Why ClickHouse at all, when dbt already produced the marts?

  DuckDB is an excellent transform engine and a poor serving database. It is
  single-writer, embedded, and has no notion of concurrent dashboard users.
  Grafana pointed at a .duckdb file is a demo, not a serving tier. ClickHouse
  reads the same Parquet the transform stage wrote and serves it with real
  concurrency, so the lakehouse stays the source of truth and the warehouse is
  a queryable projection of it, not a second copy of the logic.

Two decisions worth defending:

  * Schema is INFERRED from the Parquet rather than hand-written DDL. Hand-
    maintained DDL that must be kept in lockstep with dbt models drifts, and it
    drifts silently, in the direction of a column that quietly stopped being
    populated. ORDER BY and PARTITION BY are still declared explicitly, because
    those are performance decisions ClickHouse cannot infer.

  * Loads are atomic. Data goes into a shadow table, then EXCHANGE TABLES swaps
    it in under a lock. TRUNCATE followed by INSERT leaves a window, sometimes
    several seconds wide, where a dashboard renders zeroes. That window is how
    you get asked why revenue went to zero at 3am.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass

from naijapay.config import Settings, get_settings


@dataclass(frozen=True)
class MartSpec:
    name: str
    parquet: str
    order_by: str
    partition_by: str | None = None


MARTS: tuple[MartSpec, ...] = (
    MartSpec(
        name="fct_transactions",
        parquet="fct_transactions.parquet",
        order_by="(event_date, merchant_id, transaction_ref)",
        partition_by="toYYYYMM(event_date)",
    ),
    MartSpec(
        name="fct_settlements",
        parquet="fct_settlements.parquet",
        order_by="(settlement_date, merchant_id, settlement_id)",
        partition_by="toYYYYMM(settlement_date)",
    ),
    MartSpec(
        name="mart_settlement_reconciliation",
        parquet="mart_settlement_reconciliation.parquet",
        # Ordered so the dashboard's hottest predicate, "show me everything
        # that breached SLA", is a prefix scan rather than a full read.
        order_by="(reconciliation_bucket, merchant_id, succeeded_date)",
    ),
    MartSpec(
        name="mart_merchant_daily",
        parquet="mart_merchant_daily.parquet",
        order_by="(event_date, merchant_id)",
    ),
    MartSpec(
        name="mart_channel_performance",
        parquet="mart_channel_performance.parquet",
        order_by="(event_date, channel, bank_code)",
    ),
)


def _client(settings: Settings):
    import clickhouse_connect

    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        # Do not pin the database here: the CREATE DATABASE below has to be
        # able to run before the database exists.
        settings={"max_execution_time": 300},
    )


def _s3_expr(settings: Settings, parquet: str) -> str:
    """s3() call using the named collection defined in the ClickHouse config.

    The collection carries the endpoint and credentials, so no access key ever
    appears in a query, a log line, or this repository.
    """
    return (
        f"s3(s3_lakehouse, "
        f"filename = '{settings.bucket_marts}/{parquet}', "
        f"format = 'Parquet')"
    )


def load_mart(client, settings: Settings, spec: MartSpec) -> dict:
    db = settings.clickhouse_db
    live = f"{db}.{spec.name}"
    shadow = f"{db}.{spec.name}__incoming"
    s3 = _s3_expr(settings, spec.parquet)
    partition = f"PARTITION BY {spec.partition_by}" if spec.partition_by else ""

    client.command(f"DROP TABLE IF EXISTS {shadow}")

    # EMPTY AS SELECT infers the column types from the Parquet footer without
    # reading a single row of data.
    client.command(
        f"CREATE TABLE {shadow} ENGINE = MergeTree {partition} "
        f"ORDER BY {spec.order_by} EMPTY AS SELECT * FROM {s3}"
    )
    client.command(f"INSERT INTO {shadow} SELECT * FROM {s3}")
    rows = client.command(f"SELECT count() FROM {shadow}")

    if int(rows) == 0:
        client.command(f"DROP TABLE IF EXISTS {shadow}")
        raise RuntimeError(
            f"{spec.name}: loaded 0 rows from {spec.parquet}. Refusing to swap "
            "an empty table over live data."
        )

    exists = int(
        client.command(
            f"SELECT count() FROM system.tables "
            f"WHERE database = '{db}' AND name = '{spec.name}'"
        )
    )
    if exists:
        # Atomic. Readers see either the old table or the new one, never
        # an empty one.
        client.command(f"EXCHANGE TABLES {live} AND {shadow}")
        client.command(f"DROP TABLE IF EXISTS {shadow}")
    else:
        client.command(f"RENAME TABLE {shadow} TO {live}")

    return {"table": spec.name, "rows": int(rows)}


def run(settings: Settings) -> list[dict]:
    client = _client(settings)
    client.command(f"CREATE DATABASE IF NOT EXISTS {settings.clickhouse_db}")
    return [load_mart(client, settings, spec) for spec in MARTS]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--only", default=None, help="load a single mart by name")
    args = ap.parse_args()

    s = get_settings()
    client = _client(s)
    client.command(f"CREATE DATABASE IF NOT EXISTS {s.clickhouse_db}")

    specs = [m for m in MARTS if args.only in (None, m.name)]
    if not specs:
        raise SystemExit(f"no mart named {args.only!r}. Known: {[m.name for m in MARTS]}")

    for spec in specs:
        print(load_mart(client, s, spec))


if __name__ == "__main__":
    main()
