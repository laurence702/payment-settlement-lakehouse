from __future__ import annotations

import json
from pathlib import Path

import pytest

from naijapay.generate import generate_events

SEED = 20260909


@pytest.fixture(scope="session")
def events():
    """A small but pathological dataset: duplicates, out-of-order, unsettled."""
    return generate_events(n_transactions=3000, days=14, seed=SEED)


@pytest.fixture(scope="session")
def events_small():
    """Smaller dataset for the fast, pure-logic tests."""
    return generate_events(n_transactions=2000, days=14, seed=SEED)


@pytest.fixture(scope="session")
def raw_dir(tmp_path_factory, events) -> Path:
    tx, stl = events
    d = tmp_path_factory.mktemp("raw")
    for name, rows in (("transactions", tx), ("settlements", stl)):
        (d / f"{name}.jsonl").write_text(
            "\n".join(json.dumps(r, separators=(",", ":")) for r in rows)
        )
    return d


@pytest.fixture(scope="session")
def raw_parquet_dir(tmp_path_factory, events) -> Path:
    """Raw events as Parquet, matching what the ingest task writes to the object store."""
    pa = pytest.importorskip("pyarrow")
    import pyarrow.parquet as pq

    from naijapay.schemas import settlement_event_schema, transaction_event_schema

    from datetime import datetime, timezone

    tx, stl = events
    d = tmp_path_factory.mktemp("raw_parquet")
    now = datetime.now(timezone.utc)

    def coerce(rows, schema):
        out = []
        for r in rows:
            rec = {}
            for f in schema:
                if f.name == "ingested_at":
                    rec[f.name] = now
                elif f.name == "settlement_date":
                    rec[f.name] = datetime.fromisoformat(r[f.name]).date()
                elif f.name in ("event_ts", "created_at", "updated_at", "settled_at"):
                    rec[f.name] = datetime.fromisoformat(r[f.name])
                else:
                    rec[f.name] = r.get(f.name)
            out.append(rec)
        return pa.Table.from_pylist(out, schema=schema)

    for name, rows, schema in (
        ("transactions", tx, transaction_event_schema()),
        ("settlements", stl, settlement_event_schema()),
    ):
        sub = d / name
        sub.mkdir()
        pq.write_table(coerce(rows, schema), sub / "part-0000.parquet", compression="zstd")
    return d


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    from naijapay.transform_spark import build_session

    s = build_session(driver_memory="1g", shuffle_partitions=4)
    yield s
    s.stop()


@pytest.fixture(scope="session")
def staged_dir(tmp_path_factory, spark, raw_parquet_dir) -> Path:
    """Runs the REAL Spark transform. Not a reimplementation of it.

    The dbt tests consume this, so the SQL is always tested against output the
    production code actually produces. A hand-rolled DuckDB stand-in would drift
    from the Spark job and quietly stop testing anything.
    """
    from naijapay.transform_spark import transform_settlements, transform_transactions

    out = tmp_path_factory.mktemp("staged")
    transform_transactions(spark, raw_parquet_dir / "transactions", out / "transactions")
    transform_settlements(spark, raw_parquet_dir / "settlements", out / "settlements")
    return out
