"""
Standalone script: generate staged Parquet (via real Spark transform) then run
`dbt build` directly so its PASS/ERROR summary prints to the terminal.

Usage (from project root):
    JAVA_HOME=/opt/homebrew/opt/openjdk@17 \
    .venv/bin/python scripts/run_dbt_direct.py
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROJECT_DIR = ROOT / "dbt" / "settlement"
sys.path.insert(0, str(ROOT / "src"))


def coerce_to_parquet(rows, schema):
    import pyarrow as pa

    out = []
    for r in rows:
        rec = {}
        for f in schema:
            if f.name == "ingested_at":
                rec[f.name] = datetime.now(UTC)
            elif f.name == "settlement_date":
                rec[f.name] = datetime.fromisoformat(r[f.name]).date()
            elif f.name in (
                "event_ts",
                "created_at",
                "updated_at",
                "settled_at",
                "settlement_source_watermark_at",
            ):
                rec[f.name] = datetime.fromisoformat(r[f.name])
            else:
                rec[f.name] = r.get(f.name)
        out.append(rec)
    return pa.Table.from_pylist(out, schema=schema)


def main():
    import pyarrow.parquet as pq

    from settlement.generate import generate_events
    from settlement.schemas import settlement_event_schema, transaction_event_schema
    from settlement.transform_spark import (
        build_session,
        transform_settlements,
        transform_transactions,
    )

    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        raw_dir = tmp / "raw"
        staged_dir = tmp / "staged"
        marts_dir = tmp / "marts"
        target_dir = tmp / "dbt_target"
        home_dir = tmp / "home"
        duckdb_dir = tmp / "duckdb"
        for d in (raw_dir, staged_dir, marts_dir, target_dir, home_dir, duckdb_dir):
            d.mkdir(parents=True)

        # --- 1. Generate synthetic events ---
        print("→ Generating 3 000 synthetic transactions …", flush=True)
        tx_events, stl_events = generate_events(
            n_transactions=3000, days=14, seed=20260909, settlement_source_delay_days=4
        )

        # --- 2. Write raw Parquet ---
        print("→ Writing raw Parquet …", flush=True)
        for name, rows, schema in (
            ("transactions", tx_events, transaction_event_schema()),
            ("settlements", stl_events, settlement_event_schema()),
        ):
            sub = raw_dir / name
            sub.mkdir()
            pq.write_table(
                coerce_to_parquet(rows, schema),
                sub / "part-0000.parquet",
                compression="zstd",
            )

        # --- 3. Spark transform ---
        print("→ Running Spark transform (this starts the JVM) …", flush=True)
        spark = build_session(driver_memory="1g", shuffle_partitions=4)
        try:
            transform_transactions(spark, raw_dir / "transactions", staged_dir / "transactions")
            transform_settlements(spark, raw_dir / "settlements", staged_dir / "settlements")
        finally:
            spark.stop()

        # --- 4. dbt build ---
        print("\n→ Running dbt build (localfs target) …\n", flush=True)
        dbt_bin = str(ROOT / ".venv" / "bin" / "dbt")
        variables = {
            "staged_transactions_path": f"{staged_dir}/transactions/**/*.parquet",
            "staged_settlements_path": f"{staged_dir}/settlements/**/*.parquet",
            "marts_location": str(marts_dir),
        }
        result = subprocess.run(
            [
                dbt_bin,
                "build",
                "--profiles-dir",
                str(PROJECT_DIR),
                "--project-dir",
                str(PROJECT_DIR),
                "--target",
                "localfs",
                "--target-path",
                str(target_dir),
                "--vars",
                json.dumps(variables),
            ],
            env={
                "PATH": f"{Path(dbt_bin).parent}:/usr/local/bin:/usr/bin:/bin",
                "HOME": str(home_dir),
                "DBT_DUCKDB_PATH": str(duckdb_dir / "t.duckdb"),
                "DBT_LOG_PATH": str(target_dir / "logs"),
                "JAVA_HOME": "/opt/homebrew/opt/openjdk@17",
            },
        )
        sys.exit(result.returncode)


if __name__ == "__main__":
    main()
