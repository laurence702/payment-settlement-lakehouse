"""Builds every dbt model and runs every dbt test against local Parquet.

No object store, no ClickHouse, no network. The input is the output of the real Spark
transform (see the staged_dir fixture), so the SQL is always tested against data
shaped exactly like production.

Marked slow because it starts a JVM to produce its input.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.slow

PROJECT_DIR = Path(__file__).resolve().parents[1] / "dbt" / "naijapay"


def _dbt_bin() -> str:
    """Absolute path to a dbt executable.

    Absolute, not a bare name: the subprocess below runs with a controlled PATH
    and would otherwise fail to find a dbt that lives inside a virtualenv.
    """
    explicit = os.getenv("DBT_BIN")
    candidates = [explicit] if explicit else []
    candidates += [
        "/home/airflow/dbt-venv/bin/dbt",
        str(Path(sys.executable).parent / "dbt"),
    ]
    for c in candidates:
        if c and Path(c).exists():
            return c
    found = shutil.which("dbt")
    if found:
        return found
    pytest.skip("dbt not found. Set DBT_BIN, or run inside the airflow image.")


@pytest.fixture(scope="module")
def dbt_run(staged_dir, tmp_path_factory):
    marts = tmp_path_factory.mktemp("marts")
    target = tmp_path_factory.mktemp("dbt_target")
    variables = {
        "staged_transactions_path": f"{staged_dir}/transactions/**/*.parquet",
        "staged_settlements_path": f"{staged_dir}/settlements/**/*.parquet",
        "marts_location": str(marts),
    }
    dbt_bin = _dbt_bin()
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
            str(target),
            "--vars",
            json.dumps(variables),
            "--no-use-colors",
        ],
        capture_output=True,
        text=True,
        env={
            # dbt's own directory first: a venv dbt must be able to find its
            # own interpreter and console scripts.
            "PATH": f"{Path(dbt_bin).parent}:/usr/local/bin:/usr/bin:/bin",
            "HOME": str(tmp_path_factory.mktemp("home")),
            "DBT_DUCKDB_PATH": str(tmp_path_factory.mktemp("duckdb") / "t.duckdb"),
            "DBT_LOG_PATH": str(target / "logs"),
        },
    )
    assert result.returncode == 0, f"dbt build failed:\n{result.stdout[-4000:]}"
    return marts, result.stdout


def test_all_models_and_tests_pass(dbt_run):
    _, stdout = dbt_run
    assert "ERROR=0" in stdout
    assert "Completed successfully" in stdout


def test_reconciliation_grain_and_buckets(dbt_run):
    duckdb = pytest.importorskip("duckdb")
    marts, _ = dbt_run
    con = duckdb.connect()
    p = f"{marts}/mart_settlement_reconciliation.parquet"

    total, distinct = con.execute(
        f"select count(*), count(distinct transaction_ref) from '{p}'"
    ).fetchone()
    assert total == distinct, "reconciliation mart is not one row per transaction"

    buckets = dict(
        con.execute(f"select reconciliation_bucket, count(*) from '{p}' group by 1").fetchall()
    )
    assert "settled" in buckets
    assert sum(v for k, v in buckets.items() if k != "settled") > 0, (
        "every transaction settled; the unsettled gap vanished"
    )


def test_settled_rows_carry_no_outstanding_balance(dbt_run):
    duckdb = pytest.importorskip("duckdb")
    marts, _ = dbt_run
    n = (
        duckdb.connect()
        .execute(
            f"select count(*) from '{marts}/mart_settlement_reconciliation.parquet' "
            "where is_settled and outstanding_net_kobo != 0"
        )
        .fetchone()[0]
    )
    assert n == 0


def test_outstanding_exposure_matches_unsettled_rows(dbt_run):
    duckdb = pytest.importorskip("duckdb")
    marts, _ = dbt_run
    con = duckdb.connect()
    p = f"{marts}/mart_settlement_reconciliation.parquet"
    unsettled, exposure = con.execute(
        f"select count(*) filter (where not is_settled), sum(outstanding_net_kobo) from '{p}'"
    ).fetchone()
    assert unsettled > 0
    assert exposure > 0, "unsettled transactions but zero exposure; the sum is wrong"


def test_merchant_daily_attempts_tie_back_to_the_fact_table(dbt_run):
    duckdb = pytest.importorskip("duckdb")
    marts, _ = dbt_run
    con = duckdb.connect()
    daily = con.execute(
        f"select sum(attempts) from '{marts}/mart_merchant_daily.parquet'"
    ).fetchone()[0]
    fact = con.execute(f"select count(*) from '{marts}/fct_transactions.parquet'").fetchone()[0]
    assert daily == fact, "daily aggregate does not tie back to the fact table"
