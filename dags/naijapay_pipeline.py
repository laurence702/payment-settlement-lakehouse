"""End-to-end NaijaPay settlement reconciliation pipeline.

    generate -> kafka -> raw parquet (S3)
             -> spark dedupe/latest-wins -> staged parquet (S3)
             -> dbt-duckdb -> marts parquet (S3)
             -> clickhouse -> grafana

Scheduling note: this DAG is manual-trigger by default. A local laptop stack
that wakes up hourly to churn a 6 GB VM is a laptop with no battery. Set a
schedule when it runs somewhere that is meant to be always on.
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=2),
    # Fail fast rather than occupying the single LocalExecutor slot for an hour
    # because a broker went away.
    "execution_timeout": pendulum.duration(minutes=25),
}


@dag(
    dag_id="naijapay_pipeline",
    description="Nigerian payments settlement reconciliation, ingest to serving",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["naijapay", "lakehouse", "portfolio"],
    params={
        "event_count": 50000,
        "days": 14,
        "seed": 20260909,
        # Bumping this makes the ingest task re-read the topics from the
        # beginning, because it joins a brand new consumer group. Without it, a
        # second run legitimately finds nothing left to consume.
        "consumer_group_suffix": "v1",
        "skip_generate": False,
    },
)
def naijapay_pipeline():

    @task
    def preflight() -> dict:
        """Verify network connectivity to Kafka, S3, and ClickHouse before pipeline execution."""
        import socket

        from naijapay.config import get_settings

        s = get_settings()
        targets = {
            "kafka": tuple(s.kafka_bootstrap.split(":")),
            "s3": tuple(s.s3_endpoint.split(":")),
            "clickhouse": (s.clickhouse_host, str(s.clickhouse_port)),
        }
        unreachable = []
        for name, (host, port) in targets.items():
            try:
                with socket.create_connection((host, int(port)), timeout=5):
                    pass
            except OSError as exc:
                unreachable.append(f"{name} ({host}:{port}): {exc}")

        if unreachable:
            raise RuntimeError(
                "cannot reach: " + "; ".join(unreachable) + ". "
                "On a 6 GB VM these profiles are usually not all up at once. "
                "Check `make ps` in the shared-infra repo."
            )
        return {name: f"{h}:{p}" for name, (h, p) in targets.items()}

    @task
    def generate_events(**context) -> dict:
        """Publish synthetic transaction and settlement events to Kafka."""
        from naijapay.config import get_settings
        from naijapay.generate import generate_events as gen
        from naijapay.generate import publish

        params = context["params"]
        if params["skip_generate"]:
            return {"skipped": True}

        s = get_settings()
        tx, stl = gen(
            n_transactions=int(params["event_count"]),
            days=int(params["days"]),
            seed=int(params["seed"]),
        )
        published_tx = publish(tx, s.topic_transactions, s.kafka_bootstrap, "transaction_ref")
        published_stl = publish(stl, s.topic_settlements, s.kafka_bootstrap, "transaction_ref")
        return {"transactions": published_tx, "settlements": published_stl}

    @task
    def ingest_to_raw(**context) -> dict:
        """Drain both topics into partitioned Parquet in the raw bucket."""
        from naijapay.config import get_settings
        from naijapay.ingest import drain_topic
        from naijapay.schemas import settlement_event_schema, transaction_event_schema

        s = get_settings()
        suffix = context["params"]["consumer_group_suffix"]
        out = {}
        for topic, schema, dataset in (
            (s.topic_transactions, transaction_event_schema(), "transactions"),
            (s.topic_settlements, settlement_event_schema(), "settlements"),
        ):
            out[dataset] = drain_topic(
                topic=topic,
                schema=schema,
                dataset=dataset,
                settings=s,
                group_id=f"naijapay-ingest-{dataset}-{suffix}",
            )
        if sum(v["written"] for v in out.values()) == 0:
            raise RuntimeError(
                "ingest wrote 0 rows. Either nothing was produced, or this "
                "consumer group already consumed the topics. Re-run with a new "
                "consumer_group_suffix."
            )
        return out

    @task
    def spark_raw_to_staged() -> dict:
        """Dedupe, collapse to one row per transaction, derive NGN amounts.

        Runs in local mode inside this task's process. See the memory notes in
        the shared-infra .env: the Spark driver heap lives under the Airflow
        scheduler container's limit, which is why the scheduler gets 1.8 GB.
        """
        import os

        from naijapay.config import get_settings
        from naijapay.transform_spark import run

        return run(
            get_settings(),
            driver_memory=os.getenv("SPARK_DRIVER_MEMORY", "1g"),
            shuffle_partitions=int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "8")),
        )

    # dbt runs as a subprocess against its own virtualenv, never imported into
    # the Airflow process. `dbt build` runs models and their tests together, so
    # a model whose test fails does not propagate downstream.
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            "set -euo pipefail; "
            "cd $DBT_PROJECT_DIR && "
            "$DBT_VENV/bin/dbt build "
            "  --profiles-dir $DBT_PROJECT_DIR "
            "  --target local "
            "  --no-use-colors"
        ),
        env={
            "DBT_PROJECT_DIR": "/opt/airflow/dbt/naijapay",
            "DBT_VENV": "/home/airflow/dbt-venv",
            # dbt writes its catalog and logs somewhere writable that is not a
            # bind mount, so a failed run does not litter the host repo.
            "DBT_DUCKDB_PATH": "/tmp/naijapay.duckdb",
            "DBT_LOG_PATH": "/tmp/dbt-logs",
            "DBT_TARGET_PATH": "/tmp/dbt-target",
            "S3_ENDPOINT": "{{ var.value.get('s3_endpoint', 'seaweedfs:8333') }}",
            "S3_ACCESS_KEY": "{{ var.value.get('s3_access_key', 'dataeng') }}",
            "S3_SECRET_KEY": "{{ var.value.get('s3_secret_key', 'dataeng_local_only') }}",
        },
        append_env=True,
    )

    @task
    def load_clickhouse() -> list[dict]:
        """Load each mart into ClickHouse behind an atomic table swap."""
        from naijapay.config import get_settings
        from naijapay.serve import run

        return run(get_settings())

    @task
    def serving_quality_checks() -> dict:
        """Validate what ClickHouse serves, not just what dbt wrote."""
        from naijapay.config import get_settings
        from naijapay.quality import run_checks

        hard, soft = run_checks(get_settings())
        if hard:
            raise RuntimeError(f"serving-layer checks failed: {hard}")
        return {"passed": True, "warnings": soft}

    (
        preflight()
        >> generate_events()
        >> ingest_to_raw()
        >> spark_raw_to_staged()
        >> dbt_build
        >> load_clickhouse()
        >> serving_quality_checks()
    )


naijapay_pipeline()
