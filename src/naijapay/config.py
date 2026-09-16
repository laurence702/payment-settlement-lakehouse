"""Single place that reads the environment.

Every module imports settings from here rather than calling os.getenv inline,
so a missing variable fails once, loudly, at import time, instead of three
tasks into a DAG run.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(key: str, default: str | None = None) -> str:
    val = os.getenv(key, default)
    if val is None:
        raise RuntimeError(
            f"required environment variable {key!r} is not set. "
            "In Airflow these come from the compose env_file; locally, "
            "run `make shell` or source the project .env."
        )
    return val


@dataclass(frozen=True)
class Settings:
    # --- Kafka --------------------------------------------------------------
    kafka_bootstrap: str = field(default_factory=lambda: _env("KAFKA_BOOTSTRAP", "kafka:19092"))
    topic_transactions: str = "naijapay.transactions.v1"
    topic_settlements: str = "naijapay.settlements.v1"

    # --- Object store -------------------------------------------------------
    # S3_*, not SEAWEEDFS_*. pyarrow, DuckDB httpfs and ClickHouse all speak
    # plain S3 and none of them care which server answers. See docs/adr/0005.
    s3_endpoint: str = field(default_factory=lambda: _env("S3_ENDPOINT", "seaweedfs:8333"))
    s3_access_key: str = field(default_factory=lambda: _env("S3_ACCESS_KEY", "dataeng"))
    s3_secret_key: str = field(default_factory=lambda: _env("S3_SECRET_KEY", "dataeng_local_only"))

    bucket_raw: str = "lakehouse-raw"
    bucket_staged: str = "lakehouse-staged"
    bucket_marts: str = "lakehouse-marts"

    # --- Warehouse ----------------------------------------------------------
    clickhouse_host: str = field(default_factory=lambda: _env("CLICKHOUSE_HOST", "clickhouse"))
    clickhouse_port: int = field(default_factory=lambda: int(_env("CLICKHOUSE_PORT", "8123")))
    clickhouse_user: str = field(default_factory=lambda: _env("CLICKHOUSE_USER", "dataeng"))
    clickhouse_password: str = field(
        default_factory=lambda: _env("CLICKHOUSE_PASSWORD", "dataeng_local_only")
    )
    clickhouse_db: str = field(default_factory=lambda: _env("CLICKHOUSE_DB", "naijapay"))

    @property
    def s3_endpoint_url(self) -> str:
        return f"http://{self.s3_endpoint}"

    def s3_uri(self, bucket: str, key: str = "") -> str:
        return f"s3://{bucket}/{key}".rstrip("/")


def get_settings() -> Settings:
    return Settings()
