"""The object-store configuration surface.

The rest of the suite never imports `config`, which meant the MinIO to
SeaweedFS rename in docs/adr/0005 could have been left half-finished with
every test still green. This covers the seam that swap ran through.

No Docker, no network, no object store. Just the contract everything
downstream reads.
"""

from __future__ import annotations

import pytest

from naijapay.config import Settings, get_settings

S3_ENV = {
    "S3_ENDPOINT": "seaweedfs:8333",
    "S3_ACCESS_KEY": "dataeng",
    "S3_SECRET_KEY": "dataeng_local_only",
}


@pytest.fixture
def settings(monkeypatch) -> Settings:
    for k, v in S3_ENV.items():
        monkeypatch.setenv(k, v)
    return get_settings()


def test_s3_settings_read_from_the_environment(settings):
    assert settings.s3_endpoint == "seaweedfs:8333"
    assert settings.s3_access_key == "dataeng"
    assert settings.s3_secret_key == "dataeng_local_only"


def test_endpoint_url_is_a_url_and_the_endpoint_is_not(settings):
    """pyarrow wants host:port, DuckDB and boto3 want a scheme. Both come from here.

    Getting this backwards produces `http://http://host:port`, which surfaces as
    a DNS failure three tasks into a DAG run rather than as a config error.
    """
    assert settings.s3_endpoint_url == "http://seaweedfs:8333"
    assert "://" not in settings.s3_endpoint


def test_bucket_names_are_stable(settings):
    """These are written into Parquet paths and into ClickHouse s3() calls.

    Renaming one silently orphans every object already under the old prefix.
    """
    assert settings.bucket_raw == "lakehouse-raw"
    assert settings.bucket_staged == "lakehouse-staged"
    assert settings.bucket_marts == "lakehouse-marts"


def test_s3_uri_builds_a_path_and_drops_an_empty_key(settings):
    assert settings.s3_uri("lakehouse-raw", "d=2026-09-11/part.parquet") == (
        "s3://lakehouse-raw/d=2026-09-11/part.parquet"
    )
    assert settings.s3_uri("lakehouse-raw") == "s3://lakehouse-raw"


def test_no_minio_named_settings_survive():
    """The rename in ADR 0005 has to be total.

    A leftover `minio_*` attribute means some caller is still reading a setting
    that nothing populates any more, and it will read its default instead of
    failing. That is the quiet kind of broken.
    """
    leftovers = [f for f in Settings.__dataclass_fields__ if "minio" in f.lower()]
    assert leftovers == [], f"ADR 0005 rename incomplete: {leftovers}"


def test_defaults_point_at_the_current_service_names(monkeypatch):
    """With nothing set, the fallbacks must name services that actually exist.

    Every S3 field carries a default, so an unset environment does not raise;
    it silently yields the fallback. That is fine as long as the fallback is
    current. After ADR 0005 a default of `minio:9000` would resolve to nothing
    and surface as a connection timeout inside a Spark task, which is the
    slowest possible way to learn that a rename was incomplete.
    """
    for k in (*S3_ENV, "KAFKA_BOOTSTRAP"):
        monkeypatch.delenv(k, raising=False)
    s = get_settings()
    assert s.s3_endpoint == "seaweedfs:8333"
    assert s.kafka_bootstrap == "kafka:19092"
    assert "minio" not in s.s3_endpoint
