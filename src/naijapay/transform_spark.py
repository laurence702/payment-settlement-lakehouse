"""Raw -> staged, in local-mode PySpark.

Scope, stated plainly because it matters more than the code:

  At 50k transactions, DuckDB would do this faster than Spark can start its JVM.
  Spark is here because the job below is genuinely a Spark-shaped job (a global
  deduplication and a window function over the whole dataset) and because it is
  the code you would submit unchanged to a real cluster. It runs with
  master=local[*]. There is no cluster. See docs/adr/0002-spark-local-mode.md.

  I/O is via the local filesystem, not s3a://. Wiring hadoop-aws and a matching
  AWS SDK into a local-mode job means version-matching two jars against Spark's
  bundled Hadoop and carrying ~200 MB in an image on a 6 GB VM. The transform
  logic is identical either way, so the objects are staged down with pyarrow,
  processed, and staged back up. On a real cluster you would delete the two
  pyarrow calls and pass an s3a:// path. See the ADR.

What this stage actually fixes:

  * Duplicate delivery. Kafka is at-least-once; the same event_id appears twice.
  * Multiple events per transaction. pending then success is two rows for one
    charge. Downstream wants one.
  * Out-of-order arrival. The terminal event can land before its own pending
    event, so "last row wins by arrival order" is wrong. Ordering is by
    updated_at, with a deterministic status-rank tiebreak so a rerun on the same
    input produces byte-identical output.
"""

from __future__ import annotations

import argparse
import contextlib
import shutil
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from naijapay.config import Settings, get_settings
from naijapay.schemas import STATUS_RANK


def _s3(settings: Settings):
    from pyarrow import fs as pafs

    host, _, port = settings.s3_endpoint.partition(":")
    return pafs.S3FileSystem(
        access_key=settings.s3_access_key,
        secret_key=settings.s3_secret_key,
        endpoint_override=f"{host}:{port or '9000'}",
        scheme="http",
        allow_bucket_creation=False,
    )


def _download(settings: Settings, bucket: str, prefix: str, dest: Path) -> int:
    """Pull every parquet object under a prefix into a local directory."""
    from pyarrow import fs as pafs

    s3 = _s3(settings)
    dest.mkdir(parents=True, exist_ok=True)
    selector = pafs.FileSelector(f"{bucket}/{prefix}", recursive=True, allow_not_found=True)
    n = 0
    for info in s3.get_file_info(selector):
        if info.type != pafs.FileType.File or not info.path.endswith(".parquet"):
            continue
        safe_name = info.path.replace("/", "_")
        local = dest / f"{n:05d}_{safe_name}"
        with s3.open_input_stream(info.path) as src, local.open("wb") as out:
            shutil.copyfileobj(src, out)
        n += 1
    return n


def _upload(settings: Settings, src_dir: Path, bucket: str, prefix: str) -> int:
    s3 = _s3(settings)
    with contextlib.suppress(Exception):
        s3.delete_dir_contents(f"{bucket}/{prefix}", missing_dir_ok=True)
    n = 0
    for local in sorted(src_dir.rglob("*.parquet")):
        rel = local.relative_to(src_dir).as_posix()
        with local.open("rb") as fh, s3.open_output_stream(f"{bucket}/{prefix}/{rel}") as sink:
            shutil.copyfileobj(fh, sink)
        n += 1
    return n


def build_session(driver_memory: str, shuffle_partitions: int):
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName("naijapay-raw-to-staged")
        # local[2] instead of local[*]: all executor threads share the single
        # driver JVM inside the 1.8 GB scheduler container. With 4 vCPUs the
        # peak RSS of concurrent window-function shuffles exceeds the limit.
        # Two threads halves the in-flight memory at a ~30 % speed cost that
        # is irrelevant on a portfolio demo stack.
        .master("local[2]")
        .config("spark.driver.memory", driver_memory)
        # The default of 200 shuffle partitions on a laptop produces 200 tiny
        # files and spends more time on task overhead than on the actual work.
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        # Reduce the fraction of heap Spark reserves for execution/storage so
        # JVM metaspace + Python overhead fits within the container budget.
        .config("spark.memory.fraction", "0.6")
        .config("spark.memory.storageFraction", "0.3")
        .getOrCreate()
    )


def transform_transactions(spark, in_dir: Path, out_dir: Path) -> dict:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    raw = spark.read.parquet(in_dir.as_posix())
    raw_count = raw.count()

    # 1. Drop exact redeliveries per transaction. event_id is the producer's idempotency key.
    deduped = raw.dropDuplicates(["transaction_ref", "event_id"])
    after_dedupe = deduped.count()

    # 2. Collapse the event stream to one row per transaction, latest wins.
    #    The status_rank tiebreak makes this deterministic when two events for
    #    one reference share an updated_at, which happens when a producer emits
    #    a pending and a terminal status inside the same clock tick.
    rank_expr = F.create_map(*[x for k, v in STATUS_RANK.items() for x in (F.lit(k), F.lit(v))])
    ranked = deduped.withColumn("status_rank", rank_expr[F.col("status")])

    w = Window.partitionBy("transaction_ref").orderBy(
        F.col("updated_at").desc(), F.col("status_rank").desc(), F.col("event_id").desc()
    )
    latest = ranked.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

    # 3. Derived columns the marts need and should not each recompute.
    staged = (
        latest.withColumn(
            "amount_ngn_kobo",
            F.when(
                F.col("currency") == "USD",
                (F.col("amount_kobo") * F.coalesce(F.col("fx_rate_to_ngn"), F.lit(0.0))).cast(
                    "long"
                ),
            ).otherwise(F.col("amount_kobo")),
        )
        .withColumn(
            "fee_ngn_kobo",
            F.when(
                F.col("currency") == "USD",
                (F.col("fee_kobo") * F.coalesce(F.col("fx_rate_to_ngn"), F.lit(0.0))).cast("long"),
            ).otherwise(F.col("fee_kobo")),
        )
        # How late the event was relative to when it happened. This is the
        # column that lets you answer "is our pipeline behind, or is the
        # upstream slow", which are different incidents.
        .withColumn(
            "arrival_lag_seconds",
            (F.col("ingested_at").cast("long") - F.col("updated_at").cast("long")),
        )
        .withColumn("is_terminal", F.col("status").isin("success", "failed", "reversed"))
        .withColumn("event_date", F.to_date("created_at"))
        .withColumn("processed_at", F.lit(datetime.now(UTC)).cast("timestamp"))
        .drop("status_rank")
    )

    out_count = staged.count()
    staged.repartition("event_date").write.mode("overwrite").partitionBy("event_date").parquet(
        out_dir.as_posix()
    )

    return {
        "raw_events": raw_count,
        "after_dedupe": after_dedupe,
        "duplicates_removed": raw_count - after_dedupe,
        "transactions_out": out_count,
        "events_collapsed": after_dedupe - out_count,
    }


def transform_settlements(spark, in_dir: Path, out_dir: Path) -> dict:
    from pyspark.sql import functions as F

    raw = spark.read.parquet(in_dir.as_posix())
    raw_count = raw.count()
    staged = raw.dropDuplicates(["settlement_id", "transaction_ref"]).withColumn(
        "processed_at", F.lit(datetime.now(UTC)).cast("timestamp")
    )
    out_count = staged.count()
    staged.repartition("settlement_date").write.mode("overwrite").partitionBy(
        "settlement_date"
    ).parquet(out_dir.as_posix())
    return {
        "raw_events": raw_count,
        "settlements_out": out_count,
        "duplicates_removed": raw_count - out_count,
    }


def run(settings: Settings, driver_memory: str, shuffle_partitions: int) -> dict:
    spark = build_session(driver_memory, shuffle_partitions)
    metrics: dict = {}
    try:
        with tempfile.TemporaryDirectory(prefix="naijapay-spark-") as tmp:
            root = Path(tmp)
            for dataset, fn, out_prefix in (
                ("transactions", transform_transactions, "transactions"),
                ("settlements", transform_settlements, "settlements"),
            ):
                in_dir = root / "in" / dataset
                out_dir = root / "out" / dataset
                files = _download(settings, settings.bucket_raw, dataset, in_dir)
                if files == 0:
                    raise RuntimeError(
                        f"no raw parquet found under "
                        f"{settings.bucket_raw}/{dataset}. Run the ingest task first."
                    )
                metrics[dataset] = fn(spark, in_dir, out_dir)
                metrics[dataset]["input_files"] = files
                metrics[dataset]["output_files"] = _upload(
                    settings, out_dir, settings.bucket_staged, out_prefix
                )
    finally:
        spark.stop()
    return metrics


def main() -> None:
    import os

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--driver-memory", default=os.getenv("SPARK_DRIVER_MEMORY", "1g"))
    ap.add_argument(
        "--shuffle-partitions", type=int, default=int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
    )
    args = ap.parse_args()

    metrics = run(get_settings(), args.driver_memory, args.shuffle_partitions)
    for dataset, m in metrics.items():
        print(f"{dataset}: {m}")

    tx = metrics["transactions"]
    if tx["transactions_out"] == 0:
        raise SystemExit("staged transactions is empty, refusing to succeed")
    if tx["duplicates_removed"] == 0:
        # Not fatal, but worth shouting about: the generator injects ~2%
        # duplicates, so zero here means dedupe silently stopped working.
        print("WARNING: no duplicates removed. Verify the generator and dedupe key.")


if __name__ == "__main__":
    main()
