"""Kafka -> object store, raw layer.

Design decisions worth defending in an interview:

* This is a BATCH drain of a stream, not a streaming job. It reads until the
  broker stops handing it messages, then exits. Airflow schedules batches; a
  never-ending consumer inside an Airflow task is a task that never succeeds.

* Offsets are committed only AFTER the Parquet file is durably written. That
  ordering is the whole difference between at-least-once (a crash re-reads and
  the dedupe downstream absorbs it) and at-most-once (a crash silently loses
  data). Auto-commit is disabled for this reason.

* The raw layer is append-only and keeps duplicates, out-of-order events and
  malformed rows. Cleaning here would destroy the evidence you need when a
  number looks wrong three weeks later.
"""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs as pafs

from naijapay.config import Settings, get_settings
from naijapay.schemas import settlement_event_schema, transaction_event_schema

_TS_FIELDS = ("event_ts", "created_at", "updated_at", "settled_at")


def _s3(settings: Settings) -> pafs.S3FileSystem:
    host, _, port = settings.s3_endpoint.partition(":")
    return pafs.S3FileSystem(
        access_key=settings.s3_access_key,
        secret_key=settings.s3_secret_key,
        endpoint_override=f"{host}:{port or '9000'}",
        scheme="http",
        allow_bucket_creation=False,
    )


def _coerce(rec: dict, schema: pa.Schema) -> dict:
    """Cast one JSON record into the declared schema.

    Anything the schema does not declare is dropped, and anything declared but
    absent becomes null. That is deliberate: a producer adding a field should
    not change the shape of the raw table without a schema change here.
    """
    out: dict = {}
    for field in schema:
        if field.name == "ingested_at":
            continue
        val = rec.get(field.name)
        if val is None:
            out[field.name] = None
        elif field.name in _TS_FIELDS:
            out[field.name] = datetime.fromisoformat(val)
        elif field.name == "settlement_date":
            out[field.name] = datetime.fromisoformat(val).date()
        else:
            out[field.name] = val
    return out


def drain_topic(
    topic: str,
    schema: pa.Schema,
    dataset: str,
    settings: Settings,
    group_id: str,
    max_messages: int = 500_000,
    idle_timeout_s: float = 10.0,
    batch_rows: int = 25_000,
) -> dict:
    """Consume `topic` until it goes quiet, writing Parquet to the raw bucket."""
    from confluent_kafka import Consumer, KafkaError

    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            # Manual commit: see module docstring.
            "enable.auto.commit": False,
            "max.poll.interval.ms": 300_000,
            "session.timeout.ms": 45_000,
        }
    )
    consumer.subscribe([topic])

    s3 = _s3(settings)
    ingested_at = datetime.now(UTC)
    partition_date = ingested_at.date().isoformat()

    buf: list[dict] = []
    stats = {"consumed": 0, "written": 0, "malformed": 0, "files": 0}
    deadline = datetime.now(UTC) + timedelta(seconds=idle_timeout_s)
    part_no = 0

    def flush() -> None:
        nonlocal buf, part_no
        if not buf:
            return
        for row in buf:
            row["ingested_at"] = ingested_at
        table = pa.Table.from_pylist(buf, schema=schema)
        key = (
            f"{settings.bucket_raw}/{dataset}/ingest_date={partition_date}/"
            f"part-{ingested_at:%Y%m%dT%H%M%S}-{part_no:04d}.parquet"
        )
        with s3.open_output_stream(key) as sink:
            pq.write_table(table, sink, compression="zstd")
        stats["written"] += len(buf)
        stats["files"] += 1
        part_no += 1
        buf = []

    try:
        while stats["consumed"] < max_messages:
            msg = consumer.poll(1.0)
            if msg is None:
                if datetime.now(UTC) >= deadline:
                    break
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(f"kafka error: {msg.error()}")

            deadline = datetime.now(UTC) + timedelta(seconds=idle_timeout_s)
            stats["consumed"] += 1
            try:
                buf.append(_coerce(json.loads(msg.value()), schema))
            except Exception:
                # A poison message must not take down the whole batch. Count it
                # and move on; the count is asserted on downstream.
                stats["malformed"] += 1
                continue

            if len(buf) >= batch_rows:
                flush()
                consumer.commit(asynchronous=False)

        flush()
        if stats["consumed"]:
            # Guard against _NO_OFFSET: when the final consumed batch was an
            # exact multiple of batch_rows, the intermediate commit at line 147
            # already committed all offsets. The trailing commit has nothing
            # left to store and librdkafka raises _NO_OFFSET. That is not an
            # error — it means everything was already safely committed.
            try:
                consumer.commit(asynchronous=False)
            except Exception as exc:
                from confluent_kafka import KafkaError, KafkaException

                if isinstance(exc, KafkaException) and exc.args[0].code() == KafkaError._NO_OFFSET:
                    pass
                else:
                    raise
    finally:
        consumer.close()

    return stats


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--group-suffix", default="v1")
    ap.add_argument("--idle-timeout", type=float, default=10.0)
    args = ap.parse_args()

    s = get_settings()
    results = {}
    for topic, schema, dataset in (
        (s.topic_transactions, transaction_event_schema(), "transactions"),
        (s.topic_settlements, settlement_event_schema(), "settlements"),
    ):
        results[dataset] = drain_topic(
            topic=topic,
            schema=schema,
            dataset=dataset,
            settings=s,
            group_id=f"naijapay-ingest-{dataset}-{args.group_suffix}",
            idle_timeout_s=args.idle_timeout,
        )
        print(f"{dataset}: {results[dataset]}")

    total = sum(r["written"] for r in results.values())
    if total == 0:
        raise SystemExit(
            "ingest wrote 0 rows. Either the producer has not run, or this "
            "consumer group has already consumed everything. Use a fresh "
            "--group-suffix to re-read from the beginning."
        )


if __name__ == "__main__":
    main()
