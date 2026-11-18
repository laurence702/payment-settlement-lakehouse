"""Count a chaos run's events in Kafka and in the raw layer.

Runs inside the Airflow scheduler container, which has the Kafka client, the
object-store credentials and the generator; scripts/chaos_generate.py pipes it
in with `docker exec -i ... python - <args>`. Prints one JSON document.

The expected event_ids are regenerated locally from the run's parameters. They
come from the seeded RNG alone, so they match what the DAG published. The
generator injects duplicate event_ids on purpose, so counts are compared as
multisets: a correct run has each id exactly as often as the generator emits it.
"""

from __future__ import annotations

import json
import sys
import uuid
from collections import Counter

from confluent_kafka import Consumer, KafkaError
from pyarrow import fs as pafs
from pyarrow import parquet as pq

from settlement.config import get_settings
from settlement.generate import iter_chunks


def expected_ids(n: int, days: int, seed: int, chunk: int) -> dict[str, Counter]:
    exp = {"transactions": Counter(), "settlements": Counter()}
    for tx, stl in iter_chunks(n, days, seed, chunk):
        exp["transactions"].update(e["event_id"] for e in tx)
        exp["settlements"].update(e["event_id"] for e in stl)
    return exp


def kafka_counts(topic: str, wanted: Counter, isolation: str) -> tuple[Counter, int]:
    s = get_settings()
    consumer = Consumer(
        {
            "bootstrap.servers": s.kafka_bootstrap,
            "group.id": f"chaos-check-{uuid.uuid4().hex}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.partition.eof": True,
            "isolation.level": isolation,
        }
    )
    consumer.subscribe([topic])
    seen: Counter = Counter()
    total = 0
    at_eof: set[int] = set()
    partitions = None
    idle = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if partitions is None and consumer.assignment():
                partitions = {p.partition for p in consumer.assignment()}
            if msg is None:
                idle += 1
                if idle > 30:
                    break
                continue
            idle = 0
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    at_eof.add(msg.partition())
                    if partitions and at_eof >= partitions:
                        break
                continue
            event_id = json.loads(msg.value()).get("event_id")
            if event_id in wanted:
                seen[event_id] += 1
                total += 1
    finally:
        consumer.close()
    return seen, total


def raw_counts(dataset: str, wanted: Counter) -> Counter:
    s = get_settings()
    s3 = s.s3_filesystem()
    selector = pafs.FileSelector(f"{s.bucket_raw}/{dataset}", recursive=True, allow_not_found=True)
    seen: Counter = Counter()
    for info in s3.get_file_info(selector):
        if info.type != pafs.FileType.File or not info.path.endswith(".parquet"):
            continue
        with s3.open_input_file(info.path) as fh:
            ids = pq.read_table(fh, columns=["event_id"]).column("event_id").to_pylist()
        seen.update(i for i in ids if i in wanted)
    return seen


def compare(seen: Counter, exp: Counter) -> dict:
    over = sum(max(0, seen[k] - v) for k, v in exp.items())
    under = sum(max(0, v - seen[k]) for k, v in exp.items())
    return {
        "expected": sum(exp.values()),
        "found": sum(seen.values()),
        "extra": over,
        "missing": under,
        "exact": over == 0 and under == 0,
    }


def main() -> None:
    n, days, seed, chunk = (int(a) for a in sys.argv[1:5])
    s = get_settings()
    exp = expected_ids(n, days, seed, chunk)
    out: dict = {}
    for dataset, topic in (
        ("transactions", s.topic_transactions),
        ("settlements", s.topic_settlements),
    ):
        committed, _ = kafka_counts(topic, exp[dataset], "read_committed")
        _, uncommitted_total = kafka_counts(topic, exp[dataset], "read_uncommitted")
        out[dataset] = {
            "kafka_read_committed": compare(committed, exp[dataset]),
            "kafka_read_uncommitted_found": uncommitted_total,
            "raw_parquet": compare(raw_counts(dataset, exp[dataset]), exp[dataset]),
        }
    print(json.dumps(out))


if __name__ == "__main__":
    main()
