"""Transactional publishing and the chunk ledger.

The fake producer models what the guarantee depends on: messages produced inside
a transaction become visible to read_committed consumers only when the
transaction commits, and an abort discards them.
"""

from __future__ import annotations

import json
import os
import uuid
from collections import Counter

import pytest

from settlement.generate import (
    TransactionalPublisher,
    generate_and_publish_chunked,
    iter_chunks,
)
from settlement.publish_ledger import InMemoryChunkLedger, run_key, transactional_id

N, DAYS, SEED, CHUNK = 900, 7, 7, 200  # five chunks, the last one partial
TOPIC_TX, TOPIC_STL = "tx", "stl"


class FakeBroker:
    def __init__(self) -> None:
        self.committed: dict[str, list[dict]] = {TOPIC_TX: [], TOPIC_STL: []}
        self.init_calls: list[str] = []


class FakeProducer:
    """confluent_kafka.Producer stand-in with transactional semantics."""

    def __init__(self, config: dict, broker: FakeBroker, fail_commit_on: int | None = None):
        self.config = config
        self.broker = broker
        self.fail_commit_on = fail_commit_on
        self.commits = 0
        self.calls: list[str] = []
        self._open: list[tuple[str, bytes]] | None = None
        self._pending_callbacks: list = []

    def init_transactions(self, _timeout=None):
        self.calls.append("init")
        self.broker.init_calls.append(self.config["transactional.id"])

    def begin_transaction(self):
        assert self._open is None, "transaction already open"
        self.calls.append("begin")
        self._open = []

    def produce(self, topic, key, value, on_delivery):
        assert self._open is not None, "produce outside a transaction"
        self._open.append((topic, value))
        self._pending_callbacks.append(on_delivery)

    def poll(self, _timeout=0):
        return 0

    def flush(self, _timeout=None):
        for cb in self._pending_callbacks:
            cb(None, None)
        self._pending_callbacks.clear()
        return 0

    def commit_transaction(self, _timeout=None):
        self.calls.append("commit")
        if self.fail_commit_on is not None and self.commits == self.fail_commit_on:
            raise RuntimeError("broker went away")
        for topic, value in self._open:
            self.broker.committed[topic].append(json.loads(value))
        self._open = None
        self.commits += 1

    def abort_transaction(self, _timeout=None):
        self.calls.append("abort")
        self._open = None


def _publisher_factory(broker: FakeBroker, created: list, fail_commit_on: int | None = None):
    def factory(bootstrap, txn_id):
        def producer_factory(config):
            p = FakeProducer(config, broker, fail_commit_on)
            created.append(p)
            return p

        return TransactionalPublisher(bootstrap, txn_id, producer_factory=producer_factory)

    return factory


def _run(broker, ledger, key, created, fail_commit_on=None):
    return generate_and_publish_chunked(
        n_transactions=N,
        days=DAYS,
        seed=SEED,
        bootstrap="fake:9092",
        topic_transactions=TOPIC_TX,
        topic_settlements=TOPIC_STL,
        chunk_size=CHUNK,
        run_key=key,
        ledger=ledger,
        publisher_factory=_publisher_factory(broker, created, fail_commit_on),
    )


def _expected_counts() -> tuple[int, int]:
    tx = stl = 0
    for t, s in iter_chunks(N, DAYS, SEED, CHUNK):
        tx += len(t)
        stl += len(s)
    return tx, stl


def test_one_producer_one_transaction_per_chunk_covering_both_topics():
    broker, created = FakeBroker(), []
    key = run_key("manual__1", N, DAYS, SEED, CHUNK)
    totals = _run(broker, InMemoryChunkLedger(), key, created)

    assert len(created) == 1
    calls = created[0].calls
    assert calls[0] == "init" and calls.count("init") == 1
    assert calls.count("begin") == calls.count("commit") == 5
    assert "abort" not in calls
    assert created[0].config["transactional.id"] == transactional_id(key)

    exp_tx, exp_stl = _expected_counts()
    assert (totals["transactions_delivered"], totals["settlements_delivered"]) == (exp_tx, exp_stl)
    assert (totals["chunks"], totals["chunks_skipped"]) == (5, 0)
    assert len(broker.committed[TOPIC_TX]) == exp_tx
    assert len(broker.committed[TOPIC_STL]) == exp_stl


def test_retry_after_a_failed_commit_publishes_each_chunk_exactly_once():
    """A task that fails mid-run and is retried leaves no duplicates in the topics."""
    key = run_key("manual__2", N, DAYS, SEED, CHUNK)
    ledger = InMemoryChunkLedger()
    broker, created = FakeBroker(), []

    # Attempt 1: chunks 0 and 1 commit, the commit of chunk 2 fails.
    with pytest.raises(RuntimeError, match="broker went away"):
        _run(broker, ledger, key, created, fail_commit_on=2)
    assert created[0].calls[-1] == "abort"
    assert ledger.committed(key) == {0, 200}

    # Attempt 2: same run key, same transactional ID; skips what is ledgered.
    totals = _run(broker, ledger, key, created)
    assert (totals["chunks"], totals["chunks_skipped"]) == (3, 2)
    assert broker.init_calls == [transactional_id(key)] * 2

    clean_broker, clean_created = FakeBroker(), []
    _run(clean_broker, InMemoryChunkLedger(), "clean", clean_created)
    for topic in (TOPIC_TX, TOPIC_STL):
        ids = Counter(ev["event_id"] for ev in broker.committed[topic])
        clean_ids = Counter(ev["event_id"] for ev in clean_broker.committed[topic])
        assert ids == clean_ids, topic


def test_without_a_ledger_a_rerun_duplicates_every_chunk():
    """The failure mode the ledger exists to prevent, shown directly."""
    broker, created = FakeBroker(), []
    _run(broker, None, "no-ledger", created)
    _run(broker, None, "no-ledger", created)
    exp_tx, _ = _expected_counts()
    assert len(broker.committed[TOPIC_TX]) == 2 * exp_tx


def test_fully_ledgered_run_opens_no_producer():
    key = run_key("manual__3", N, DAYS, SEED, CHUNK)
    ledger = InMemoryChunkLedger()
    for start in range(0, N, CHUNK):
        ledger.record(key, start, 1, 1)
    created: list = []
    totals = _run(FakeBroker(), ledger, key, created)
    assert created == []
    assert totals == {
        "transactions_delivered": 0,
        "settlements_delivered": 0,
        "chunks": 0,
        "chunks_skipped": 5,
    }


def test_delivery_failure_aborts_and_is_not_ledgered():
    class FailingDelivery(FakeProducer):
        def flush(self, _timeout=None):
            for cb in self._pending_callbacks:
                cb("MSG_TIMED_OUT", None)
            self._pending_callbacks.clear()
            return 0

    broker, ledger, created = FakeBroker(), InMemoryChunkLedger(), []

    def factory(bootstrap, txn_id):
        def producer_factory(config):
            p = FailingDelivery(config, broker)
            created.append(p)
            return p

        return TransactionalPublisher(bootstrap, txn_id, producer_factory=producer_factory)

    with pytest.raises(RuntimeError, match="deliveries failed"):
        generate_and_publish_chunked(
            N,
            DAYS,
            SEED,
            "fake:9092",
            TOPIC_TX,
            TOPIC_STL,
            CHUNK,
            run_key="k",
            ledger=ledger,
            publisher_factory=factory,
        )
    assert created[0].calls[-2:] == ["begin", "abort"]
    assert broker.committed == {TOPIC_TX: [], TOPIC_STL: []}
    assert ledger.committed("k") == set()


def test_run_key_changes_with_any_generate_parameter():
    base = run_key("manual__4", 1000, 14, 1, 50)
    assert run_key("manual__4", 1000, 14, 1, 50) == base
    for other in (
        run_key("manual__5", 1000, 14, 1, 50),
        run_key("manual__4", 1001, 14, 1, 50),
        run_key("manual__4", 1000, 15, 1, 50),
        run_key("manual__4", 1000, 14, 2, 50),
        run_key("manual__4", 1000, 14, 1, 51),
    ):
        assert other != base


def test_transactional_id_is_stable_and_bounded():
    key = run_key("manual__2026-11-18T10:00:00+00:00", 10**6, 14, 1, 50_000)
    assert transactional_id(key) == transactional_id(key)
    assert transactional_id(key) != transactional_id(key + "x")
    assert len(transactional_id(key)) < 64


def test_producer_config_is_transactional():
    created: list = []
    TransactionalPublisher(
        "fake:9092",
        "settlement-generator-abc",
        producer_factory=lambda cfg: created.append(FakeProducer(cfg, FakeBroker())) or created[-1],
    )
    cfg = created[0].config
    assert cfg["transactional.id"] == "settlement-generator-abc"
    assert cfg["enable.idempotence"] is True
    assert cfg["transaction.timeout.ms"] == 300_000


def test_ingest_reads_committed_only():
    from settlement.config import get_settings
    from settlement.ingest import consumer_config

    cfg = consumer_config(get_settings(), "g")
    assert cfg["isolation.level"] == "read_committed"
    assert cfg["enable.auto.commit"] is False


# --- Postgres ledger (runs under `make test-ops`, skipped without Postgres) ---


@pytest.fixture
def pg_ledger_settings():
    pytest.importorskip("psycopg2")
    from settlement.config import Settings
    from settlement.ops.db import connect
    from settlement.ops.migrate import migrate

    name = f"ledger_t_{uuid.uuid4().hex[:10]}"
    settings = Settings(ops_db_name=name)
    try:
        migrate(settings)
    except Exception as exc:
        if os.getenv("OPS_REQUIRE_POSTGRES") == "1":
            raise
        pytest.skip(f"no Postgres reachable: {exc.__class__.__name__}")
    yield settings
    admin = connect(settings, dbname="postgres")
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
    admin.close()


def test_postgres_ledger_records_once_and_scopes_by_run_key(pg_ledger_settings):
    from settlement.publish_ledger import PostgresChunkLedger

    ledger = PostgresChunkLedger(pg_ledger_settings)
    assert ledger.committed("a") == set()
    ledger.record("a", 0, 10, 4)
    ledger.record("a", 0, 99, 99)  # a resent chunk does not overwrite the first row
    ledger.record("a", 200, 10, 4)
    ledger.record("b", 0, 10, 4)
    assert ledger.committed("a") == {0, 200}
    assert ledger.committed("b") == {0}
