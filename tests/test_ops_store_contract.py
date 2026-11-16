"""One behavioural contract, run against the in-memory store and against Postgres.

The Postgres cases need a server. Point POSTGRES_HOST / POSTGRES_PORT /
POSTGRES_USER / POSTGRES_PASSWORD at one (`make test-ops` starts a throwaway
container) and they run; otherwise they skip. Set OPS_REQUIRE_POSTGRES=1 to make
a missing server a failure, which is what CI does.
"""

from __future__ import annotations

import hashlib
import os
import threading
import uuid
from dataclasses import replace
from datetime import date

import pytest
from ops_memory_store import MemoryCaseStore

from settlement.config import Settings
from settlement.ops.lifecycle import Action, AnalystStatus, InvalidTransition, MissingNote
from settlement.ops.models import BreakRow, QueueFilter
from settlement.ops.service import CaseNotFound, IdempotencyConflict, record_action


def _case_id(ref: str) -> str:
    return hashlib.sha256(f"case:v1:{ref}".encode()).hexdigest()


def brk(
    n: int,
    *,
    break_type: str = "short_paid",
    exposure: int = 1_000,
    day: int = 1,
) -> BreakRow:
    ref = f"TXN_{n:06d}"
    return BreakRow(
        case_id=_case_id(ref),
        transaction_ref=ref,
        merchant_id="MRC_0001",
        channel="card",
        gateway="gw_a",
        bank_code="058",
        currency="NGN",
        succeeded_date=date(2026, 11, day),
        amount_ngn_kobo=10_000,
        expected_net_kobo=9_850,
        settled_net_kobo=None if break_type == "missing_settlement" else 9_000,
        break_type=break_type,
        exposure_kobo=exposure,
    )


def act(store, row: BreakRow, action: Action, key: str, notes: str | None = "checked", who="ada"):
    return record_action(
        store,
        case_id=row.case_id,
        action=action,
        analyst=who,
        notes=notes,
        idempotency_key=key,
    )


# --- Postgres plumbing ------------------------------------------------------


def _pg_settings(dbname: str) -> Settings:
    return Settings(ops_db_name=dbname)


def _admin(settings: Settings):
    from settlement.ops.db import connect

    conn = connect(settings, dbname="postgres")
    conn.autocommit = True
    return conn


@pytest.fixture(scope="module")
def pg_template():
    pytest.importorskip("psycopg2")
    from settlement.ops.migrate import migrate

    name = f"ops_tmpl_{uuid.uuid4().hex[:10]}"
    settings = _pg_settings(name)
    try:
        migrate(settings)
    except Exception as exc:
        if os.getenv("OPS_REQUIRE_POSTGRES") == "1":
            raise
        pytest.skip(f"no Postgres reachable: {exc.__class__.__name__}")
    yield name
    admin = _admin(settings)
    with admin.cursor() as cur:
        cur.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
    admin.close()


@pytest.fixture
def pg_store(pg_template):
    from settlement.ops.postgres_store import PostgresCaseStore

    name = f"ops_t_{uuid.uuid4().hex[:10]}"
    admin = _admin(_pg_settings(name))
    with admin.cursor() as cur:
        cur.execute(f'CREATE DATABASE "{name}" TEMPLATE "{pg_template}"')
    store = PostgresCaseStore(_pg_settings(name), max_connections=10)
    yield store
    store.close()
    with admin.cursor() as cur:
        cur.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
    admin.close()


@pytest.fixture(params=["memory", "postgres"])
def store(request):
    if request.param == "memory":
        return MemoryCaseStore()
    return request.getfixturevalue("pg_store")


# --- Contract ---------------------------------------------------------------


def test_sync_opens_cases_and_is_idempotent(store):
    rows = [brk(1), brk(2, break_type="overpaid")]
    assert store.sync_breaks(iter(rows)) == (2, 0)
    assert store.sync_breaks(iter(rows)) == (2, 0)
    cases, total = store.list_cases(QueueFilter(view="all"))
    assert total == 2
    assert {c.effective_status for c in cases} == {"OPEN"}
    assert all(c.display_id.startswith("CASE-") and len(c.display_id) == 21 for c in cases)


def test_card_payments_have_no_bank_code(store):
    card = replace(brk(1), channel="card", bank_code=None)
    store.sync_breaks(iter([card]))
    assert store.get_case(card.case_id).bank_code is None


def test_a_case_that_stops_being_a_break_is_cleared_not_deleted(store):
    a, b = brk(1), brk(2)
    store.sync_breaks(iter([a, b]))
    assert store.sync_breaks(iter([a])) == (1, 1)
    assert store.get_case(b.case_id).effective_status == "CLEARED"
    assert store.sync_breaks(iter([a, b])) == (2, 0)
    assert store.get_case(b.case_id).effective_status == "OPEN"


def test_a_decision_survives_every_pipeline_rerun(store):
    row = brk(1)
    store.sync_breaks(iter([row]))
    act(store, row, Action.CLOSE, "key-close-0001", notes="bank confirmed payout")

    for _ in range(3):
        store.sync_breaks(iter([row]))

    case = store.get_case(row.case_id)
    assert case.effective_status == "CLOSED"
    assert case.analyst_status is AnalystStatus.CLOSED
    assert not case.drifted
    assert len(store.list_actions(row.case_id)) == 1


def test_a_decision_survives_the_break_disappearing_and_returning(store):
    row = brk(1)
    store.sync_breaks(iter([row]))
    act(store, row, Action.CLOSE, "key-close-0001")
    store.sync_breaks(iter([]))
    assert store.get_case(row.case_id).effective_status == "CLOSED"
    store.sync_breaks(iter([row]))
    assert store.get_case(row.case_id).effective_status == "CLOSED"


def test_a_closed_case_whose_facts_change_is_flagged_not_reopened(store):
    row = brk(1, exposure=1_000)
    store.sync_breaks(iter([row]))
    act(store, row, Action.CLOSE, "key-close-0001")

    worse = brk(1, exposure=2_500)
    store.sync_breaks(iter([worse]))

    case = store.get_case(row.case_id)
    assert case.effective_status == "CLOSED"
    assert case.drifted
    assert case.exposure_kobo == 2_500
    active, _ = store.list_cases(QueueFilter(view="active"))
    assert [c.case_id for c in active] == [row.case_id]
    closed, _ = store.list_cases(QueueFilter(view="closed"))
    assert closed == []


def test_reopen_returns_the_case_to_the_system_status(store):
    row = brk(1)
    store.sync_breaks(iter([row]))
    act(store, row, Action.CLOSE, "key-close-0001")
    act(store, row, Action.REOPEN, "key-reopen-001", notes="payout bounced")
    case = store.get_case(row.case_id)
    assert case.analyst_status is None
    assert case.effective_status == "OPEN"


def test_invalid_transition_writes_nothing(store):
    row = brk(1)
    store.sync_breaks(iter([row]))
    with pytest.raises(InvalidTransition):
        act(store, row, Action.REOPEN, "key-reopen-001")
    assert store.list_actions(row.case_id) == []
    assert store.get_case(row.case_id).analyst_status is None


def test_missing_note_writes_nothing(store):
    row = brk(1)
    store.sync_breaks(iter([row]))
    with pytest.raises(MissingNote):
        act(store, row, Action.CLOSE, "key-close-0001", notes="  ")
    assert store.list_actions(row.case_id) == []


def test_unknown_case_is_reported(store):
    with pytest.raises(CaseNotFound):
        act(store, brk(99), Action.CLOSE, "key-close-0001")


def test_replaying_a_request_returns_the_original_result(store):
    row = brk(1)
    store.sync_breaks(iter([row]))
    first = act(store, row, Action.ESCALATE, "key-escalate-1")
    again = act(store, row, Action.ESCALATE, "key-escalate-1")
    assert not first.replayed and again.replayed
    assert again.action.action_id == first.action.action_id
    assert len(store.list_actions(row.case_id)) == 1


def test_reusing_a_key_for_a_different_request_is_rejected(store):
    a, b = brk(1), brk(2)
    store.sync_breaks(iter([a, b]))
    act(store, a, Action.ESCALATE, "key-shared-001")
    with pytest.raises(IdempotencyConflict):
        act(store, b, Action.ESCALATE, "key-shared-001")
    with pytest.raises(IdempotencyConflict):
        act(store, a, Action.CLOSE, "key-shared-001")


def test_concurrent_identical_requests_record_one_action(store):
    row = brk(1)
    store.sync_breaks(iter([row]))
    results: list = []
    errors: list = []

    def worker():
        try:
            results.append(act(store, row, Action.ESCALATE, "key-race-00001"))
        except Exception as exc:  # collected and asserted below
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    assert sum(1 for r in results if not r.replayed) == 1
    assert len(store.list_actions(row.case_id)) == 1


def test_queue_is_ordered_by_exposure_then_age(store):
    rows = [
        brk(1, exposure=500, day=3),
        brk(2, exposure=9_000, day=5),
        brk(3, exposure=500, day=1),
        brk(4, exposure=2_000, day=2),
    ]
    store.sync_breaks(iter(rows))
    cases, total = store.list_cases(QueueFilter(view="active"))
    assert total == 4
    assert [c.transaction_ref for c in cases] == [
        "TXN_000002",
        "TXN_000004",
        "TXN_000003",
        "TXN_000001",
    ]


def test_queue_paginates_and_filters(store):
    rows = [brk(i, exposure=i * 10) for i in range(1, 8)] + [
        brk(20, break_type="overpaid", exposure=5)
    ]
    store.sync_breaks(iter(rows))
    page, total = store.list_cases(QueueFilter(view="active", limit=3, offset=3))
    assert total == 8 and len(page) == 3
    only, n = store.list_cases(QueueFilter(view="active", break_type="overpaid"))
    assert n == 1 and only[0].break_type == "overpaid"


def test_closed_cases_leave_the_active_queue(store):
    a, b = brk(1), brk(2)
    store.sync_breaks(iter([a, b]))
    act(store, a, Action.CLOSE, "key-close-0001")
    active, n = store.list_cases(QueueFilter(view="active"))
    assert n == 1 and active[0].case_id == b.case_id
    closed, m = store.list_cases(QueueFilter(view="closed"))
    assert m == 1 and closed[0].case_id == a.case_id


def test_summary_counts_active_work_in_integer_kobo(store):
    store.sync_breaks(
        iter(
            [
                brk(1, exposure=1_000),
                brk(2, exposure=2_000),
                brk(3, break_type="overpaid", exposure=300),
                brk(4, exposure=50_000),
            ]
        )
    )
    act(store, brk(4), Action.CLOSE, "key-close-0001")
    s = store.summary()
    assert s.active_cases == 3
    assert s.active_exposure_kobo == 3_300
    assert [(b.break_type, b.cases, b.exposure_kobo) for b in s.by_break_type] == [
        ("short_paid", 2, 3_000),
        ("overpaid", 1, 300),
    ]


def test_exposure_beyond_float_precision_is_exact(store):
    big = 2**53 + 1
    store.sync_breaks(iter([brk(1, exposure=big)]))
    assert store.get_case(brk(1).case_id).exposure_kobo == big
    assert store.summary().active_exposure_kobo == big


# --- Postgres-only guarantees ----------------------------------------------


def test_audit_trail_cannot_be_rewritten(pg_store):
    import psycopg2

    row = brk(1)
    pg_store.sync_breaks(iter([row]))
    act(pg_store, row, Action.CLOSE, "key-close-0001")
    with pytest.raises(psycopg2.Error, match="append-only"), pg_store.transaction() as tx:
        tx._cur.execute("UPDATE ops.case_actions SET notes = 'edited'")
    with pytest.raises(psycopg2.Error, match="append-only"), pg_store.transaction() as tx:
        tx._cur.execute("DELETE FROM ops.case_actions")
    with pytest.raises(psycopg2.Error, match="append-only"), pg_store.transaction() as tx:
        tx._cur.execute("TRUNCATE ops.case_actions")
    assert len(pg_store.list_actions(row.case_id)) == 1


def test_database_rejects_negative_exposure(pg_store):
    import psycopg2

    with pytest.raises(psycopg2.IntegrityError):
        pg_store.sync_breaks(iter([brk(1, exposure=-1)]))
    assert pg_store.list_cases(QueueFilter(view="all"))[1] == 0


def test_a_failed_sync_leaves_existing_cases_untouched(pg_store):
    import psycopg2

    good = brk(1)
    pg_store.sync_breaks(iter([good]))
    act(pg_store, good, Action.CLOSE, "key-close-0001")

    def broken():
        yield brk(2)
        raise RuntimeError("source failed mid-stream")

    with pytest.raises(RuntimeError):
        pg_store.sync_breaks(broken())

    assert pg_store.get_case(good.case_id).effective_status == "CLOSED"
    assert pg_store.get_case(brk(2).case_id) is None
    assert psycopg2  # imported to fail early when the driver is missing


def test_migrations_are_idempotent_and_immutable(pg_template, tmp_path):
    from settlement.ops.migrate import MigrationError, discover, migrate

    settings = _pg_settings(pg_template)
    assert migrate(settings) == []

    copy_dir = tmp_path / "migrations"
    copy_dir.mkdir()
    for version, _, sql in discover():
        (copy_dir / f"{version}.sql").write_text(sql + "\n-- edited later\n")
    with pytest.raises(MigrationError):
        migrate(settings, copy_dir)
