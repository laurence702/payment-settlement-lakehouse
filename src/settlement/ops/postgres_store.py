"""Postgres implementation of the case store.

All SQL here is parameterised. The only text interpolated into a statement is
chosen from the fixed mappings in this module, never from request input.
"""

from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import contextmanager
from itertools import islice

from settlement.config import Settings
from settlement.ops.lifecycle import (
    Action,
    AnalystStatus,
    SystemStatus,
    display_id,
    fingerprint,
)
from settlement.ops.models import (
    ActionView,
    BreakRow,
    BreakTypeSummary,
    CaseState,
    CaseView,
    QueueFilter,
    QueueSummary,
)

_CASE_COLUMNS = (
    "case_id, display_id, transaction_ref, merchant_id, channel, gateway, bank_code, "
    "currency, succeeded_date, amount_ngn_kobo, expected_net_kobo, settled_net_kobo, "
    "break_type, exposure_kobo, system_status, system_fingerprint, analyst_status, "
    "decided_fingerprint, effective_status, drifted, first_seen_at, last_seen_at"
)

_ACTION_COLUMNS = (
    "action_id, case_id, analyst, action, notes, idempotency_key, resulting_status, "
    "fingerprint_at_action, created_at"
)

_VIEW_PREDICATES = {
    "active": "(effective_status NOT IN ('CLOSED', 'CLEARED') OR drifted)",
    "closed": "(effective_status = 'CLOSED' AND NOT drifted)",
    "cleared": "(effective_status = 'CLEARED')",
    "all": "TRUE",
}

_SYNC_PAGE = 5000


def _case(row) -> CaseView:
    return CaseView(
        case_id=row[0],
        display_id=row[1],
        transaction_ref=row[2],
        merchant_id=row[3],
        channel=row[4],
        gateway=row[5],
        bank_code=row[6],
        currency=row[7],
        succeeded_date=row[8],
        amount_ngn_kobo=row[9],
        expected_net_kobo=row[10],
        settled_net_kobo=row[11],
        break_type=row[12],
        exposure_kobo=row[13],
        system_status=SystemStatus(row[14]),
        system_fingerprint=row[15],
        analyst_status=AnalystStatus(row[16]) if row[16] else None,
        decided_fingerprint=row[17],
        effective_status=row[18],
        drifted=row[19],
        first_seen_at=row[20],
        last_seen_at=row[21],
    )


def _action(row) -> ActionView:
    return ActionView(
        action_id=row[0],
        case_id=row[1],
        analyst=row[2],
        action=Action(row[3]),
        notes=row[4],
        idempotency_key=row[5],
        resulting_status=AnalystStatus(row[6]) if row[6] else None,
        fingerprint_at_action=row[7],
        created_at=row[8],
    )


class _Transaction:
    def __init__(self, cursor) -> None:
        self._cur = cursor

    def lock_case(self, case_id: str) -> CaseState | None:
        self._cur.execute(
            "SELECT case_id, analyst_status, system_fingerprint FROM ops.cases "
            "WHERE case_id = %s FOR UPDATE",
            (case_id,),
        )
        row = self._cur.fetchone()
        if row is None:
            return None
        return CaseState(row[0], AnalystStatus(row[1]) if row[1] else None, row[2])

    def find_action(self, idempotency_key: str) -> ActionView | None:
        self._cur.execute(
            f"SELECT {_ACTION_COLUMNS} FROM ops.case_actions WHERE idempotency_key = %s",
            (idempotency_key,),
        )
        row = self._cur.fetchone()
        return _action(row) if row else None

    def insert_action(
        self,
        *,
        case_id: str,
        analyst: str,
        action: Action,
        notes: str,
        idempotency_key: str,
        resulting_status: AnalystStatus | None,
        fingerprint_at_action: str,
    ) -> ActionView | None:
        self._cur.execute(
            "INSERT INTO ops.case_actions (case_id, analyst, action, notes, idempotency_key, "
            "resulting_status, fingerprint_at_action) VALUES (%s, %s, %s, %s, %s, %s, %s) "
            f"ON CONFLICT (idempotency_key) DO NOTHING RETURNING {_ACTION_COLUMNS}",
            (
                case_id,
                analyst,
                action.value,
                notes,
                idempotency_key,
                resulting_status.value if resulting_status else None,
                fingerprint_at_action,
            ),
        )
        row = self._cur.fetchone()
        return _action(row) if row else None

    def set_analyst_state(
        self, case_id: str, status: AnalystStatus | None, decided_fingerprint: str | None
    ) -> None:
        self._cur.execute(
            "UPDATE ops.cases SET analyst_status = %s, decided_fingerprint = %s, "
            "updated_at = now() WHERE case_id = %s",
            (status.value if status else None, decided_fingerprint, case_id),
        )


class PostgresCaseStore:
    """Connection per unit of work, drawn from a small pool."""

    def __init__(self, settings: Settings, max_connections: int = 4) -> None:
        self._settings = settings
        self._max = max_connections
        self._pool = None
        self._lock = threading.Lock()
        # psycopg2's pool raises when exhausted instead of waiting. Callers
        # should queue, so a bounded semaphore sits in front of it.
        self._slots = threading.BoundedSemaphore(max_connections)

    def _get_pool(self):
        from psycopg2.pool import ThreadedConnectionPool

        with self._lock:
            if self._pool is None:
                s = self._settings
                self._pool = ThreadedConnectionPool(
                    1,
                    self._max,
                    host=s.ops_db_host,
                    port=s.ops_db_port,
                    dbname=s.ops_db_name,
                    user=s.ops_db_user,
                    password=s.ops_db_password,
                    connect_timeout=10,
                    application_name="settlement-ops-console",
                )
            return self._pool

    @contextmanager
    def _cursor(self):
        with self._slots:
            pool = self._get_pool()
            conn = pool.getconn()
            try:
                with conn.cursor() as cur:
                    yield cur
                conn.commit()
            except BaseException:
                conn.rollback()
                raise
            finally:
                pool.putconn(conn)

    def close(self) -> None:
        with self._lock:
            if self._pool is not None:
                self._pool.closeall()
                self._pool = None

    def transaction(self):
        @contextmanager
        def _tx():
            with self._cursor() as cur:
                yield _Transaction(cur)

        return _tx()

    def list_cases(self, query: QueueFilter) -> tuple[list[CaseView], int]:
        predicate = _VIEW_PREDICATES[query.view]
        where = predicate
        params: list = []
        if query.break_type:
            where += " AND break_type = %s"
            params.append(query.break_type)
        with self._cursor() as cur:
            cur.execute(f"SELECT count(*) FROM ops.v_cases_effective WHERE {where}", params)
            total = cur.fetchone()[0]
            cur.execute(
                f"SELECT {_CASE_COLUMNS} FROM ops.v_cases_effective WHERE {where} "
                "ORDER BY exposure_kobo DESC, succeeded_date ASC, case_id ASC "
                "LIMIT %s OFFSET %s",
                [*params, query.limit, query.offset],
            )
            return [_case(r) for r in cur.fetchall()], total

    def get_case(self, case_id: str) -> CaseView | None:
        with self._cursor() as cur:
            cur.execute(
                f"SELECT {_CASE_COLUMNS} FROM ops.v_cases_effective WHERE case_id = %s",
                (case_id,),
            )
            row = cur.fetchone()
            return _case(row) if row else None

    def list_actions(self, case_id: str) -> list[ActionView]:
        with self._cursor() as cur:
            cur.execute(
                f"SELECT {_ACTION_COLUMNS} FROM ops.case_actions "
                "WHERE case_id = %s ORDER BY action_id",
                (case_id,),
            )
            return [_action(r) for r in cur.fetchall()]

    def summary(self) -> QueueSummary:
        active = _VIEW_PREDICATES["active"]
        with self._cursor() as cur:
            cur.execute(
                "SELECT break_type, count(*), COALESCE(sum(exposure_kobo), 0)::bigint "
                f"FROM ops.v_cases_effective WHERE {active} "
                "GROUP BY break_type ORDER BY 3 DESC, 1"
            )
            by_type = tuple(BreakTypeSummary(r[0], r[1], r[2]) for r in cur.fetchall())
            cur.execute("SELECT count(*) FROM ops.v_cases_effective WHERE drifted")
            drifted = cur.fetchone()[0]
        return QueueSummary(
            active_cases=sum(b.cases for b in by_type),
            active_exposure_kobo=sum(b.exposure_kobo for b in by_type),
            drifted_cases=drifted,
            by_break_type=by_type,
        )

    def sync_breaks(self, rows: Iterator[BreakRow]) -> tuple[int, int]:
        """Upsert system columns for the latest breaks and clear the rest.

        Analyst columns are not mentioned anywhere in this method. That is the
        whole guarantee: a pipeline run cannot change a decision.
        """
        from psycopg2.extras import execute_values

        with self._cursor() as cur:
            cur.execute(
                "CREATE TEMP TABLE _sync_batch ("
                "case_id text PRIMARY KEY, display_id text, transaction_ref text, "
                "merchant_id text, channel text, gateway text, bank_code text, currency text, "
                "succeeded_date date, amount_ngn_kobo bigint, expected_net_kobo bigint, "
                "settled_net_kobo bigint, break_type text, exposure_kobo bigint, "
                "system_fingerprint text) ON COMMIT DROP"
            )
            iterator = iter(rows)
            while page := list(islice(iterator, _SYNC_PAGE)):
                execute_values(
                    cur,
                    "INSERT INTO _sync_batch VALUES %s",
                    [
                        (
                            r.case_id,
                            display_id(r.case_id),
                            r.transaction_ref,
                            r.merchant_id,
                            r.channel,
                            r.gateway,
                            r.bank_code,
                            r.currency,
                            r.succeeded_date,
                            r.amount_ngn_kobo,
                            r.expected_net_kobo,
                            r.settled_net_kobo,
                            r.break_type,
                            r.exposure_kobo,
                            fingerprint(r.break_type, r.exposure_kobo),
                        )
                        for r in page
                    ],
                )
            cur.execute("SELECT count(*) FROM _sync_batch")
            current = cur.fetchone()[0]
            cur.execute(
                "INSERT INTO ops.cases (case_id, display_id, transaction_ref, merchant_id, "
                "channel, gateway, bank_code, currency, succeeded_date, amount_ngn_kobo, "
                "expected_net_kobo, settled_net_kobo, break_type, exposure_kobo, "
                "system_status, system_fingerprint) "
                "SELECT case_id, display_id, transaction_ref, merchant_id, channel, gateway, "
                "bank_code, currency, succeeded_date, amount_ngn_kobo, expected_net_kobo, "
                "settled_net_kobo, break_type, exposure_kobo, 'OPEN', system_fingerprint "
                "FROM _sync_batch "
                "ON CONFLICT (case_id) DO UPDATE SET "
                "break_type = EXCLUDED.break_type, "
                "exposure_kobo = EXCLUDED.exposure_kobo, "
                "amount_ngn_kobo = EXCLUDED.amount_ngn_kobo, "
                "expected_net_kobo = EXCLUDED.expected_net_kobo, "
                "settled_net_kobo = EXCLUDED.settled_net_kobo, "
                "system_status = 'OPEN', "
                "system_fingerprint = EXCLUDED.system_fingerprint, "
                "last_seen_at = now(), updated_at = now()"
            )
            cur.execute(
                "UPDATE ops.cases c SET system_status = 'CLEARED', updated_at = now() "
                "WHERE c.system_status = 'OPEN' "
                "AND NOT EXISTS (SELECT 1 FROM _sync_batch b WHERE b.case_id = c.case_id)"
            )
            return current, cur.rowcount
