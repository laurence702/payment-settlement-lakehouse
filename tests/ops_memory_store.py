"""In-memory CaseStore used by fast tests.

It is held to the same contract as the Postgres store by
tests/test_ops_store_contract.py, which runs every test against both. If this
fake and Postgres ever disagree, a contract test fails.
"""

from __future__ import annotations

import copy
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from itertools import count

from settlement.ops.lifecycle import (
    AnalystStatus,
    SystemStatus,
    display_id,
    effective_status,
    fingerprint,
    is_drifted,
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


@dataclass
class _Case:
    row: BreakRow
    system_status: SystemStatus
    system_fingerprint: str
    first_seen_at: datetime
    last_seen_at: datetime
    analyst_status: AnalystStatus | None = None
    decided_fingerprint: str | None = None


@dataclass
class _State:
    cases: dict[str, _Case] = field(default_factory=dict)
    actions: list[ActionView] = field(default_factory=list)


def _now() -> datetime:
    return datetime.now(UTC)


class _Tx:
    def __init__(self, store: MemoryCaseStore) -> None:
        self._s = store

    def lock_case(self, case_id: str) -> CaseState | None:
        case = self._s._state.cases.get(case_id)
        if case is None:
            return None
        return CaseState(case_id, case.analyst_status, case.system_fingerprint)

    def find_action(self, idempotency_key: str) -> ActionView | None:
        return next(
            (a for a in self._s._state.actions if a.idempotency_key == idempotency_key), None
        )

    def insert_action(
        self,
        *,
        case_id,
        analyst,
        action,
        notes,
        idempotency_key,
        resulting_status,
        fingerprint_at_action,
    ) -> ActionView | None:
        if self.find_action(idempotency_key) is not None:
            return None
        view = ActionView(
            action_id=next(self._s._ids),
            case_id=case_id,
            analyst=analyst,
            action=action,
            notes=notes,
            idempotency_key=idempotency_key,
            resulting_status=resulting_status,
            fingerprint_at_action=fingerprint_at_action,
            created_at=_now(),
        )
        self._s._state.actions.append(view)
        return view

    def set_analyst_state(self, case_id, status, decided_fingerprint) -> None:
        case = self._s._state.cases[case_id]
        case.analyst_status = status
        case.decided_fingerprint = decided_fingerprint


class MemoryCaseStore:
    def __init__(self) -> None:
        self._state = _State()
        self._ids = count(1)
        self._lock = threading.RLock()

    def close(self) -> None:
        return None

    @contextmanager
    def transaction(self):
        # One lock for the whole store stands in for Postgres row locks, and a
        # snapshot restores state on failure the way a rollback would.
        with self._lock:
            snapshot = copy.deepcopy(self._state)
            try:
                yield _Tx(self)
            except BaseException:
                self._state = snapshot
                raise

    def _view(self, c: _Case) -> CaseView:
        r = c.row
        return CaseView(
            case_id=r.case_id,
            display_id=display_id(r.case_id),
            transaction_ref=r.transaction_ref,
            merchant_id=r.merchant_id,
            channel=r.channel,
            gateway=r.gateway,
            bank_code=r.bank_code,
            currency=r.currency,
            succeeded_date=r.succeeded_date,
            amount_ngn_kobo=r.amount_ngn_kobo,
            expected_net_kobo=r.expected_net_kobo,
            settled_net_kobo=r.settled_net_kobo,
            break_type=r.break_type,
            exposure_kobo=r.exposure_kobo,
            system_status=c.system_status,
            system_fingerprint=c.system_fingerprint,
            analyst_status=c.analyst_status,
            decided_fingerprint=c.decided_fingerprint,
            effective_status=effective_status(c.system_status, c.analyst_status),
            drifted=is_drifted(c.analyst_status, c.decided_fingerprint, c.system_fingerprint),
            first_seen_at=c.first_seen_at,
            last_seen_at=c.last_seen_at,
        )

    @staticmethod
    def _in_view(view: str, v: CaseView) -> bool:
        if view == "active":
            return v.effective_status not in ("CLOSED", "CLEARED") or v.drifted
        if view == "closed":
            return v.effective_status == "CLOSED" and not v.drifted
        if view == "cleared":
            return v.effective_status == "CLEARED"
        return True

    def list_cases(self, query: QueueFilter) -> tuple[list[CaseView], int]:
        with self._lock:
            views = [self._view(c) for c in self._state.cases.values()]
        views = [
            v
            for v in views
            if self._in_view(query.view, v)
            and (query.break_type is None or v.break_type == query.break_type)
        ]
        views.sort(key=lambda v: (-v.exposure_kobo, v.succeeded_date, v.case_id))
        return views[query.offset : query.offset + query.limit], len(views)

    def get_case(self, case_id: str) -> CaseView | None:
        with self._lock:
            case = self._state.cases.get(case_id)
            return self._view(case) if case else None

    def list_actions(self, case_id: str) -> list[ActionView]:
        with self._lock:
            return [a for a in self._state.actions if a.case_id == case_id]

    def summary(self) -> QueueSummary:
        with self._lock:
            views = [self._view(c) for c in self._state.cases.values()]
        active = [v for v in views if self._in_view("active", v)]
        grouped: dict[str, list[CaseView]] = {}
        for v in active:
            grouped.setdefault(v.break_type, []).append(v)
        by_type = sorted(
            (
                BreakTypeSummary(t, len(vs), sum(v.exposure_kobo for v in vs))
                for t, vs in grouped.items()
            ),
            key=lambda b: (-b.exposure_kobo, b.break_type),
        )
        return QueueSummary(
            active_cases=len(active),
            active_exposure_kobo=sum(v.exposure_kobo for v in active),
            drifted_cases=sum(1 for v in views if v.drifted),
            by_break_type=tuple(by_type),
        )

    def sync_breaks(self, rows: Iterator[BreakRow]) -> tuple[int, int]:
        with self._lock:
            batch = {r.case_id: r for r in rows}
            now = _now()
            for case_id, r in batch.items():
                existing = self._state.cases.get(case_id)
                fp = fingerprint(r.break_type, r.exposure_kobo)
                if existing is None:
                    self._state.cases[case_id] = _Case(r, SystemStatus.OPEN, fp, now, now)
                else:
                    existing.row = replace(r)
                    existing.system_status = SystemStatus.OPEN
                    existing.system_fingerprint = fp
                    existing.last_seen_at = now
            cleared = 0
            for case_id, case in self._state.cases.items():
                if case_id not in batch and case.system_status is SystemStatus.OPEN:
                    case.system_status = SystemStatus.CLEARED
                    cleared += 1
            return len(batch), cleared
