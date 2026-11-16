"""Recording an analyst action. The only code path that writes analyst state.

The store is a Protocol so this logic is tested against an in-memory fake and
against Postgres with the same contract tests. The transactional guarantees
(row lock, unique idempotency key, append-only trail) live in the Postgres
store; this module decides what is allowed.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import Protocol

from settlement.ops.lifecycle import Action, AnalystStatus, apply_action, require_note
from settlement.ops.models import (
    ActionView,
    BreakRow,
    CaseState,
    CaseView,
    QueueFilter,
    QueueSummary,
)


class CaseNotFound(LookupError):
    pass


class IdempotencyConflict(ValueError):
    """The idempotency key was already used for a different request."""


class CaseTransaction(Protocol):
    def lock_case(self, case_id: str) -> CaseState | None:
        """Return the case and hold its row lock until the transaction ends."""

    def find_action(self, idempotency_key: str) -> ActionView | None: ...

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
        """Append to the audit trail. None when the idempotency key already exists."""

    def set_analyst_state(
        self, case_id: str, status: AnalystStatus | None, decided_fingerprint: str | None
    ) -> None: ...


class CaseStore(Protocol):
    def transaction(self) -> AbstractContextManager[CaseTransaction]: ...

    def list_cases(self, query: QueueFilter) -> tuple[list[CaseView], int]:
        """Return one page and the total number of cases matching the filter."""

    def get_case(self, case_id: str) -> CaseView | None: ...

    def list_actions(self, case_id: str) -> list[ActionView]: ...

    def summary(self) -> QueueSummary: ...

    def sync_breaks(self, rows: Iterator[BreakRow]) -> tuple[int, int]:
        """Reconcile system columns with the latest breaks. Returns (open, cleared)."""


@dataclass(frozen=True)
class ActionResult:
    action: ActionView
    replayed: bool


def record_action(
    store: CaseStore,
    *,
    case_id: str,
    action: Action,
    analyst: str,
    notes: str | None,
    idempotency_key: str,
) -> ActionResult:
    """Apply one analyst action atomically and idempotently.

    The case row is locked first, so concurrent requests on one case are
    serialised and a retry always observes the first attempt's result. A key
    that was already used replays the stored result when the request matches
    and is rejected when it does not.
    """
    analyst = analyst.strip()
    if not analyst:
        raise ValueError("analyst is required")

    with store.transaction() as tx:
        case = tx.lock_case(case_id)
        if case is None:
            raise CaseNotFound(case_id)

        existing = tx.find_action(idempotency_key)
        if existing is not None:
            if existing.case_id != case_id or existing.action != action:
                raise IdempotencyConflict(
                    "idempotency key was already used for a different request"
                )
            return ActionResult(existing, replayed=True)

        cleaned = require_note(action, notes)
        new_status = apply_action(case.analyst_status, action)
        recorded = tx.insert_action(
            case_id=case_id,
            analyst=analyst,
            action=action,
            notes=cleaned,
            idempotency_key=idempotency_key,
            resulting_status=new_status,
            fingerprint_at_action=case.system_fingerprint,
        )
        if recorded is None:
            raise IdempotencyConflict("idempotency key was used concurrently")
        tx.set_analyst_state(
            case_id,
            new_status,
            case.system_fingerprint if new_status is not None else None,
        )
        return ActionResult(recorded, replayed=False)
