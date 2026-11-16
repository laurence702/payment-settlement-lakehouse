"""Records exchanged between the ops store, the service and the console."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from settlement.ops.lifecycle import Action, AnalystStatus, SystemStatus


@dataclass(frozen=True)
class BreakRow:
    """One current break, as the pipeline sync reads it from the serving layer."""

    case_id: str
    transaction_ref: str
    merchant_id: str
    channel: str
    gateway: str
    bank_code: str
    currency: str
    succeeded_date: date
    amount_ngn_kobo: int
    expected_net_kobo: int
    settled_net_kobo: int | None
    break_type: str
    exposure_kobo: int


@dataclass(frozen=True)
class CaseView:
    case_id: str
    display_id: str
    transaction_ref: str
    merchant_id: str
    channel: str
    gateway: str
    bank_code: str
    currency: str
    succeeded_date: date
    amount_ngn_kobo: int
    expected_net_kobo: int
    settled_net_kobo: int | None
    break_type: str
    exposure_kobo: int
    system_status: SystemStatus
    system_fingerprint: str
    analyst_status: AnalystStatus | None
    decided_fingerprint: str | None
    effective_status: str
    drifted: bool
    first_seen_at: datetime
    last_seen_at: datetime


@dataclass(frozen=True)
class ActionView:
    action_id: int
    case_id: str
    analyst: str
    action: Action
    notes: str
    idempotency_key: str
    resulting_status: AnalystStatus | None
    fingerprint_at_action: str
    created_at: datetime


@dataclass(frozen=True)
class CaseState:
    """What the write path needs to decide a transition."""

    case_id: str
    analyst_status: AnalystStatus | None
    system_fingerprint: str


@dataclass(frozen=True)
class QueueFilter:
    view: str = "active"
    break_type: str | None = None
    limit: int = 50
    offset: int = 0


@dataclass(frozen=True)
class BreakTypeSummary:
    break_type: str
    cases: int
    exposure_kobo: int


@dataclass(frozen=True)
class QueueSummary:
    active_cases: int
    active_exposure_kobo: int
    drifted_cases: int
    by_break_type: tuple[BreakTypeSummary, ...]


QUEUE_VIEWS = ("active", "closed", "cleared", "all")
