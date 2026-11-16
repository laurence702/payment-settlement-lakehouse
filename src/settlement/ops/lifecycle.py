"""Case lifecycle rules. Pure functions, no I/O.

A case has two independent states:

  * the system status, owned by the pipeline: is this still a break in the
    latest run?
  * the analyst status, owned by people: what has been decided about it?

The pipeline sync only ever writes the first. Analyst actions only ever write
the second. Because the two never share a column, no pipeline run can overwrite
a decision, and the status an analyst sees is derived rather than stored.
"""

from __future__ import annotations

from enum import StrEnum


class SystemStatus(StrEnum):
    OPEN = "OPEN"
    CLEARED = "CLEARED"


class AnalystStatus(StrEnum):
    MANUAL_REVIEW = "MANUAL_REVIEW"
    ESCALATED = "ESCALATED"
    EXPECTED_DELAY = "EXPECTED_DELAY"
    CLOSED = "CLOSED"


class Action(StrEnum):
    START_REVIEW = "START_REVIEW"
    ESCALATE = "ESCALATE"
    MARK_EXPECTED_DELAY = "MARK_EXPECTED_DELAY"
    CLOSE = "CLOSE"
    REOPEN = "REOPEN"


class InvalidTransition(ValueError):
    """The action is not allowed from the case's current analyst status."""


class MissingNote(ValueError):
    """The action requires a justification and none was given."""


# None means no analyst has touched the case yet, so the system status governs.
_TRANSITIONS: dict[AnalystStatus | None, dict[Action, AnalystStatus | None]] = {
    None: {
        Action.START_REVIEW: AnalystStatus.MANUAL_REVIEW,
        Action.ESCALATE: AnalystStatus.ESCALATED,
        Action.MARK_EXPECTED_DELAY: AnalystStatus.EXPECTED_DELAY,
        Action.CLOSE: AnalystStatus.CLOSED,
    },
    AnalystStatus.MANUAL_REVIEW: {
        Action.ESCALATE: AnalystStatus.ESCALATED,
        Action.MARK_EXPECTED_DELAY: AnalystStatus.EXPECTED_DELAY,
        Action.CLOSE: AnalystStatus.CLOSED,
    },
    AnalystStatus.ESCALATED: {
        Action.CLOSE: AnalystStatus.CLOSED,
    },
    AnalystStatus.EXPECTED_DELAY: {
        Action.START_REVIEW: AnalystStatus.MANUAL_REVIEW,
        Action.ESCALATE: AnalystStatus.ESCALATED,
        Action.CLOSE: AnalystStatus.CLOSED,
    },
    AnalystStatus.CLOSED: {
        Action.REOPEN: None,
    },
}

# Decisions that end or hand off a case must say why. The audit trail is only
# useful if the reason is in it.
_NOTE_REQUIRED = frozenset({Action.ESCALATE, Action.CLOSE, Action.REOPEN})

# Analyst states that assert something about the facts, and so can go stale.
_FACT_ASSERTING = frozenset({AnalystStatus.CLOSED, AnalystStatus.EXPECTED_DELAY})


def allowed_actions(current: AnalystStatus | None) -> frozenset[Action]:
    return frozenset(_TRANSITIONS[current])


def apply_action(current: AnalystStatus | None, action: Action) -> AnalystStatus | None:
    """Return the analyst status after `action`, or raise InvalidTransition."""
    try:
        return _TRANSITIONS[current][action]
    except KeyError:
        label = current.value if current else "UNTOUCHED"
        raise InvalidTransition(f"{action.value} is not allowed from {label}") from None


def require_note(action: Action, notes: str | None) -> str:
    """Return the trimmed note, raising MissingNote when the action needs one."""
    cleaned = (notes or "").strip()
    if action in _NOTE_REQUIRED and not cleaned:
        raise MissingNote(f"{action.value} requires a note")
    return cleaned


def system_status(present_in_latest_run: bool) -> SystemStatus:
    return SystemStatus.OPEN if present_in_latest_run else SystemStatus.CLEARED


def effective_status(system: SystemStatus, analyst: AnalystStatus | None) -> str:
    """The status a person sees: their own decision when there is one."""
    return (analyst or system).value


def display_id(case_id: str) -> str:
    """Human-facing label. Never a key: sixteen hex characters is 64 bits."""
    return f"CASE-{case_id[:16].upper()}"


def fingerprint(break_type: str, exposure_kobo: int) -> str:
    """The facts a decision is made against. Readable on purpose."""
    return f"{break_type}:{exposure_kobo}"


def is_drifted(analyst: AnalystStatus | None, recorded: str | None, current: str | None) -> bool:
    """True when a decision rests on facts that have since changed.

    A closed case whose break type or exposure moved is not reopened behind
    the analyst's back, and it is not silently trusted either. It is flagged.
    """
    if analyst not in _FACT_ASSERTING or recorded is None or current is None:
        return False
    return recorded != current
