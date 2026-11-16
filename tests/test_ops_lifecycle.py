from __future__ import annotations

import itertools

import pytest

from settlement.ops.lifecycle import (
    Action,
    AnalystStatus,
    InvalidTransition,
    MissingNote,
    SystemStatus,
    allowed_actions,
    apply_action,
    effective_status,
    fingerprint,
    is_drifted,
    require_note,
    system_status,
)
from settlement.ops.money import format_ngn

ALL_STATES = [None, *AnalystStatus]

# The complete, intended transition table. The implementation is checked against
# this literal so an accidental edit to the rules fails loudly.
EXPECTED: dict[AnalystStatus | None, dict[Action, AnalystStatus | None]] = {
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
    AnalystStatus.ESCALATED: {Action.CLOSE: AnalystStatus.CLOSED},
    AnalystStatus.EXPECTED_DELAY: {
        Action.START_REVIEW: AnalystStatus.MANUAL_REVIEW,
        Action.ESCALATE: AnalystStatus.ESCALATED,
        Action.CLOSE: AnalystStatus.CLOSED,
    },
    AnalystStatus.CLOSED: {Action.REOPEN: None},
}


@pytest.mark.parametrize(("state", "action"), list(itertools.product(ALL_STATES, Action)))
def test_every_state_action_pair_matches_the_table(state, action):
    expected = EXPECTED[state]
    if action in expected:
        assert apply_action(state, action) == expected[action]
    else:
        with pytest.raises(InvalidTransition):
            apply_action(state, action)


@pytest.mark.parametrize("state", ALL_STATES)
def test_allowed_actions_agree_with_apply_action(state):
    assert allowed_actions(state) == frozenset(EXPECTED[state])


def test_closed_is_only_left_through_reopen():
    assert allowed_actions(AnalystStatus.CLOSED) == {Action.REOPEN}


def test_a_decision_is_never_lost_to_a_system_status_change():
    for analyst in AnalystStatus:
        for system in SystemStatus:
            assert effective_status(system, analyst) == analyst.value


def test_without_a_decision_the_system_status_governs():
    assert effective_status(SystemStatus.OPEN, None) == "OPEN"
    assert effective_status(SystemStatus.CLEARED, None) == "CLEARED"


def test_system_status_follows_presence_in_the_latest_run():
    assert system_status(True) is SystemStatus.OPEN
    assert system_status(False) is SystemStatus.CLEARED


@pytest.mark.parametrize("action", [Action.ESCALATE, Action.CLOSE, Action.REOPEN])
@pytest.mark.parametrize("notes", [None, "", "   \n"])
def test_deciding_actions_require_a_note(action, notes):
    with pytest.raises(MissingNote):
        require_note(action, notes)


def test_other_actions_accept_an_empty_note():
    assert require_note(Action.START_REVIEW, None) == ""
    assert require_note(Action.MARK_EXPECTED_DELAY, "  ") == ""


def test_notes_are_trimmed():
    assert require_note(Action.CLOSE, "  bank confirmed  ") == "bank confirmed"


def test_fingerprint_changes_when_the_facts_change():
    base = fingerprint("short_paid", 1500)
    assert base == fingerprint("short_paid", 1500)
    assert base != fingerprint("short_paid", 1501)
    assert base != fingerprint("overpaid", 1500)


@pytest.mark.parametrize("analyst", [AnalystStatus.CLOSED, AnalystStatus.EXPECTED_DELAY])
def test_a_decision_on_changed_facts_is_flagged(analyst):
    assert is_drifted(analyst, "short_paid:1500", "short_paid:900")
    assert not is_drifted(analyst, "short_paid:1500", "short_paid:1500")


@pytest.mark.parametrize("analyst", [None, AnalystStatus.MANUAL_REVIEW, AnalystStatus.ESCALATED])
def test_open_work_is_never_flagged_as_drifted(analyst):
    assert not is_drifted(analyst, "short_paid:1500", "short_paid:900")


def test_missing_fingerprints_are_not_drift():
    assert not is_drifted(AnalystStatus.CLOSED, None, "short_paid:900")
    assert not is_drifted(AnalystStatus.CLOSED, "short_paid:900", None)


@pytest.mark.parametrize(
    ("kobo", "text"),
    [
        (0, "0.00"),
        (5, "0.05"),
        (100, "1.00"),
        (123456789, "1,234,567.89"),
        (-250, "-2.50"),
    ],
)
def test_format_ngn_is_exact_integer_math(kobo, text):
    assert format_ngn(kobo) == text


def test_format_ngn_is_exact_beyond_float_precision():
    # 2**53 + 1 is not representable as a float; integer math must keep it.
    assert format_ngn(2**53 + 1) == "90,071,992,547,409.93"
