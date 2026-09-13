from naijapay.schemas import (
    BANKS,
    STATUS_RANK,
    TERMINAL_STATUSES,
    Channel,
    Gateway,
    Status,
)


def test_status_rank_covers_every_status():
    assert set(STATUS_RANK) == set(Status.ALL)


def test_status_rank_is_a_total_order():
    assert len(set(STATUS_RANK.values())) == len(STATUS_RANK)


def test_pending_ranks_below_every_terminal_status():
    assert all(STATUS_RANK[s] > STATUS_RANK[Status.PENDING] for s in TERMINAL_STATUSES)


def test_reversed_outranks_success():
    # A reversal always supersedes the success it reverses, even if a clock skew
    # gives them the same updated_at.
    assert STATUS_RANK[Status.REVERSED] > STATUS_RANK[Status.SUCCESS]


def test_terminal_statuses_exclude_pending():
    assert Status.PENDING not in TERMINAL_STATUSES
    assert TERMINAL_STATUSES == {Status.SUCCESS, Status.FAILED, Status.REVERSED}


def test_bank_codes_are_unique_and_stringy():
    codes = [c for c, _ in BANKS]
    assert len(codes) == len(set(codes))
    assert all(isinstance(c, str) and c.isdigit() for c in codes)


def test_enum_constants_stringify_to_their_wire_value():
    # The whole reason these are plain strings and not enums.
    assert str(Channel.CARD) == "card"
    assert str(Status.SUCCESS) == "success"
    assert str(Gateway.PAYSTACK) == "paystack"
