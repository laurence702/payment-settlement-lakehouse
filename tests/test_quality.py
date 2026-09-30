from __future__ import annotations

from settlement.config import Settings


class _Client:
    """Minimal stub that returns fixture counts keyed by query substrings.

    The mock must mirror the query patterns in quality.run_checks in the same
    order they are evaluated.  Any query that does not match falls through to a
    safe default of "1".
    """

    def __init__(self, matched_inconsistencies: int = 0, grain_mismatch: bool = False):
        self.matched_inconsistencies = matched_inconsistencies
        self.grain_mismatch = grain_mismatch
        # Simulated serving-layer counts
        self._recon_rows = 12  # eligible (10) + reversed-with-settlement (2)
        self._expected_rows = 10 if grain_mismatch else 12

    def command(self, query: str):
        # Duplicate-ref check
        if "SELECT count() FROM (" in query:
            return "0"

        # Check 3: matched rows must carry zero outstanding balance
        if "WHERE is_amount_matched" in query:
            return "0"

        # Check 5b: amount-discrepancy soft check — must be matched before the
        # generic mart count below, because this query also references the mart.
        if "countIf(currency = 'NGN'" in query:
            return (4, 3)

        # Check 4: reconciliation grain tied to fact.
        # The subquery checks reversed rows in fct_settlements.
        if "fct_settlements" in query:
            return str(self._expected_rows)
        if "mart_settlement_reconciliation" in query and "WHERE" not in query:
            return str(self._recon_rows)

        # Check 5a: matched-status consistency
        if "reconciliation_status = 'matched'" in query:
            return str(self.matched_inconsistencies)

        return "1"


def test_quality_accepts_explicit_amount_discrepancies(monkeypatch):
    from settlement import quality

    monkeypatch.setattr(quality, "_client", lambda _: _Client())
    hard, soft = quality.run_checks(Settings())

    assert hard == []
    assert soft == [
        {
            "check": "settlement_amount_discrepancies",
            "ngn_amount_discrepancies": 4,
            "usd_rows_with_variance": 3,
            "note": "review by reconciliation status before escalation",
        }
    ]


def test_quality_rejects_inconsistent_matched_rows(monkeypatch):
    from settlement import quality

    monkeypatch.setattr(quality, "_client", lambda _: _Client(matched_inconsistencies=1))
    hard, _ = quality.run_checks(Settings())

    assert hard == [{"check": "matched_reconciliation_consistency", "rows": 1}]


def test_quality_rejects_grain_mismatch(monkeypatch):
    """Check #4 must detect when the mart row count diverges from the expected
    grain (eligible + reversed-with-settlement).  A mismatch means one of the
    two loads is stale.
    """
    from settlement import quality

    monkeypatch.setattr(quality, "_client", lambda _: _Client(grain_mismatch=True))
    hard, _ = quality.run_checks(Settings())

    assert any(h["check"] == "reconciliation_ties_to_fact" for h in hard)


def test_quality_passes_with_reversed_settlement_rows(monkeypatch):
    """When the mart includes reversed-after-settlement rows, the grain check
    must still pass as long as mart count == eligible + reversed-with-settlement.
    """
    from settlement import quality

    monkeypatch.setattr(quality, "_client", lambda _: _Client(grain_mismatch=False))
    hard, _ = quality.run_checks(Settings())

    assert not any(h["check"] == "reconciliation_ties_to_fact" for h in hard)
