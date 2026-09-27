from __future__ import annotations

from datetime import UTC, datetime

from settlement.ingest import _QUARANTINE_MAX_PAYLOAD_BYTES, quarantine_record


def test_quarantine_record_retains_payload_with_safe_error_metadata():
    ingested_at = datetime(2026, 9, 27, tzinfo=UTC)
    record = quarantine_record(
        b'{"not":"valid"',
        "settlement.transactions.v1",
        ingested_at,
        ValueError("token=secret"),
    )

    assert record == {
        "source_topic": "settlement.transactions.v1",
        "ingested_at": ingested_at,
        "error_type": "ValueError",
        "raw_payload": '{"not":"valid"',
        "payload_truncated": False,
    }
    assert "secret" not in str(record)


def test_quarantine_record_bounds_oversized_payloads():
    record = quarantine_record(
        b"x" * (_QUARANTINE_MAX_PAYLOAD_BYTES + 1),
        "settlement.settlements.v1",
        datetime.now(UTC),
        ValueError(),
    )

    assert record["payload_truncated"] is True
    assert len(record["raw_payload"]) == _QUARANTINE_MAX_PAYLOAD_BYTES
