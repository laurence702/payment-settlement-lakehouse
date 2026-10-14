from __future__ import annotations

from datetime import UTC, datetime

from settlement.ingest import _QUARANTINE_MAX_PAYLOAD_BYTES, _coerce, quarantine_record
from settlement.schemas import settlement_event_schema, transaction_event_schema


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


def test_coerce_drops_undeclared_fields_and_fills_missing_with_null():
    schema = transaction_event_schema()
    raw = {
        "transaction_ref": "tx_12345",
        "merchant_id": "m_1",
        "unexpected_extra_field": "injected_by_producer",
    }
    coerced = _coerce(raw, schema)

    assert coerced["transaction_ref"] == "tx_12345"
    assert coerced["merchant_id"] == "m_1"
    assert "unexpected_extra_field" not in coerced
    assert coerced["amount_kobo"] is None
    assert "ingested_at" not in coerced


def test_coerce_parses_iso_timestamps_and_settlement_dates():
    settlement_schema = settlement_event_schema()
    settlement_raw = {
        "settlement_id": "stl_001",
        "transaction_ref": "tx_12345",
        "event_ts": "2026-09-27T10:15:30+00:00",
        "settled_at": "2026-09-28T09:00:00+00:00",
        "settlement_date": "2026-09-28",
    }
    coerced_stl = _coerce(settlement_raw, settlement_schema)

    assert isinstance(coerced_stl["event_ts"], datetime)
    assert isinstance(coerced_stl["settled_at"], datetime)
    assert coerced_stl["settlement_date"] == datetime(2026, 9, 28).date()

    tx_schema = transaction_event_schema()
    tx_raw = {
        "event_id": "ev_001",
        "event_ts": "2026-09-27T10:15:30+00:00",
        "transaction_ref": "tx_12345",
        "created_at": "2026-09-27T10:15:30+00:00",
        "updated_at": "2026-09-27T10:16:00+00:00",
        "settlement_source_watermark_at": "2026-09-28T12:00:00+00:00",
    }
    coerced_tx = _coerce(tx_raw, tx_schema)

    assert isinstance(coerced_tx["created_at"], datetime)
    assert isinstance(coerced_tx["updated_at"], datetime)
    assert isinstance(coerced_tx["settlement_source_watermark_at"], datetime)
