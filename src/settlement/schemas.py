"""Explicit schemas for every layer.

Deliberately hand-written rather than inferred. Schema inference over JSON is
the reason so many "working" pipelines silently change a column's type the
first week a null shows up in a new position.
"""
from __future__ import annotations

from typing import Final

# --- Domain enumerations ----------------------------------------------------
# Plain string constants rather than enums. These values are serialized into
# JSON, Parquet, SQL and Kafka keys; a type that stringifies to "Channel.CARD"
# in one Python version and "card" in another is a bug waiting to happen in a
# codebase that spans an Airflow image, a Spark driver and a dbt macro.


class Channel:
    CARD: Final = "card"
    BANK_TRANSFER: Final = "bank_transfer"
    USSD: Final = "ussd"
    QR: Final = "qr"
    ALL: Final = ("card", "bank_transfer", "ussd", "qr")


class Status:
    PENDING: Final = "pending"
    SUCCESS: Final = "success"
    FAILED: Final = "failed"
    REVERSED: Final = "reversed"
    ALL: Final = ("pending", "success", "failed", "reversed")


class Gateway:
    PAYSTACK: Final = "paystack"
    FLUTTERWAVE: Final = "flutterwave"
    ALL: Final = ("paystack", "flutterwave")


# Terminal states. Once a transaction reaches one of these, a later PENDING
# event for the same reference is out-of-order noise and must not win.
TERMINAL_STATUSES: frozenset[str] = frozenset({Status.SUCCESS, Status.FAILED, Status.REVERSED})

# Rank used to resolve two events that share a reference AND a timestamp.
# Without a deterministic tiebreak, "latest wins" is not reproducible, and a
# pipeline that produces different marts on a rerun is not a pipeline.
STATUS_RANK: dict[str, int] = {
    Status.PENDING: 0,
    Status.FAILED: 1,
    Status.SUCCESS: 2,
    Status.REVERSED: 3,
}

FAILURE_REASONS: tuple[str, ...] = (
    "insufficient_funds",
    "do_not_honour",
    "invalid_pin",
    "timeout",
    "limit_exceeded",
    "suspected_fraud",
)

# Real CBN institution codes, so the data looks like something a Nigerian
# payments engineer would recognise rather than bank_a / bank_b.
BANKS: tuple[tuple[str, str], ...] = (
    ("044", "Access Bank"),
    ("058", "Guaranty Trust Bank"),
    ("057", "Zenith Bank"),
    ("011", "First Bank of Nigeria"),
    ("033", "United Bank for Africa"),
    ("070", "Fidelity Bank"),
    ("214", "First City Monument Bank"),
    ("032", "Union Bank"),
    ("035", "Wema Bank"),
    ("232", "Sterling Bank"),
    ("50211", "Kuda Microfinance Bank"),
    ("100004", "OPay"),
    ("100033", "PalmPay"),
    ("50515", "Moniepoint MFB"),
)

MERCHANT_CATEGORIES: tuple[str, ...] = (
    "ecommerce",
    "logistics",
    "education",
    "utilities",
    "travel",
    "food_delivery",
    "digital_services",
    "healthcare",
)


# --- Arrow schemas ----------------------------------------------------------
# Imported lazily so that pure-logic modules and their tests do not need
# pyarrow installed.

def transaction_event_schema():
    import pyarrow as pa

    return pa.schema(
        [
            pa.field("event_id", pa.string(), nullable=False),
            pa.field("event_ts", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("transaction_ref", pa.string(), nullable=False),
            pa.field("merchant_id", pa.string(), nullable=False),
            pa.field("customer_id", pa.string(), nullable=False),
            pa.field("gateway", pa.string(), nullable=False),
            pa.field("channel", pa.string(), nullable=False),
            pa.field("bank_code", pa.string(), nullable=True),
            pa.field("status", pa.string(), nullable=False),
            # Money is an integer number of kobo. Never a float. A float naira
            # column is how you end up owing someone 0.30000000000000004.
            pa.field("amount_kobo", pa.int64(), nullable=False),
            pa.field("fee_kobo", pa.int64(), nullable=False),
            pa.field("currency", pa.string(), nullable=False),
            pa.field("fx_rate_to_ngn", pa.float64(), nullable=True),
            pa.field("failure_reason", pa.string(), nullable=True),
            pa.field("created_at", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("updated_at", pa.timestamp("us", tz="UTC"), nullable=False),
            # Stamped by the ingester, not the producer. The gap between this
            # and event_ts is the late-arrival measurement.
            pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
        ]
    )


def settlement_event_schema():
    import pyarrow as pa

    return pa.schema(
        [
            pa.field("event_id", pa.string(), nullable=False),
            pa.field("event_ts", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("settlement_id", pa.string(), nullable=False),
            pa.field("merchant_id", pa.string(), nullable=False),
            pa.field("transaction_ref", pa.string(), nullable=False),
            pa.field("gross_kobo", pa.int64(), nullable=False),
            pa.field("fee_kobo", pa.int64(), nullable=False),
            pa.field("net_kobo", pa.int64(), nullable=False),
            pa.field("currency", pa.string(), nullable=False),
            pa.field("settlement_date", pa.date32(), nullable=False),
            pa.field("settled_at", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
        ]
    )
