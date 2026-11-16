"""Display formatting for integer kobo. Never goes through a float."""

from __future__ import annotations


def format_ngn(kobo: int) -> str:
    """Format integer kobo as naira, for example 123456789 -> '1,234,567.89'."""
    sign = "-" if kobo < 0 else ""
    naira, rem = divmod(abs(kobo), 100)
    return f"{sign}{naira:,}.{rem:02d}"
