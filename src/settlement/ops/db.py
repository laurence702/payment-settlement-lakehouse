"""Connections to the operations database."""

from __future__ import annotations

from settlement.config import Settings


def connect(settings: Settings, dbname: str | None = None):
    """Open a connection. `dbname` overrides the configured ops database."""
    import psycopg2

    return psycopg2.connect(
        host=settings.ops_db_host,
        port=settings.ops_db_port,
        dbname=dbname or settings.ops_db_name,
        user=settings.ops_db_user,
        password=settings.ops_db_password,
        connect_timeout=10,
        application_name="settlement-ops",
    )
