"""Versioned SQL migrations for the operations database.

Migrations are plain .sql files applied in filename order, each in its own
transaction, and recorded with a checksum. An applied migration that has since
been edited is an error: history is immutable, so changes go in a new file.

A Postgres advisory lock serialises concurrent runners, so two containers
starting together cannot apply the same migration twice.
"""

from __future__ import annotations

import argparse
import hashlib
from pathlib import Path

from settlement.config import Settings, get_settings
from settlement.ops.db import connect

MIGRATIONS_DIR = Path(__file__).parent / "migrations"

# Arbitrary, but fixed and distinct from any other advisory lock in this stack.
_LOCK_KEY = 7_040_007


class MigrationError(RuntimeError):
    pass


def discover(directory: Path = MIGRATIONS_DIR) -> list[tuple[str, str, str]]:
    """Return (version, checksum, sql) for every migration, in order."""
    found = []
    for path in sorted(directory.glob("*.sql")):
        sql = path.read_text(encoding="utf-8")
        found.append((path.stem, hashlib.sha256(sql.encode("utf-8")).hexdigest(), sql))
    return found


def ensure_database(settings: Settings) -> None:
    """Create the ops database when it does not exist yet.

    init-scripts only run on a fresh Postgres volume, so a stack that already
    has data needs this to pick the database up without a volume wipe.
    """
    conn = connect(settings, dbname="postgres")
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (settings.ops_db_name,))
            if cur.fetchone() is None:
                # Identifiers cannot be bound as parameters; quote it instead.
                from psycopg2 import sql

                cur.execute(
                    sql.SQL("CREATE DATABASE {}").format(sql.Identifier(settings.ops_db_name))
                )
    finally:
        conn.close()


def migrate(settings: Settings, directory: Path = MIGRATIONS_DIR) -> list[str]:
    """Apply pending migrations. Returns the versions applied by this call."""
    ensure_database(settings)
    migrations = discover(directory)
    applied_now: list[str] = []

    conn = connect(settings)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(%s)", (_LOCK_KEY,))
            cur.execute("CREATE SCHEMA IF NOT EXISTS ops")
            cur.execute(
                "CREATE TABLE IF NOT EXISTS ops.schema_migrations ("
                "version text PRIMARY KEY, checksum text NOT NULL, "
                "applied_at timestamptz NOT NULL DEFAULT now())"
            )
        conn.commit()

        try:
            with conn.cursor() as cur:
                cur.execute("SELECT version, checksum FROM ops.schema_migrations")
                recorded = dict(cur.fetchall())
            conn.commit()

            for version, checksum, sql in migrations:
                if version in recorded:
                    if recorded[version] != checksum:
                        raise MigrationError(
                            f"{version} was edited after it was applied. "
                            "Add a new migration instead of changing an old one."
                        )
                    continue
                with conn.cursor() as cur:
                    cur.execute(sql)
                    cur.execute(
                        "INSERT INTO ops.schema_migrations (version, checksum) VALUES (%s, %s)",
                        (version, checksum),
                    )
                conn.commit()
                applied_now.append(version)
        finally:
            conn.rollback()
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock(%s)", (_LOCK_KEY,))
            conn.commit()
    finally:
        conn.close()
    return applied_now


def main() -> None:
    argparse.ArgumentParser(description=__doc__.splitlines()[0]).parse_args()
    applied = migrate(get_settings())
    print(f"applied: {', '.join(applied) if applied else 'nothing to do'}")


if __name__ == "__main__":
    main()
