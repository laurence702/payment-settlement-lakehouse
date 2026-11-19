"""Ledger of generator chunks whose Kafka transaction has committed.

Kafka transactions make each chunk all-or-nothing, but they do not tell a
retried task which chunks the failed attempt already committed. Without that, a
retry starts from chunk zero and every committed chunk reaches the topics twice.
The ledger is that record: a chunk is listed only after its transaction commits,
and a retry skips every listed chunk. See docs/adr/0009.

One gap remains by design. If the Kafka commit succeeds and the ledger write
then fails, the retry publishes that one chunk again. The duplicates carry the
same event_ids (chunks are seeded deterministically), and the Spark dedup on
(transaction_ref, event_id) removes them.
"""

from __future__ import annotations

import hashlib
from typing import Protocol

from settlement.config import Settings


def run_key(run_id: str, n_transactions: int, days: int, seed: int, chunk_size: int) -> str:
    """Identify one generate request.

    The parameters are part of the key so a run re-triggered with a different
    size, seed or chunking never skips chunks recorded under the old shape.
    """
    return f"{run_id}|n={n_transactions}|days={days}|seed={seed}|chunk={chunk_size}"


def transactional_id(key: str) -> str:
    """Kafka transactional ID for a run key.

    Stable across retries of the same run, so the broker fences the failed
    attempt's producer and aborts its open transaction when the retry starts.
    """
    return "settlement-generator-" + hashlib.sha256(key.encode()).hexdigest()[:24]


class ChunkLedger(Protocol):
    def committed(self, key: str) -> set[int]:
        """chunk_start of every chunk already committed under `key`."""
        ...

    def record(self, key: str, chunk_start: int, transactions: int, settlements: int) -> None:
        """Mark a chunk committed. Recording the same chunk twice is a no-op."""
        ...


class InMemoryChunkLedger:
    """Process-local ledger for tests and for CLI runs that never retry."""

    def __init__(self) -> None:
        self.rows: dict[tuple[str, int], tuple[int, int]] = {}

    def committed(self, key: str) -> set[int]:
        return {start for (k, start) in self.rows if k == key}

    def record(self, key: str, chunk_start: int, transactions: int, settlements: int) -> None:
        self.rows.setdefault((key, chunk_start), (transactions, settlements))


class PostgresChunkLedger:
    """Ledger in the operations database (pipeline.publish_ledger)."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def _connect(self):
        from settlement.ops.db import connect

        return connect(self._settings)

    def committed(self, key: str) -> set[int]:
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT chunk_start FROM pipeline.publish_ledger WHERE run_key = %s",
                    (key,),
                )
                return {row[0] for row in cur.fetchall()}
        finally:
            conn.close()

    def record(self, key: str, chunk_start: int, transactions: int, settlements: int) -> None:
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO pipeline.publish_ledger "
                    "(run_key, chunk_start, transactions, settlements) "
                    "VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                    (key, chunk_start, transactions, settlements),
                )
            conn.commit()
        finally:
            conn.close()
