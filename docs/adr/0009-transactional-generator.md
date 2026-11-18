# 0009: The generator publishes in Kafka transactions and resumes from a ledger

Status: accepted, 2026-11-18

## Context

`generate_events` published each chunk with an idempotent producer and was set
to `retries=0` (ADR 0008). Idempotence removes duplicates caused by the
producer's own resends within one producer session. A retried task starts a new
session with a new producer ID, so the broker cannot tell its messages from the
first attempt's. Two consequences followed:

* A task that failed after publishing part of a run could not be retried. A
  retry would publish every chunk again, so a transient broker fault failed the
  whole DAG run.
* A chunk interrupted mid-publish left part of its events in the topics, with
  no marker that they belonged to a failed attempt.

Spark's dedup on (`transaction_ref`, `event_id`) removed the duplicates
downstream, but the raw layer held them, and the dedup was the only defence.

## Decision

* **One transaction per chunk, across both topics.** `TransactionalPublisher`
  creates one producer per task with a `transactional.id` derived from the run
  key, calls `init_transactions` once, and wraps each chunk's transaction and
  settlement events in one transaction. A chunk is visible in full or not at
  all.
* **Consumers read committed data only.** `ingest.py` sets
  `isolation.level=read_committed` explicitly. It was already librdkafka's
  default; stating it means a config change cannot silently let aborted chunks
  into the raw layer.
* **A ledger records committed chunks.** After a chunk's transaction commits,
  the task writes (`run_key`, `chunk_start`) to `pipeline.publish_ledger` in the
  operations database. A retry reads the ledger and skips those chunks. The run
  key is the DAG run id plus count, days, seed and chunk size, so a run
  re-triggered with different parameters never skips chunks recorded for
  another shape.
* **Same run, same transactional ID.** On a retry, `init_transactions` with the
  same ID fences the failed attempt's producer and aborts any transaction it
  left open, so a zombie producer cannot commit late.
* **`generate_events` now inherits the default retries** (ADR 0008).

Chunk generation depends only on `seed + chunk_start`, so a skipped chunk is
never regenerated and a resumed chunk is byte-for-byte the one the failed
attempt tried to send.

## Known gap

The Kafka commit and the ledger write are two systems with no shared
transaction. If the commit succeeds and the ledger write fails, the retry sends
that one chunk again in a new committed transaction. Its events carry the same
`event_id`s, and the Spark dedup removes them. The gap is at most one chunk per
failure, and only in that window.

Closing it would need the ledger inside Kafka (for example a marker message in
the same transaction, read back on start-up), which adds a third topic and a
read path to the generator. The Spark dedup already exists and is tested, so
the simpler design was chosen.

## Costs

* `generate_events` now needs Postgres (the Airflow metadata server, which is
  always up when the scheduler is) and runs the ops migrations before
  publishing.
* While a crashed attempt's transaction is open, read_committed consumers stop
  at it until the retry fences it or `transaction.timeout.ms` (300 s) passes.
  `ingest_to_raw` runs only after `generate_events` succeeds, so in the DAG no
  transaction is open by then.
* Each chunk adds a commit round trip and a ledger insert. The overhead at
  1,000,000 transactions has not been measured yet (see Proof).

## Proof

Unit tests (`tests/test_publish_ledger.py`) use a fake producer with commit and
abort semantics. They show one producer and one transaction per chunk, that a
run whose third commit fails and is then retried leaves each `event_id` in the
committed topics exactly as often as a clean run, that the same rerun without a
ledger doubles the topics, and that a delivery failure aborts the chunk and
leaves the ledger empty. `make test-ops` runs the Postgres ledger test.

`make chaos-generate` triggers a run with a seed no other run uses, waits until
the ledger lists `--kill-after-chunks` chunks, then sends SIGKILL to the
`generate_events` task process: no abort and no cleanup. Airflow retries the
task. The check regenerates the expected event_ids from the seed and counts
them in both topics (read_committed and read_uncommitted) and in the raw
Parquet layer. Reports go to `chaos-report/`.

### Results (2026-11-18)

Both runs used the default profile's container limits (scheduler 1,800 MiB) in a
Docker VM with 8 CPUs and 7.8 GiB of memory, after `make verify` passed on the
same code.

| Run | Shape | Killed | Retry | Committed topics | Raw Parquet | Aborted events seen only by read_uncommitted |
|---|---|---|---|---|---|---|
| `chaos_generate__20261118T165605` | 200,000 transactions, 8 chunks of 25,000 | after 2 chunks, between chunks | try 2 published 6 chunks; run `success` | 406,026 transaction events and 170,665 settlement events, exactly as expected | same counts, no extras | 0 |
| `chaos_generate__20261118T170042` | 200,000 transactions, 4 chunks of 50,000 | 1.5 s after chunk 1, inside chunk 2's open transaction | try 2 published 3 chunks; run `success` | 406,115 and 171,012, exactly as expected | same counts, no extras | 9,448 transaction events |

The second run is the one that exercises the transaction: 9,448 events from the
killed attempt reached the broker inside a transaction that never committed.
The retry's `init_transactions` aborted it, the ingest consumer skipped it, and
none of those events reached the raw layer. The ledger listed every chunk once
in both runs.

Not yet measured: the overhead of per-chunk commits and ledger writes at
1,000,000 transactions. It will be added to `docs/benchmarks.md` with a run id.
