# 0008: Retries, backoff and what must never retry

Status: accepted, 2026-11-16

## Context

At 1,000,000 events the warehouse load failed twice with ClickHouse code 241
(server memory limit). The retry that followed two minutes later failed
identically, because the cause was a ceiling set too low, not a transient fault.
That run showed two things: a fixed single retry is too little for faults that
do clear (a background merge finishing, a container restarting), and a retry
cannot repair a deterministic shortfall.

## Decision

* Default for every task: `retries=3`, `retry_delay` 1 minute, exponential
  backoff, capped at 8 minutes. Delays run 1, 2 and 4 minutes, so a dependency
  that is down for a few minutes is waited out, and the per-task
  `execution_timeout` still bounds the total.
* `generate_events` is the exception: `retries=0`. It publishes the whole event
  set to Kafka, so a retry would publish it twice. A failed generation is fixed
  by rerunning the DAG run.
* Every other task is idempotent and safe to repeat. `load_clickhouse` drops and
  rebuilds its shadow table before the atomic swap; Spark and dbt overwrite their
  outputs; the ops sync is an upsert of system columns; `ingest_to_raw` resumes
  from committed consumer offsets.
* A failure that repeats four times is treated as real and left failed for a
  person to read. Retrying forever hides capacity problems.

## What was rejected

* Retrying inside the application code as well. Two layers multiply the delay
  and make the attempt count impossible to read in the Airflow UI.
* Auto-raising memory on a memory failure. That belongs in a profile chosen by
  a person against a budget (ADR 0003), not in the pipeline.

## Proof

`make chaos-retry` triggers a run, stops ClickHouse when `dbt_build` starts,
starts it again after `--outage-s` seconds, and passes only if the run succeeds
and `load_clickhouse` needed more than one try. The pipeline code is unchanged
for the test; the fault is external. Reports go to `chaos-report/`.
