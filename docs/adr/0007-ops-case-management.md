# 0007. Exception cases and analyst state live in Postgres, fed by a sync, not a join

Status: accepted (2026-11-15)

## Context

The pipeline classifies every settlement-eligible transaction in
`mart_settlement_reconciliation`, but it stops at the mart. Nobody owns the
rows that need a human: a short-paid settlement, a missing settlement past its
SLA, a reversal after payout. Two constraints shape where that work can live.

* `serve.py` rebuilds every mart and swaps it into ClickHouse with
  `EXCHANGE TABLES`. Anything an analyst decides cannot be stored in the mart or
  in ClickHouse, because the next run replaces both. A re-run must never undo an
  analyst's decision.
* ClickHouse and DuckDB are analytical engines. A case has a mutable current
  state and an audit trail with idempotent writes. That is a transactional
  workload with uniqueness constraints.

## Decision

**What is a break.** A row is a break when `reconciliation_status` is one of
`short_paid`, `partial_settlement`, `overpaid`, `reversed_after_settlement`, or
when it is `missing_settlement` and `reconciliation_bucket` is `breached_sla`.
`matched`, `unverified_source` and every `pending_*` bucket are not breaks. A
stale settlement source is a pipeline condition, not a reconciliation exception,
and opening cases for it is how an operations queue turns into noise.

**Case identity is produced once, in dbt.** `case_id` is the SHA-256 of
`'case:v1:' || transaction_ref`, as lowercase hex. The mart grain is already one
row per `transaction_ref`, so no date is part of the key and the identity cannot
shift when a derived date does. The prefix versions the scheme. Python never
recomputes the hash; two implementations of one hash drift. dbt `unique` and
`not_null` tests guard it. The human-facing label is `CASE-` plus the first 16
hex characters, and it is a display value only. Twelve characters (48 bits)
would give roughly a 0.2% chance of a collision at one million cases, so the
full digest is the key.

**State lives in Postgres, schema `ops`.** The stack already runs Postgres, so
this adds no container. Two tables:

* `ops.cases`: current system columns (what the pipeline last saw) and current
  analyst columns (what a person decided).
* `ops.case_actions`: append-only audit trail with a unique idempotency key.

**Two-phase state.** The sync step writes only system columns. Analyst columns
are written only inside the transaction that records an action. The effective
status shown to an analyst is `COALESCE(analyst_status, system_status)`, so no
pipeline run can overwrite a decision.

**A sync, not a cross-engine join.** The console needs to filter, sort and page
"open cases". Doing that across ClickHouse and Postgres means merging in the
application. Instead an Airflow task reads the current breaks from ClickHouse
and upserts them into `ops.cases`. The console then reads one database. Breaks
number in the thousands, not millions, so the copy is cheap. A case that
is no longer a break in the latest run is marked `CLEARED`, not deleted, and
any analyst decision on it is kept.

**Closed does not mean frozen.** Each action stores a fingerprint of the facts
it was decided against (break type and outstanding kobo). If a later run
produces a different fingerprint for a closed case, the case is flagged
`drifted` and surfaced for review. It is neither silently reopened nor silently
trusted.

**Money is integer kobo** in every table, endpoint and template. Display
formatting uses integer division and modulo.

## Alternatives rejected

* SQLite or DuckDB for case state: single writer, and no way to enforce an
  idempotency key under concurrent requests.
* `COALESCE(ops_state, mart_status)` evaluated at query time across engines:
  correct but pushes filtering and paging into application code.
* Streamlit: reruns the script on each interaction, which is a poor fit for
  idempotent writes, and it costs more memory than a small ASGI app.

## Consequences

* The mart gains four additive columns: `case_id`, `is_break`, `break_type` and
  `exposure_kobo` (money at risk, integer kobo, used to sort the queue). No
  existing column changes.
* The console is an optional compose profile (`ops`). Its memory comes out of
  the existing budget, and the change is recorded in ADR 0003 and
  `.env.example` in the same commit.
* The console has no authentication. It binds to localhost and takes an analyst
  name per request. That is a documented limit, not a design goal.
