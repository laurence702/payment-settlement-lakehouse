# Payment Settlement Lakehouse Settlement Lakehouse — Project Explainer

Two perspectives on the same system: one for anyone curious, one for a
technical interview.

---

## Part 1 — Like you're 15

### The problem this solves

Imagine you pay ₦10,000 on Paystack to buy something online.
Three things happen almost instantly:

1. The app says "payment successful" ✅
2. Paystack notes that it owes the merchant ₦9,700 (they keep a small fee).
3. One or two days later, Paystack actually sends ₦9,700 to the merchant's
   bank account — this is called **settlement**.

Now imagine you run a company with 50,000 transactions a day.
How do you know every single one of them got settled correctly?
What if Paystack sent ₦9,600 instead of ₦9,700 for one of them?
What if they forgot one entirely?

That's the question this project answers automatically.

### What this project actually is

It is a **fake (synthetic) version** of the above problem,
built entirely with real engineering tools on a laptop.
It does not connect to real Paystack, real banks, or real money.
It generates its own pretend payment events — 50,000 at a time —
runs them through the same pipeline a real fintech would use,
and then checks whether every pretend payment got settled correctly.

Think of it as a flight simulator for a payment data platform.

### The journey of one payment event

```
Your fake "₦10,000 payment" gets created
      ↓
Sent into Kafka   (a fast message queue — like a conveyor belt)
      ↓
Saved as a raw file in SeaweedFS   (S3-compatible storage — like a hard drive in the cloud)
      ↓
PySpark cleans and deduplicates it   (Apache Spark — a data processing engine)
      ↓
dbt builds the analysis tables   (dbt — SQL templating tool)
      ↓
Results are loaded into ClickHouse   (a very fast analytics database)
      ↓
Airflow ran all of the above in order and checked the result   (Airflow — the job scheduler)
```

All of this runs inside Docker containers on a single laptop — 6 GB of RAM total.

### What the final answer looks like

For every payment, the system produces one of these labels:

| Label | Plain English |
|---|---|
| `matched` | Paid exactly the right amount ✅ |
| `missing_settlement` | Not paid yet — and it's overdue |
| `partial_settlement` | Less than 10% of what was owed |
| `short_paid` | A little short |
| `overpaid` | Paid too much |
| `reversed_after_settlement` | Payment was refunded but money had already gone out |
| `unverified_source` | Not paid yet, but the settlement data is too old to be sure |

---

## Part 2 — Like you're in a technical interview

### What I built and why

This is a **production-shaped settlement reconciliation lakehouse** that
answers the question:

> "For every successful payment charge, did a settlement arrive for the
> correct net amount within the merchant's contractual SLA — and if not,
> why not?"

It is a local, synthetic proof-of-concept. It is not connected to real
payment service providers, bank feeds, or customer data. All events are
generated deterministically from a seeded synthetic data model.

### Architecture

```
┌─────────────┐    produce     ┌────────────────┐
│  generate.py│ ─────────────► │  Kafka topics  │
│ (synthetic  │                │ transactions   │
│  PSP events)│                │ settlements    │
└─────────────┘                └───────┬────────┘
                                       │ consume (manual commit)
                               ┌───────▼────────┐
                               │  ingest.py     │  raw Parquet →
                               │  + quarantine  │  SeaweedFS (lakehouse-raw)
                               └───────┬────────┘
                                       │
                               ┌───────▼────────┐
                               │ transform_spark│  dedupe, latest-wins,
                               │    .py         │  kobo conversion →
                               └───────┬────────┘  SeaweedFS (lakehouse-staged)
                                       │
                               ┌───────▼────────┐
                               │  dbt build     │  stg → fct → mart →
                               │  (DuckDB)      │  SeaweedFS (lakehouse-marts)
                               └───────┬────────┘
                                       │
                               ┌───────▼────────┐
                               │  ClickHouse    │  atomic table swap,
                               │  serve.py      │  serving layer
                               └───────┬────────┘
                                       │
                               ┌───────▼────────┐
                               │  quality.py    │  post-load assertions
                               └────────────────┘
     All tasks orchestrated by Airflow 3.3.1 (settlement_pipeline DAG)
```

### Key engineering decisions and why I made them

**Integer kobo throughout**
> Money never touches a float. All arithmetic uses integer kobo (1 NGN = 100
> kobo). This eliminates rounding drift that accumulates silently in float
> pipelines. The conversion to naira happens only at the presentation edge.

**Manual Kafka commits**
> `ingest.py` calls `consumer.commit()` only after a batch is durably written
> to object storage. If the write fails, the offset is not advanced and the
> batch is re-consumed on the next run. This prevents silent data loss at the
> cost of possible duplicates — duplicates are removed downstream by Spark's
> deduplication step.

**Raw layer immutability**
> The raw Parquet files in `lakehouse-raw` are never modified after ingest.
> Spark reads from raw and writes deduplicated output to `lakehouse-staged`.
> This preserves a complete audit trail and allows re-derivation of any
> downstream layer from the source.

**dbt external materializations (DuckDB → Parquet)**
> dbt models are materialised as Parquet files on SeaweedFS via the
> `dbt-duckdb` adapter's external materialization. DuckDB reads staged Parquet
> directly via `read_parquet()` with `hive_partitioning = true`. No DuckDB
> file is ever persisted in the repo — it is a query engine, not a store.

**Atomic ClickHouse table swap (serve.py)**
> Each mart is loaded into a `_new` shadow table, then `RENAME TABLE` swaps it
> atomically. Reads against the serving layer see either the old or the new
> complete table — never a partial load.

**Merchant-specific SLA (v0.2)**
> The generator assigns each merchant a `settlement_lag_days` (T+1 or T+2).
> The staging layer propagates `expected_settlement_date` as
> `successful_at_date + settlement_lag_days`. The reconciliation mart classifies
> overdue using that date, not a global 3-day SLA. This is the same logic a
> real treasury ops team uses.

**Amount-aware reconciliation (v0.2)**
> A settlement record existing is not the same as the correct amount arriving.
> The mart computes `expected_net_kobo`, `settled_net_kobo`, and
> `settlement_variance_kobo`, then assigns an explicit `reconciliation_status`
> with seven distinct values. A short-paid row is never marked `matched`.

**Source freshness / unverified_source (v0.2)**
> The generator writes a `settlement_source_watermark_at` timestamp to each
> transaction event. If that watermark is before the `expected_settlement_date`,
> the mart labels the row `unverified_source` instead of `missing_settlement`,
> so a delayed provider feed is not counted as a breach.

**Payout batches (v0.2)**
> Settlement events now carry a `payout_id` (one-to-many: one payout can
> cover multiple transactions). Staging and facts surface this field.
> `fct_settlements` has grain `settlement_id`; `fct_transactions` retains
> grain `transaction_ref`. The reconciliation mart joins on `transaction_ref`.

**Quarantine for malformed messages (v0.2)**
> Previously, parsing failures incremented a counter and were dropped.
> Now `ingest.py` writes malformed payloads to an append-only quarantine
> dataset in object storage with `source_topic`, `ingestion_time`, and
> `raw_payload`, but never leaks secrets. A pure helper function makes this
> unit-testable without a live object store.

**6 GB memory envelope**
> The entire stack — Kafka, ZooKeeper, SeaweedFS, Spark (local mode inside
> Airflow), dbt-DuckDB, ClickHouse, Airflow scheduler + webserver — must
> fit within 6 GB. This is a hard design constraint documented in ADR-0003.
> Spark runs in local mode inside the Airflow scheduler container to avoid
> spinning up a separate JVM host.

### What it does NOT do (stated honestly)

- No real PSP or bank integration (Paystack/Flutterwave are label names only).
- No schema registry — schema is enforced via PyArrow at ingest.
- No business-day calendar, provider cutoff windows, or channel-specific rules.
- No signed provider webhooks or API ingestion.
- No bank-credit feed confirming money reached the merchant account.
- No production monitoring, alerting, or SLA dashboards.
- No replication or high-availability.

### Test strategy

| Layer | Tool | What is tested |
|---|---|---|
| Generator | pytest | Correct proportions, merchant SLA, payout batches |
| Ingest | pytest | Schema coercion, quarantine path for malformed messages |
| Spark | pytest + PySpark local | Deduplication, kobo arithmetic, latest-event wins |
| dbt | pytest + DuckDB | Model logic, reconciliation status values, grain uniqueness |
| Quality | pytest (mocked ClickHouse) | All 5 checks, grain definition, discrepancy soft-warnings |

`make lint && make test` — no Docker required.
`make verify` — full Docker stack end-to-end.

### Numbers from the latest pipeline run

| Table | Rows |
|---|---|
| `fct_transactions` | 100,000 |
| `fct_settlements` | 85,103 |
| `mart_settlement_reconciliation` | ~43,929 eligible + reversed rows |

All `serving_quality_checks` assertions passed.
