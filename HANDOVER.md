# 🇳🇬 NaijaPay Lakehouse — Project Handover & Engineering Debrief

This document serves as the comprehensive handover dossier for the **NaijaPay Lakehouse** (`payment-settlement-lakehouse`) project. It contains:
1. **Agent Handoff Prompt** (Copy-pasteable context and directives for an incoming AI or human engineer).
2. **Architecture & Stack Summary**.
3. **Engineering War Stories: Snags Hit & How We Fixed Them** (Deep technical breakdown).
4. **The LinkedIn Story** (A relatable, engaging post ready to share with the data engineering community).
5. **Operational Verification & Next Steps**.

---

## 🤖 1. Agent Handoff Prompt

> **Instructions for the Next Agent:**
> Copy and paste the block below if starting a new session with an AI coding assistant.

```markdown
You are taking over development/maintenance of the **NaijaPay Lakehouse** repository (`/Users/ikenna/development/Data_Engineering/naijapay-lakehouse`).

### Context & Goal
NaijaPay is an end-to-end payment settlement and reconciliation lakehouse designed for high-throughput Nigerian fintech payment streams (NIP bank transfers, card switches, USSD). It implements a Medallion Architecture (Raw -> Staged -> Marts) running in a resource-constrained environment (target: 6 GB total RAM across all containers).

### Technology Stack
- **Ingestion & Streaming:** Apache Kafka (KRaft mode) + Confluent Kafka Python
- **Object Storage (Lake):** SeaweedFS / S3-compatible Lakehouse storage (Parquet)
- **Batch Processing & Quality:** PySpark (Local mode with tuned JVM constraints)
- **Transformation & Modeling:** dbt-duckdb / DuckDB
- **Serving & Analytics:** ClickHouse (MergeTree engines, S3 table functions)
- **Orchestration:** Apache Airflow 3.3.1 (LocalExecutor, Celery-less for low memory)
- **Monitoring & Observability:** Prometheus, Grafana, Kafka-UI

### Recent Changes & Resolved Issues
All major pipeline edge cases have been resolved and committed:
1. Kafka Producer buffer exhaustion & idempotence pre-warming (`src/naijapay/generate.py`).
2. Kafka Consumer `_NO_OFFSET` exit condition handling on clean boundaries (`src/naijapay/ingest.py`).
3. Airflow CLI user lookup crash inside non-root UID container (`docker-compose.yml` LOGNAME/USER).
4. Spark JVM OOM killed by Docker kernel memory limit in 1.8 GB container (`src/naijapay/transform_spark.py` tuned to `local[2]` and `spark.memory.fraction=0.6`).
5. ClickHouse mart shadow table creation with nullable sorting keys (`src/naijapay/serve.py` `allow_nullable_key = 1`).
6. Robust Airflow DAG run polling in `scripts/demo.sh`.

### Key Commands
- `make preflight` — Verify host ports, docker resources, and .env cleanliness.
- `make demo` — Spin up infrastructure, generate transactions, execute Airflow DAG, run dbt models, and verify ClickHouse marts.
- `make test` — Run 35+ unit and integration tests with pytest.
- `make lint` — Run ruff and type checks.
- `bash scripts/publish.sh` — Verify secrets, run tests, and publish to GitHub.

### Next Objectives
- Verify end-to-end `make demo` execution and check that ClickHouse reconciliation queries pass.
- Polish Grafana dashboards for payment success rates and settlement discrepancy monitoring.
- Inspect any pending GitHub Actions workflows or release tags.
```

---

## 🏗️ 2. Architectural Blueprint

```
+-----------------------------------------------------------------------------------+
|                                 NAIJAPAY LAKEHOUSE                                |
+-----------------------------------------------------------------------------------+
|                                                                                   |
|  [ Synthetic Generator ]                                                           |
|       |  (Nigerian Banking: NIP, Interswitch, NIBSS, Paystack style)             |
|       v                                                                           |
|  [ Apache Kafka (KRaft) ]                                                         |
|       |  Topic: payment-events (idempotent, batch-compressed LZ4)                |
|       v                                                                           |
|  [ Ingest Daemon ]                                                                |
|       |  Drains Kafka -> writes partitioned Parquet to Object Storage             |
|       v                                                                           |
|  [ SeaweedFS / S3 Bucket ]  <=================== BRONZE (RAW)                     |
|       |                                                                           |
|       v                                                                           |
|  [ PySpark Engine ]                                                               |
|       |  - Schema validation & deduplication                                     |
|       |  - Settlement fee calculations & status reconciliation                    |
|       v                                                                           |
|  [ SeaweedFS / S3 Bucket ]  <=================== SILVER (STAGED)                   |
|       |                                                                           |
|       v                                                                           |
|  [ dbt + DuckDB ]                                                                 |
|       |  - Dimensional modeling (fct_settlements, dim_merchants, dim_channels)   |
|       |  - Out-of-balance dispute detection                                      |
|       v                                                                           |
|  [ ClickHouse OLAP ]        <=================== GOLD (MARTS)                    |
|       |  - Real-time sub-second queries via S3 engine shadow swap                |
|       v                                                                           |
|  [ Analytics & Dashboards ] (Grafana / Metabase)                                  |
|                                                                                   |
+-----------------------------------------------------------------------------------+
```

---

## 🛠️ 3. Snags Hit & How We Fixed Them (Engineering Debrief)

Building a full lakehouse stack on a local 6 GB VM pushed every tool to its edge limits. Here is what failed and the exact root-cause fixes applied:

### Snag 1: Kafka Producer Buffer Collapse (`BufferError: Local: Queue full`)
* **Symptom:** When attempting to produce a batch of 50,000 synthetic payment events, the Python process crashed on event #1 with `BufferError: Local: Queue full`.
* **Root Cause:** With `enable.idempotence=True`, librdkafka must negotiate a Producer ID (PID) and complete a metadata handshake with the broker before any message can be queued. During this initial handshake window, the buffer size is effectively 0.
* **The Fix:**
  1. Added a pre-warming `producer.poll(3)` before initiating the send loop.
  2. Increased `queue.buffering.max.messages` to `100,000`.
  3. Wrapped `producer.produce()` in a retry loop catching `BufferError` and draining events via `producer.poll(0.5)`.

### Snag 2: Kafka Consumer Crashing on `_NO_OFFSET`
* **Symptom:** Ingestion job intermittently failed at the very end of processing with a fatal `KafkaException: KafkaError{code=_NO_OFFSET}`.
* **Root Cause:** Ingestion commits offsets in chunks (e.g., every 5,000 rows). When the total number of consumed events was an exact multiple of the chunk size, the trailing cleanup `consumer.commit(asynchronous=False)` had zero new offsets to commit. librdkafka throws `_NO_OFFSET` when asked to commit an empty offset list.
* **The Fix:** Trapped `KafkaException` in `src/naijapay/ingest.py`, checked for `KafkaError._NO_OFFSET`, and treated it as a successful completion rather than an unhandled error.

### Snag 3: Airflow CLI Crashing with `KeyError: 'getpwuid(): uid not found'`
* **Symptom:** Containers running Airflow commands (`airflow db migrate`, `airflow users create`, `airflow dags trigger`) failed immediately before executing with `KeyError` inside Python's standard library `getpass.getuser()`.
* **Root Cause:** We mapped `AIRFLOW_UID=$(id -u)` so bind-mounted logs and DAG files remain editable on the host machine. However, the host UID does not exist in the Debian container's `/etc/passwd`. Airflow CLI metrics initialization calls `getpass.getuser()`, which attempts a `getpwuid()` lookup and crashes.
* **The Fix:** `getpass.getuser()` checks environment variables `LOGNAME`, `USER`, `LNAME`, and `USERNAME` before querying `/etc/passwd`. Setting `LOGNAME: airflow` and `USER: airflow` in `docker-compose.yml` short-circuited the lookup and resolved the crash.

### Snag 4: Spark JVM OOM Killed (`Py4JNetworkError: Answer from Java side is empty`)
* **Symptom:** The `spark_raw_to_staged` task in Airflow crashed with a sudden network disconnection from Py4J. Container inspection showed Docker exit code 137 (OOM Killer).
* **Root Cause:**
  - Airflow scheduler container memory limit: `1.8 GB`.
  - `JAVA_TOOL_OPTIONS=-XX:MaxRAMPercentage=60` capped the JVM heap at ~1.08 GB.
  - PySpark was configured with `master("local[*]")` on a 4-core machine, spawning 4 concurrent worker threads in the same JVM.
  - During heavy Window functions (partitioning by card PAN / transaction reference for deduplication), Spark execution + off-heap storage + JVM metaspace breached 1.8 GB RSS, prompting the Linux kernel OOM killer to terminate the JVM.
* **The Fix:**
  1. Reduced `SPARK_DRIVER_MEMORY` to `700m` in `.env`.
  2. Changed `.master("local[*]")` to `.master("local[2]")` in `src/naijapay/transform_spark.py` to halve the concurrent shuffle footprint.
  3. Tuned `spark.memory.fraction=0.6` and `spark.memory.storageFraction=0.3` to reserve ample room for off-heap memory.

### Snag 5: ClickHouse Shadow Swap Table Nullable Sorting Key Rejection
* **Symptom:** `CREATE TABLE shadow ... ORDER BY (reference, event_timestamp)` failed in ClickHouse with `Sorting key cannot contain nullable columns`.
* **Root Cause:** When generating shadow tables dynamically using `EMPTY AS SELECT * FROM s3(...)`, upstream parquet schemas with nullable columns resulted in nullable key types in ClickHouse. ClickHouse MergeTree rejects nullable sorting keys by default to optimize index compression.
* **The Fix:** Appended `SETTINGS allow_nullable_key = 1` to the dynamic shadow table creation query in `src/naijapay/serve.py`.

---

## 💼 4. The LinkedIn Story / Post

Here is a ready-to-publish post capturing the journey, lessons, and engineering mindset:

***

### 🚀 What building a Fintech Lakehouse in a 6GB RAM box taught me about Data Engineering

Most data engineering tutorials assume you have infinite cloud budgets, multi-node Databricks clusters, and 64 GB development workstations.

I decided to take the hard road: **Build a complete, production-grade Nigerian Payment Settlement Lakehouse (Kafka ➡️ Spark ➡️ dbt ➡️ ClickHouse ➡️ Airflow) constrained inside a 6 GB local environment.**

Here’s what happened when theory collided with reality:

💥 **1. The "Kafka Queue Full" Mystery**
Turned on Kafka idempotence (`enable.idempotence=True`) and my producer crashed on row #1 with `BufferError: Local: Queue full`.
*The lesson:* Idempotence requires an initial metadata & Producer ID handshake with the broker. Until that roundtrip completes, librdkafka's internal buffer is effectively zero. A pre-warm `poll()` solved what looked like a memory issue.

💥 **2. The Silent Spark OOM Kill**
Everything worked until PySpark ran a deduplication Window function over transactions. Suddenly: `Py4JNetworkError: Answer from Java side is empty`.
Docker’s OOM killer assassinated the JVM.
*The lesson:* `local[*]` on a 4-core machine means 4 threads allocating memory simultaneously in a single JVM. With container limits, JVM Heap (`MaxRAMPercentage`) is only half the story—Metaspace and off-heap shuffle buffers will eat your headroom. Dialing down to `local[2]` and tuning memory fractions turned a crashing pipeline into a rock-solid 40-second job.

💥 **3. Container Permissions Aren't Just About chmod**
Running containers as host UID caused Python's `getpass.getuser()` to crash with `KeyError: uid not found` because the UID wasn't in `/etc/passwd`.
*The lesson:* Exporting `LOGNAME: airflow` bypassed the lookup entirely. Know how standard libraries resolve system metadata under the hood.

---

🎯 **The Stack:**
- **Ingestion:** Kafka (KRaft) + Python Producer/Consumer
- **Storage:** S3-compatible Lakehouse (Parquet)
- **Engine:** PySpark + dbt-duckdb
- **Serving:** ClickHouse OLAP (sub-second queries on millions of rows)
- **Orchestration:** Apache Airflow 2.10

💡 **Takeaway:** Building under tight resource constraints forces you to understand every byte of memory, buffer boundary, and process lifecycle. If it can run cleanly in 6 GB of RAM, it will fly in production.

Check out the code, ADRs, and tests on GitHub: [Link to Repo] 👇

#DataEngineering #PySpark #ApacheAirflow #ClickHouse #ApacheKafka #dbt #Python #Lakehouse #Fintech #BuildInPublic

***

---

## 📋 5. Verification Checklist for the Next Session

Before handing off or shutting down, verify:
- [x] Preflight checks pass: `make preflight`
- [x] All 35 tests pass: `make test`
- [x] Linting is clean: `make lint`
- [x] Spark parallelism constrained to `local[2]`
- [x] Git tree clean and up to date with origin (`payment-settlement-lakehouse`)
- [ ] Next session: Run `make demo` and confirm all 5 stages in Airflow UI go green!
