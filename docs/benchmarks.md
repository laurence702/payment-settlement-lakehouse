# Benchmarks

Every figure here was written by `scripts/benchmark.py` (`make benchmark`) and
comes from the run id beside it. Each run started from empty volumes. Nothing
here is extrapolated; a size that has not been run is listed as not run.

Environment: Docker VM with 7.8 GiB memory on an Apple M1 host (16 GB). The
20k to 1M rows marked 6 GB profile ran with 2 CPUs on the `small` profile
(`config/profiles/small.env`); the 8 GB profile runs had 8 CPUs on `medium`.
One run per size unless stated, 2026-11-16. A single run is a data point, not a
distribution, and times across the two CPU counts are not comparable.

| Events | Runs | Outcome | Wall time | Mart rows | Run id |
|---|---|---|---|---|---|
| 20,000 | 2 | 2 of 2 succeeded | 81 s, 81 s | 17,668 | `bench_20000_1_20261116T153548`, `bench_20000_2_20261116T153753` |
| 200,000 | 1 | succeeded | 146 s | 176,043 | `bench_200000_1_20261116T154023` |
| 500,000 | 1 | succeeded | 293 s | 440,363 | `bench_500000_1_20261116T154404` |
| 1,000,000 | 1 | **failed** in `spark_raw_to_staged` (6 GB profile) | 684 s | none | `bench_1000000_1_20261116T155012` |
| 999,947 | 1 | succeeded (8 GB profile) | 391 s | 881,010 | `manual__2026-11-16T23:37:31.968781+00:00` |

Run by hand from the Airflow UI on the `medium` profile (8 GB VM), not by the
runner, so memory was not recorded: 1,000,000 events, run
`manual__2026-11-16T22:43:32.962376+00:00`. `spark_raw_to_staged` and `dbt_build`
passed. `load_clickhouse` failed twice with ClickHouse code 241 (its server
limit, 1.13 GiB, was 90% of the 1,280 MB ceiling). After the ClickHouse ceiling
was raised to 2,048 MB the task was cleared, succeeded, and the run finished
`success`. Task times from the UI: generate 2:52, ingest 1:50, Spark 0:43,
dbt 0:14, load 0:05, quality checks 0:01, ops sync 0:03. A clean, measured
repeat is the next section.

Wall time is trigger to terminal state and excludes stack start-up. The 20k
wall times were sampled at a coarser poll interval than the later runs.

## 1,000,000 events on the 8 GB profile, measured

Run `manual__2026-11-16T23:37:31.968781+00:00`, triggered by hand in the Airflow
UI from freshly wiped volumes and measured by `scripts/benchmark.py --attach`
(record in `benchmark-runs/20261116T232526/`, gitignored). Profile `medium`.
Outcome: **success, every task on its first try, no OOM kill, no restart.**
Airflow's own run time: **391 s** (23:37:33 to 23:44:04 UTC).

| Task | Seconds |
|---|---|
| generate_events | 170 |
| ingest_to_raw | 108 |
| spark_raw_to_staged | 83 |
| dbt_build | 16 |
| load_clickhouse | 3.3 |
| serving_quality_checks | 0.8 |
| sync_ops_cases | 3.3 |

What it produced (from ClickHouse and the console after the run):

| Output | Value |
|---|---|
| `fct_transactions` | 1,000,000 rows, 79.7 MiB |
| `fct_settlements` | 853,846 rows, 54.4 MiB |
| `mart_settlement_reconciliation` | 881,010 rows, 118.2 MiB |
| Matched | 813,287 |
| Breaks (`is_break`) | 63,847, total exposure NGN 753,076,872.38 |
| Cases in the analyst console | 63,847 (equals the break count) |
| Break types | missing_settlement 27,164; short_paid 14,307; overpaid 10,969; reversed_after_settlement 10,410; partial_settlement 4,873 |

Peak memory against the `medium` ceilings (sampled every 5 s):

| Container | Peak MiB | Ceiling MiB | Used |
|---|---|---|---|
| airflow scheduler (Spark driver) | 2,430 | 3,328 | 73% |
| clickhouse | 794 | 2,048 | 39% |
| kafka | 562 | 768 | 73% |
| seaweedfs | 561 | 1,024 | 55% |
| airflow api-server | 302 | 448 | 67% |
| airflow dag-processor | 216 | 384 | 56% |
| postgres | 107 | 320 | 33% |
| ops console | 54 | 128 | 42% |
| redis | 8 | 48 | 17% |

All containers together peaked at **4,094 MiB** on a 7.82 GiB VM (8 CPUs for
this run). The summed ceilings (8.3 GiB) were never approached because the
peaks do not coincide: Spark's peak is during `spark_raw_to_staged`, the others
are lower while it runs.

### Where the budget could be cut (not yet applied)

* The scheduler needed 2,430 MiB at 1M events, so the 6 GB profile's 1,800 MiB
  is below what 1M events require. The next profile to test is a 6 GB VM with
  the scheduler near 2.8 GB, funded from the headroom below.
* Headroom exists in postgres (33% used), ops console (42%), redis (17%),
  seaweedfs (55%) and dag-processor (56%).
* Do not trim ClickHouse on this evidence. `load_clickhouse` lasts about 3 s, so
  a 5 s sampler can miss its peak, and the same task was rejected at a 1,280 MB
  ceiling in an earlier run. 39% is a lower bound on the real peak.
* One run is a data point. Repeat with `make benchmark RUNS=3` before changing a
  ceiling, and record the change in ADR 0003 and `.env.example` together.

Caveat on comparisons: the 20k to 1M rows above the `medium` section were run on
a VM with 2 CPUs; this one had 8. Do not read the 500k and 1M times as a
scaling curve.

## Where the time goes (500,000 events, 6 GB profile, 2 CPUs)

| Task | Seconds |
|---|---|
| generate_events | 103 |
| ingest_to_raw | 89 |
| spark_raw_to_staged | 66 |
| dbt_build | 14 |
| load_clickhouse | 3.6 |
| serving_quality_checks, sync_ops_cases | under 3 |

## Peak container memory against its ceiling

Peak MiB sampled every five seconds by `docker stats`; the ceiling is the
`small` profile value.

| Container | 200k | 500k | 1M (failed run) | Ceiling |
|---|---|---|---|---|
| airflow scheduler (runs Spark) | 1,440 | **1,790** | **1,798** | 1,800 |
| seaweedfs | 598 | 720 | 523 | 1,024 |
| kafka | 530 | 645 | 640 | 768 |
| clickhouse | 539 | 540 | 539 | 1,280 |

## What this says

* The 6 GB profile finishes 500,000 events, but with almost no margin: the
  scheduler container, which hosts the Spark driver in local mode, peaked at
  1,790 MiB of 1,800.
* At 1,000,000 events the scheduler peak reached its ceiling (1,798 of 1,800
  MiB) and `spark_raw_to_staged` failed on both attempts. Docker did not report
  an OOM kill on a container, because the task process is what is killed, not
  the container. That the memory ceiling caused the failure is inferred from
  the peak equalling the limit; it was not confirmed from the kernel log.
* ClickHouse, Postgres and the console stay flat. The pressure is the Spark
  driver and the generator, not the warehouse.

## Not yet run

* The `large` (10 GB) profile: not needed, since `medium` completed 1M.
* Repeats: every figure above is one run. For a distribution:
  `make profile PROFILE=medium && make benchmark EVENTS=1000000 RUNS=3 PROFILE=medium`.
* Nothing else open: the retry test passed (ADR 0008).
