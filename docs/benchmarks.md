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

### Three repeats (8 GB profile, 1,000,000 events)

`make benchmark EVENTS=1000000 RUNS=3 PROFILE=medium`, each run from freshly
wiped volumes, 8 CPUs, 7.82 GiB VM. Records in `benchmark-runs/20261116T235346/`.
**3 of 3 succeeded**, every task on its first try, no OOM kill, no restart,
881,010 mart rows each time.

| Run | Airflow run time | Peak, all containers | Scheduler peak | ClickHouse peak |
|---|---|---|---|---|
| `bench_1000000_1_20261116T235346` | 353.5 s | 3,976 MiB | 2,322 MiB | 632 MiB |
| `bench_1000000_2_20261117T000019` | 344.9 s | 3,910 MiB | 2,291 MiB | 654 MiB |
| `bench_1000000_3_20261117T000650` | 357.1 s | 4,138 MiB | 2,512 MiB | 491 MiB |

Run time: min 345 s, median 354 s, max 357 s. Median task times: generate 170 s,
ingest 105 s, Spark 48 s, dbt 14 s, load 3.7 s, quality checks 0.9 s, ops sync
3.6 s. The hand-run above took 83 s in Spark against 48 s here; that run had the
Airflow UI and a browser open and I did not isolate the cause, so treat Spark
time as variable, not as a fixed figure.

Across all four 8 GB runs the scheduler peaked between 2,291 and 2,512 MiB
(75% of its 3,328 MiB ceiling at the worst), and every container stayed under
its ceiling.

### Three repeats, 1,000,000 events, 8 GB profile

`make benchmark EVENTS=1000000 RUNS=3 PROFILE=medium`, each run from wiped volumes
(`benchmark-runs/20261116T235346/`, 8 CPUs, 7.82 GiB VM). **3 of 3 succeeded**: no
failed task, no OOM kill, no restart, 881,010 mart rows every time. Note that this
runner timed from when it noticed the run and not from Airflow's own clock (the
version that read Airflow's start and end times was written after these ran), so
read these as about 5 seconds low compared with the 391 s UI run.

| Metric | Min | Median | Max |
|---|---|---|---|
| Wall time (s) | 344.9 | 353.5 | 357.1 |
| generate_events (s) | | 170.3 | 171.3 |
| ingest_to_raw (s) | | 105.1 | 106.4 |
| spark_raw_to_staged (s) | | 47.6 | 47.8 |
| dbt_build (s) | | 14.4 | 16.0 |
| load_clickhouse (s) | | 3.7 | 3.8 |

Peak MiB across the three runs: scheduler 2,512, kafka 666, clickhouse 654,
seaweedfs 596, api-server 290, dag-processor 191, postgres 86, console 55, redis 6.
All containers together peaked at 4,138 MiB. The spread between runs is under 4%
on wall time, so the 8 GB profile is stable at 1M on this machine. The single
UI run's slower Spark step (83 s against 47.6 s here) is unexplained.

### The `lean` profile at 1,000,000 events

`config/profiles/lean.env`: scheduler 2,816 MB, ClickHouse 1,536 MB, SeaweedFS
768 MB, Kafka 768 MB, the rest trimmed toward their measured peaks; ceilings sum
to about 6.7 GiB. Three runs from wiped volumes, same 7.82 GiB, 8 CPU VM
(records in `benchmark-runs/20261117T001636/`). **3 of 3 succeeded**, every task
on its first try, no OOM kill, no restart, 881,010 mart rows each.

| Run | Airflow run time | Peak, all containers | Scheduler | ClickHouse | Kafka |
|---|---|---|---|---|---|
| `bench_1000000_1_20261117T001636` | 361.2 s | 3,830 MiB | 2,358 | 815 | 507 |
| `bench_1000000_2_20261117T004015` | 351.5 s | 4,194 MiB | 2,473 | 1,243 | 745 |
| `bench_1000000_3_20261117T004653` | 360.3 s | 4,115 MiB | 2,455 | 688 | 683 |

Run time: min 352 s, median 360 s, max 361 s, about the same as `medium`
(345 to 357 s). The trimmed ceilings cost no speed.

What the peaks say about each ceiling (worst run against `lean`):

| Container | Peak MiB | Ceiling MiB | Used |
|---|---|---|---|
| kafka | 745 | 768 | **97%** |
| scheduler | 2,473 | 2,816 | 88% |
| clickhouse | 1,243 | 1,536 | 81% |
| seaweedfs | 531 | 768 | 69% |
| api-server | 296 | 384 | 77% |
| dag-processor | 217 | 256 | 85% |
| ops console | 59 | 96 | 61% |
| postgres | 99 | 192 | 52% |

* ClickHouse reached 1,243 MiB in one run, so the earlier note that 794 MiB was a
  floor was right. Its server limit is 90% of the ceiling, so it needs a ceiling
  of about 1.4 GB or more at this size. 1,280 MB is below that.
* Kafka at 97% is the weakest point. It passed three times, but different broker
  timing could push it over. Do not trim it; raising it to 896 MB is the likelier
  next change.
* Not shown by this evidence: that 1M fits a real 6 GB VM. The VM was 7.82 GiB,
  so the memory the host had beyond the container ceilings (page cache, the Docker
  daemon) was not constrained. The all-container peak of 4.2 GiB suggests it
  could. The test is to set the Docker VM to 6 GB and run
  `make profile PROFILE=lean && make benchmark EVENTS=1000000 RUNS=3 PROFILE=lean`.

The first `lean` run sat queued for 17 minutes because the reset left the DAG
paused. That wait is outside Airflow's run time, and the reset script now waits
for the DAG and unpauses it.

### Where the budget could be cut (not yet applied)

* The scheduler needed 2,430 MiB at 1M events, so the 6 GB profile's 1,800 MiB
  is below what 1M events require. The next profile to test is a 6 GB VM with
  the scheduler near 2.8 GB, funded from the headroom below.
* Headroom exists in postgres (33% used), ops console (42%), redis (17%),
  seaweedfs (55%) and dag-processor (56%).
* Do not trim ClickHouse on this evidence. `load_clickhouse` lasts about 3 s, so
  a 5 s sampler can miss its peak, and the same task was rejected at a 1,280 MB
  ceiling in an earlier run. 39% is a lower bound on the real peak.
* Three repeats are now recorded above and agree. A ceiling change still needs
  its own three-run benchmark, recorded in ADR 0003 and `.env.example` together.

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
* Repeats on a trimmed profile, which is the next experiment.
* Nothing else open: the retry test passed (ADR 0008).
