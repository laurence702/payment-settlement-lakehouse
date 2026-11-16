# Benchmarks

Every figure here was written by `scripts/benchmark.py` (`make benchmark`) and
comes from the run id beside it. Each run started from empty volumes. Nothing
here is extrapolated; a size that has not been run is listed as not run.

Environment for all runs below: Docker VM with 7.8 GiB memory and 2 CPUs, the
`small` profile (ceilings in `config/profiles/small.env`), one run per size
unless stated, 2026-11-16. A single run is a data point, not a distribution.

| Events | Runs | Outcome | Wall time | Mart rows | Run id |
|---|---|---|---|---|---|
| 20,000 | 2 | 2 of 2 succeeded | 81 s, 81 s | 17,668 | `bench_20000_1_20261116T153548`, `bench_20000_2_20261116T153753` |
| 200,000 | 1 | succeeded | 146 s | 176,043 | `bench_200000_1_20261116T154023` |
| 500,000 | 1 | succeeded | 293 s | 440,363 | `bench_500000_1_20261116T154404` |
| 1,000,000 | 1 | **failed** in `spark_raw_to_staged` | 684 s | none | `bench_1000000_1_20261116T155012` |

Run by hand from the Airflow UI on the `medium` profile (8 GB VM), not by the
runner, so memory was not recorded: 1,000,000 events, run
`manual__2026-11-16T22:43:32.962376+00:00`. `spark_raw_to_staged` and `dbt_build`
passed. `load_clickhouse` failed twice with ClickHouse code 241 (its server
limit, 1.13 GiB, was 90% of the 1,280 MB ceiling). After the ClickHouse ceiling
was raised to 2,048 MB the task was cleared, succeeded, and the run finished
`success`. Task times from the UI: generate 2:52, ingest 1:50, Spark 0:43,
dbt 0:14, load 0:05, quality checks 0:01, ops sync 0:03. A clean, measured
repeat is recorded below once run.

Wall time is trigger to terminal state and excludes stack start-up. The 20k
wall times were sampled at a coarser poll interval than the later runs.

## Where the time goes (500,000 events)

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

500,000 and 1,000,000 events with the `large` profile, which needs a Docker VM
of 16 GB or more (this machine's VM is 7.8 GiB). To produce them:

```bash
make profile PROFILE=large
make benchmark EVENTS="500000 1000000" RUNS=3 PROFILE=large
```
