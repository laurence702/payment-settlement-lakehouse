# Benchmarks

Every figure here was written by `scripts/benchmark.py` (`make benchmark`) and
comes from the run id beside it. Each run started from empty volumes. Nothing
here is extrapolated; a size that has not been run is listed as not run.

Environment: Docker VM with 7.82 GiB memory on an Apple M1 host (16 GB). Rows
marked 6 GB use the `small` profile (`config/profiles/small.env`), rows marked
8 GB use `medium`. Every run had 8 CPUs except one 1,000,000-transaction run on the
6 GB profile, which had 2 and is marked.

Sizes are transactions, not Kafka messages. Each charge emits at least two
transaction events (pending, then a terminal status, sometimes duplicated or a
reversal), and settled charges add a settlement event. Counted with the DAG's
defaults (seed 20260909, 14 days, 50,000-transaction chunks); the 853,846
settlement events at 1,000,000 equal the `fct_settlements` row count of every
1,000,000-transaction run below.

| Transactions | Transaction events | Settlement events | Kafka events in total |
|---|---|---|---|
| 20,000 | 40,629 | 17,117 | 57,746 |
| 200,000 | 406,291 | 170,574 | 576,865 |
| 500,000 | 1,015,485 | 426,771 | 1,442,256 |
| 1,000,000 | 2,030,752 | 853,846 | 2,884,598 |

`make benchmark EVENTS=...` and the DAG parameter `event_count` keep their names;
both take a transaction count.

## 6 GB profile, five runs per size (2026-11-17)

`make benchmark EVENTS="20000 200000 500000" RUNS=5 PROFILE=small`, records in
`benchmark-runs/20261117T082942/`. **15 of 15 succeeded**: no failed task, no
OOM kill, no restart, and the same mart row count on every run of a size.

| Transactions | Runs | Outcome | Airflow run time (min / median / max) | Peak, all containers | Scheduler peak (ceiling 1,800 MiB) | Mart rows | Run ids |
|---|---|---|---|---|---|---|---|
| 20,000 | 5 | 5 of 5 succeeded | 63.6 / 66.0 / 67.3 s | 2,435 MiB | 990 to 1,214 MiB | 17,668 | `bench_20000_1_20261117T082942`, `bench_20000_2_20261117T083139`, `bench_20000_3_20261117T083336`, `bench_20000_4_20261117T083533`, `bench_20000_5_20261117T083726` |
| 200,000 | 5 | 5 of 5 succeeded | 113.8 / 117.3 / 119.5 s | 3,120 MiB | 1,328 to 1,585 MiB | 176,043 | `bench_200000_1_20261117T083918`, `bench_200000_2_20261117T084205`, `bench_200000_3_20261117T084453`, `bench_200000_4_20261117T084735`, `bench_200000_5_20261117T085017` |
| 500,000 | 5 | 5 of 5 succeeded | 206.7 / 207.8 / 213.1 s | 3,435 MiB | 1,462 to 1,788 MiB | 440,363 | `bench_500000_1_20261117T085303`, `bench_500000_2_20261117T085721`, `bench_500000_3_20261117T090139`, `bench_500000_4_20261117T090557`, `bench_500000_5_20261117T091016` |

Median task times at 500,000 transactions: generate 85.1 s, ingest 67.4 s, Spark 33.4 s,
dbt 10.1 s, load 1.8 s, quality checks 0.4 s, ops sync 1.5 s.

The scheduler container, which hosts the Spark driver in local mode, came within
12 MiB of its 1,800 MiB ceiling at 500,000 transactions, and its peak varied by 326 MiB
across five identical runs. 500,000 passes every time on this profile, with
almost no memory to spare.

## 1,000,000 transactions on the 6 GB profile, and the first 8 GB run

| Transactions | Runs | Outcome | Wall time | Mart rows | Run id |
|---|---|---|---|---|---|
| 1,000,000 | 1 | **failed** in `spark_raw_to_staged` (6 GB profile, **2 CPUs**, 2026-11-16) | 684 s | none | `bench_1000000_1_20261116T155012` |
| 1,000,000 | 1 | **failed** in `load_clickhouse`, all 4 attempts (6 GB profile, 8 CPUs, 2026-11-17) | 965 s | none | `bench_1000000_1_20261117T101331` |
| 1,000,000 | 1 | succeeded (8 GB profile) | 391 s | 881,010 | `manual__2026-11-16T23:37:31.968781+00:00` |

Run by hand from the Airflow UI on the `medium` profile (8 GB VM), not by the
runner, so memory was not recorded: 1,000,000 transactions, run
`manual__2026-11-16T22:43:32.962376+00:00`. `spark_raw_to_staged` and `dbt_build`
passed. `load_clickhouse` failed twice with ClickHouse code 241 (its server
limit, 1.13 GiB, was 90% of the 1,280 MB ceiling). After the ClickHouse ceiling
was raised to 2,048 MB the task was cleared, succeeded, and the run finished
`success`. Task times from the UI: generate 2:52, ingest 1:50, Spark 0:43,
dbt 0:14, load 0:05, quality checks 0:01, ops sync 0:03. A clean, measured
repeat is the next section.

The 8 CPU run on the 6 GB profile (records in `benchmark-runs/20261117T101331/`)
got further than the 2 CPU one: `spark_raw_to_staged` passed in 59 s with the
scheduler container at 1,737 of 1,800 MiB. The run then failed in
`load_clickhouse`. All four attempts were stopped by ClickHouse's own server
memory limit, 1.13 GiB (90% of the 1,280 MB ceiling), taken from the task logs:
code 241 `MEMORY_LIMIT_EXCEEDED` on attempts 1, 3 and 4, and code 33 (out of
memory reading the mart Parquet) on attempt 2. ClickHouse peaked at 1,204 MiB.
The retries ran as designed and could not help, because the limit did not
change between attempts. So the CPU count changes which step fails first at
1,000,000 transactions on this profile, not whether the run fits.

Conclusion for sizing: 1,000,000 transactions do not run on the 6 GB profile, and this
project does not claim they do. 500,000 is the ceiling there: it passed 5 of 5,
but the scheduler container (which hosts the Spark driver) peaked at 1,788 of
1,800 MiB, so it has almost no headroom. 1,000,000 needs the 8 GB profile.

Wall time is trigger to terminal state and excludes stack start-up.

## 1,000,000 transactions on the 8 GB profile, measured

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

Input versus output: the 1,000,000 generated events produce 1,000,000 rows in
`fct_transactions`, but the reconciliation mart has one row per successful charge,
plus reversed charges that had a prior settlement. Failed charges and
anything not settlement-eligible are not reconciled, which is why the mart (881,010 rows) is smaller than the
input. Breaks are the subset of mart rows that need an analyst.

The JSON record of this run gives its size as 999,947. That figure is wrong: the
observer read `event_count` before `preset`, the reverse of the DAG, so a run
triggered with preset `1M` and a leftover `event_count` was logged with the
leftover. The run generated 1,000,000 transactions (`fct_transactions` above), and its
881,010 mart rows match every seeded 1,000,000-transaction run. `scripts/benchmark.py`
now uses the DAG's precedence.

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

### Three repeats (8 GB profile, 1,000,000 transactions)

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

Across all six 8 GB runs (this section, the hand-run above and the two
repeats below) the scheduler peaked between 2,262 and 2,737 MiB (82% of its
3,328 MiB ceiling at the worst), and every container stayed under its ceiling.

### Three repeats, 1,000,000 transactions, 8 GB profile

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

### Two more repeats (8 GB profile, 1,000,000 transactions, 2026-11-17)

`make benchmark EVENTS=1000000 RUNS=2 PROFILE=medium`, each run from freshly
wiped volumes, 8 CPUs, 7.82 GiB VM. Records in `benchmark-runs/20261117T050724/`.
**2 of 2 succeeded**, no failed task, no OOM kill, no restart, 881,010 mart rows
each time. Wall time here is Airflow's own run time.

| Run | Airflow run time | Peak, all containers | Scheduler peak | ClickHouse peak | Kafka peak |
|---|---|---|---|---|---|
| `bench_1000000_1_20261117T050724` | 348.9 s | 3,961 MiB | 2,262 MiB | 629 MiB | 650 MiB |
| `bench_1000000_2_20261117T051353` | 347.0 s | 4,448 MiB | 2,737 MiB | 634 MiB | 629 MiB |

Median task times: generate 169.8 s, ingest 105.1 s, Spark 42.8 s, dbt 14.7 s,
load 3.2 s, quality checks 0.9 s, ops sync 3.6 s.

With these, the runner has recorded **5 of 5** successful 1,000,000-transaction runs on
the 8 GB profile (345 to 357 s), plus the measured hand-run (391 s). The second
run here set new highs: 4,448 MiB (4.34 GiB) for all containers together and
2,737 MiB for the scheduler, so the earlier 4,138 MiB and 2,512 MiB were not the
ceiling of the distribution. Run-to-run spread in the scheduler peak is about
475 MiB, which is why a single run is not enough to size a ceiling.

### The `lean` profile at 1,000,000 transactions

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

* On the 8 GB profile (2 GB Spark driver) the scheduler used up to 2,737 MiB at
  1M transactions, but on the 6 GB profile (1 GB driver, 8 CPUs) Spark finished
  1M at 1,737 MiB. Scheduler use grows into the room it is given, so the 8 GB
  peak is not a minimum. At 1M on 6 GB the binding limit is ClickHouse's
  1,280 MB ceiling, which needs about 1.4 GB or more at this size (see the
  `lean` section). The next profile to test is a 6 GB VM with ClickHouse raised
  and the difference funded from the headroom below.
* Headroom exists in postgres (33% used), ops console (42%), redis (17%),
  seaweedfs (55%) and dag-processor (56%).
* Do not trim ClickHouse on this evidence. `load_clickhouse` lasts about 3 s, so
  a 5 s sampler can miss its peak, and the same task was rejected at a 1,280 MB
  ceiling in an earlier run. 39% is a lower bound on the real peak.
* Five repeats are now recorded above and agree. A ceiling change still needs
  its own three-run benchmark, recorded in ADR 0003 and `.env.example` together.

## Peak container memory against its ceiling

Peak MiB sampled every five seconds by `docker stats` on the 6 GB profile; the
ceiling is the `small` profile value. The 500k column is the highest peak across
its five runs.

| Container | 500k (max of 5 runs) | 1M, 2 CPUs (failed in Spark) | 1M, 8 CPUs (failed in load) | Ceiling |
|---|---|---|---|---|
| airflow scheduler (runs Spark) | **1,788** | **1,798** | 1,737 | 1,800 |
| seaweedfs | 588 | 523 | 601 | 1,024 |
| kafka | 487 | 640 | 663 | 768 |
| clickhouse | 479 | 539 | **1,204** | 1,280 |

## After the transactional generator (2026-11-18)

`generate_events` now publishes one Kafka transaction per chunk and records each
chunk in a Postgres ledger (ADR 0009). Three runs per size from wiped volumes,
same 7.82 GiB, 8 CPU VM, DAG defaults. Records in
`benchmark-runs/20261118T172317/` (1M, `medium`) and
`benchmark-runs/20261118T173745/` (500k, `small`). All 6 of 6 succeeded with every
task on its first try, no OOM kill, no restart, and the same mart row counts as
before (881,010 and 440,363).

| Transactions | Profile | `generate_events` before (5 runs) | `generate_events` now (3 runs) | Airflow run time now | Peak, all containers | Scheduler peak | Run ids |
|---|---|---|---|---|---|---|---|
| 500,000 | 6 GB (`small`) | 84.4 to 85.7 s | 24.8 / 25.3 / 25.4 s | 146.0 / 148.4 / 151.7 s | 3,053 to 3,230 MiB | 1,503 to 1,625 of 1,800 MiB | `bench_500000_1_20261118T173745`, `bench_500000_2_20261118T174103`, `bench_500000_3_20261118T174422` |
| 1,000,000 | 8 GB (`medium`) | 168.3 to 171.3 s | 48.2 / 50.7 / 52.4 s | 225.5 / 238.7 / 252.5 s | 3,857 to 4,103 MiB | 2,226 to 2,331 of 3,328 MiB | `bench_1000000_1_20261118T172317`, `bench_1000000_2_20261118T172821`, `bench_1000000_3_20261118T173310` |

The "before" column is the five runs per size recorded above: at 500,000,
`bench_500000_1_20261117T085303` to `bench_500000_5_20261117T091016`; at
1,000,000, `bench_1000000_1_20261116T235346`, `bench_1000000_2_20261117T000019`,
`bench_1000000_3_20261117T000650`, `bench_1000000_1_20261117T050724` and
`bench_1000000_2_20261117T051353`.

Transactions did not slow the generator. It got faster, by about 120 s at 1M and
60 s at 500k, and the cause is not the transactions. The old `publish()` opened a
new producer for each topic in each chunk and called `poll(3)` on it to wait for
the broker handshake. With nothing to deliver yet, that call waits the full 3
seconds: timed in the scheduler container on 2026-11-18, three fresh idempotent
producers each returned from `poll(3)` after 3.02 to 3.03 s with zero events
served. 20 chunks x 2 topics x 3 s = 120 s at 1M, and 10 x 2 x 3 = 60 s at 500k,
which matches the measured drops. The new publisher opens one producer per task
and `init_transactions` completes the handshake without a fixed wait. The cost
of the per-chunk commits and ledger inserts is therefore inside the remaining
~50 s at 1M; it has not been measured separately.

## What this says

* The 6 GB profile finishes 500,000 transactions every time (5 of 5), but with almost
  no margin: the scheduler container, which hosts the Spark driver in local
  mode, peaked at 1,788 MiB of 1,800.
* At 1,000,000 transactions with 2 CPUs the scheduler peak reached its ceiling
  (1,798 of 1,800 MiB) and `spark_raw_to_staged` failed on both attempts. Docker
  did not report an OOM kill on a container, because the task process is what is
  killed, not the container. That the memory ceiling caused this failure is
  inferred from the peak equalling the limit; it was not confirmed from a log.
* At 1,000,000 transactions with 8 CPUs Spark passed and ClickHouse failed on
  its own logged 1.13 GiB server limit, four attempts out of four. That failure
  is confirmed by the error codes, not inferred.
* Postgres and the console stay flat at every size. The pressure is the
  scheduler (Spark driver and generator) and, at 1,000,000, the ClickHouse load.

## Not yet run

* The `large` (10 GB) profile: not needed, since `medium` completed 1M.
* Repeats on a trimmed profile, which is the next experiment.
* Nothing else open: the retry test passed (ADR 0008).
