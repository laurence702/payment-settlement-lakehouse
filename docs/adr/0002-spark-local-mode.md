# 0002. PySpark in local mode, and an honest note about why it is here

Status: accepted (2026-09-09)

## Context

This is a portfolio project targeting data engineering roles. Spark appears in
most job descriptions. It is also, at this data volume, unnecessary: 50,000
transactions is roughly 12 MB of Parquet, and DuckDB would finish the whole
transform before Spark's JVM finished starting.

Pretending otherwise is the failure mode. An experienced reviewer can tell the
difference between someone who used Spark and someone who used Spark to look
like they use Spark, and the second is worse than not using it at all.

## Decision

PySpark runs in `local[*]` mode as a library inside an Airflow task. There is no
Spark master, no worker, no cluster, and no Spark UI. The README and this ADR
say so plainly.

Spark is given a job that is genuinely Spark-shaped rather than a token one: a
global deduplication and a window function that resolves the latest status per
transaction across the entire dataset. That code is submitted unchanged to a
real cluster; only the master URL and the input path change.

## Alternatives considered

**Standalone master plus one worker, profile-gated.** Real `spark-submit`, a
real Spark UI to screenshot. Rejected on memory: the VM has 6 GB, and Airflow's
scheduler alone needs 1.8 GB because LocalExecutor forks tasks inside it. A
master and worker would have cost 2 to 3 GB that the budget does not have.

**No Spark at all.** Cleanest engineering. Rejected because it fails keyword
filters on job applications, and because the dedupe-and-window job is legitimate
Spark work even at small scale.

## Consequences

* The Spark driver JVM runs inside the Airflow scheduler container, so
  `MEM_AIRFLOW_SCHEDULER` is 1.8 GB. Below roughly 1.5 GB the task dies with
  exit code 137 and no useful error.
* `spark.sql.shuffle.partitions` is 8, not the default 200. On one machine, 200
  shuffle partitions produces 200 tiny files and spends more time on task
  scheduling than on work.
* I/O goes through pyarrow to MinIO, not `s3a://`. Wiring `hadoop-aws` into a
  local job means version-matching two jars against Spark's bundled Hadoop and
  carrying about 200 MB in the image, for zero change to the transform logic. On
  a real cluster you delete two pyarrow calls and pass an `s3a://` path.
* Spark tests are marked `slow` and excluded from the default `pytest` run,
  because a 35 second JVM start does not belong in a fast feedback loop.
