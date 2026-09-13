# 0001. DuckDB transforms, ClickHouse serves

Status: accepted (2026-09-09)

## Context

The pipeline needs to transform Parquet in object storage and then serve the
results to a dashboard. Those are different jobs with different requirements,
and the temptation is to pick one engine for both.

## Decision

dbt-duckdb does the modelling. ClickHouse serves the marts.

DuckDB reads the staged Parquet from MinIO over httpfs, models it, and writes
the marts back to MinIO as Parquet. ClickHouse then ingests those same Parquet
files with the `s3()` table function into MergeTree tables that Grafana queries.

## Why not DuckDB alone

DuckDB is single-writer and embedded. Grafana pointed at a `.duckdb` file is a
demo, not a serving tier: concurrent dashboard refreshes contend on one file
lock, and there is no way to reload data without taking queries offline. It also
gives an interviewer an easy and fair question that has no good answer.

## Why not ClickHouse alone

Ingesting straight into ClickHouse and modelling with dbt-clickhouse works, and
for a pure streaming-analytics role it may even be the better story. It was
rejected because it deletes the object-store layer, and the lakehouse pattern
(cheap immutable files as the source of truth, engines as interchangeable
consumers) is the more transferable thing to be able to talk about.

## Consequences

* The marts exist twice: as Parquet in MinIO and as MergeTree tables. The
  Parquet is authoritative; ClickHouse is a projection that can be rebuilt from
  it at any time by re-running the load task.
* One adapter, not two. `dbt-clickhouse` is not a dependency, because ClickHouse
  reads the marts directly rather than re-modelling them.
* ClickHouse table schemas are inferred from the Parquet footer rather than
  hand-written DDL, so a dbt model change does not require a matching DDL edit
  that someone will forget. `ORDER BY` and `PARTITION BY` are still explicit,
  since those are performance decisions inference cannot make.
* Loads swap atomically (`EXCHANGE TABLES`), so a reload never shows a dashboard
  an empty table.
