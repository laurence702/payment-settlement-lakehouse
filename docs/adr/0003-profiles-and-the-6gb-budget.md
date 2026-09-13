# 0003. Compose profiles, and the 6 GB memory budget

Status: accepted (2026-09-09)

## Context

Colima is configured with `--cpu 4 --memory 6 --disk 60` on a 16 GB MacBook.
The original shared-infra compose started ten services unconditionally,
including a Zookeeper that Kafka 4.x no longer supports. Nothing declared a
memory limit, so the first container to allocate aggressively got the RAM and
the rest were OOM-killed in whatever order the kernel chose.

## Decision

Every service sits behind a compose profile, every service declares `mem_limit`,
and the limits are sized backwards from one binding constraint: Airflow's
LocalExecutor forks tasks inside the scheduler container, so the local-mode
Spark driver has to fit under the scheduler's limit.

    core (postgres + minio + redis)   0.70 GB
    kafka                             0.77 GB
    clickhouse                        1.00 GB
    airflow api + scheduler + dagproc 2.71 GB   (scheduler 1.8 GB, holds Spark)
    ----------------------------------------
    total                             5.18 GB   leaves ~0.8 GB for dockerd

That total is the entire budget. Raising one number requires lowering another,
and the `.env` comments say so at the point of change.

## What is deliberately excluded from the default run

* **kafka-ui** has its own `ui` profile, not `stream`. It costs 0.5 GB and is a
  debugging convenience; it must not be running during a pipeline run.
* **TimescaleDB** is defined but off. With ClickHouse serving the marts it has
  no job in this pipeline. It stays for IoT-shaped work that actually needs a
  hypertable.
* **Prometheus and Grafana** are in `observe`, which does not fit alongside
  kafka, clickhouse and airflow at once. Stop kafka after ingest to make room.
* **The Airflow triggerer** is not defined at all. It exists only to service
  deferrable operators, and this DAG has none. It would cost 350 MB to run a
  component with nothing to do. Add it back the day a deferring sensor appears.
* **A Kafka JMX exporter** is not included, which is why Prometheus scrapes
  ClickHouse and MinIO but not Kafka. The sidecar costs about 150 MB. On this
  budget it lost to ClickHouse having headroom. This is a known gap, not an
  oversight.

## Consequences

* There is no `make up-all` target, on purpose.
* `make preflight` checks free memory, disk inside the VM, compose version, and
  host port collisions before anything is pulled. This machine runs Herd,
  native Postgres 13/14/15 and Mongo, so 5432 and 3000 are frequent casualties;
  Grafana is mapped to 3001 and ClickHouse's native port to 9100 because MinIO
  already owns 9000.
* `make mem` prints live usage against the budget, so drift is visible.
