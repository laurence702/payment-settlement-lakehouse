# 0003. Compose profiles, and the 6 GB memory budget

Status: accepted (2026-09-09)

## Context

The container runtime VM is configured with 4 CPUs, 6 GB memory, and 60 GB disk.
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
  host port collisions before anything is pulled. Local development environments
  frequently run native background daemons (e.g. Postgres, Redis, local web servers);
  mapped ports are configured to avoid standard collisions (e.g. ClickHouse native port
  remapped to 9100).
* `make mem` prints live usage against the budget, so drift is visible.

## Update 2026-11-15: analyst console

The `ops` profile adds one container, the analyst console (`MEM_OPS=128m`).
It shares the existing Postgres server, so it adds no database process. To keep
the total unchanged, two ceilings were lowered by 64 MB each:

    redis         96 MB -> 32 MB    observed peak 5 MiB
    api-server   512 MB -> 448 MB   observed peak 278 MiB

The 128 MB ceiling for the console is an estimate for one uvicorn worker and a
small psycopg2 pool, not a measurement. `make mem` after a `make verify` run
is where it gets confirmed or corrected. If it needs more, the donor is chosen
from a measured peak, not guessed, and this section is updated in the same
commit.

## Update 2026-11-16: measured corrections, the v2 merge, and the large profile

Measurement during `make verify` showed the 2026-11-15 section above was too
tight in one place. Redis peaked at 20 MiB against its 32 MB ceiling, not at
5 MiB. The merge with the v2 scale work reconciled the ceilings once, in
`.env.example` and `config/profiles/small.env`:

    redis        32 MB -> 48 MB     measured 20 MiB of 32 MB
    clickhouse  1536 MB -> 1280 MB  v2 removed the query-log tables and set
                                    max_server_memory_usage=0; measured peak
                                    about 1 GiB
    seaweedfs    768 MB -> 1024 MB  v2 finding: weed was OOM-killed at 768 MB;
                                    GOMEMLIMIT now sits 256 MiB under the cap
                                    and is configurable as S3_GOMEMLIMIT
    dag-processor 400 MB -> 384 MB  offsets the redis increase

The summed ceilings are 6200 MB, the same as `main` before the console. They
exceed a 6 GB VM's usable memory on purpose: services do not peak together, and
a hard sum under the VM size would leave the heaviest stages starved. Preflight
now prints the sum next to the VM's real memory and warns when it is larger.

### Profiles

`config/profiles/small.env` is the 6 GB design target and the default.
`medium.env` (8 GB VM) raises only the scheduler container and Spark's driver,
the one service that hit its ceiling at 500k to 1M events. `large.env` (10 GB VM)
raises them further. 10 GB is the hard upper limit this project allows itself;
medium is tried first and large only if medium fails on memory. Apply
one with `make profile PROFILE=small|medium|large`; only the keys a profile lists are
rewritten, so generated secrets are never touched. These profiles exist
because at that scale the container ceilings, not the pipeline, decide whether
a run finishes. It is not a claim about any measured result: figures belong in
`docs/benchmarks.md` and come from `make benchmark` summaries only.

### Measured peaks at 1M events (2026-11-16)

On the `medium` profile, one run of 999,947 events (docs/benchmarks.md) peaked
at 4,094 MiB across all containers. Per container: scheduler 2,430 of 3,328,
clickhouse 794 of 2,048, kafka 562 of 768, seaweedfs 561 of 1,024, api-server
302 of 448, dag-processor 216 of 384, postgres 107 of 320, console 54 of 128,
redis 8 of 48. Two consequences:

* The 6 GB profile's 1,800 MiB scheduler ceiling is below what 1M events need
  (2,430 MiB), which is why 1M fails there.
* ClickHouse's measured 39% is a lower bound, because the load lasts about 3 s
  and the sampler runs every 5 s. No ceiling is changed on this evidence; a
  trim needs three repeat runs first.
