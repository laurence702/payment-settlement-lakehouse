# 0006. The platform layer is vendored into this repo, not shared

Status: accepted (2026-09-15). Not yet verified by a live `make bootstrap && make up`; see "What is not verified" below.

## What happened

This repo's `docker-compose.yml` pulled its platform services (Postgres,
Redis, SeaweedFS, Kafka, ClickHouse) from a sibling repo,
`data-engineering-shared-infra`, through Compose's `include:` directive. The
README's own quickstart told a reader to clone both repos side by side.

That sibling repo has never had a git remote. `git remote -v` in it returns
nothing, as of the date on this ADR. `scripts/publish.sh`'s own STEP 1 already
anticipated this exact failure mode: "if that repo is private or missing, the
quickstart is broken on arrival." It was missing. For anyone who is not
Laurence, on a fresh clone, the README's second `git clone` line 404s and
nothing after it runs.

Separately, `docker-compose.yml`'s own header comment claimed the sibling repo
was pulled in as a git submodule ("which is what makes `git clone
--recurse-submodules` enough for someone reviewing this"). There is no
`.gitmodules` file anywhere in this repo. That claim was never true; the
Quickstart's two manual `git clone` commands are what actually ran.

## Decision

Vendor the five services this pipeline actually uses (postgres, redis,
seaweedfs, kafka, clickhouse) directly into this repo's own
`docker-compose.yml`, `.env.example`, and `config/`/`init-scripts/`. Drop the
`include:` directive, `INFRA_ROOT`, and every script's delegation to
`-C $(INFRA)`. One clone, one `.env`, no second repo required for this
pipeline to run end to end.

`data-engineering-shared-infra` is not deleted or deprecated. It keeps its
full nine-service platform (this pipeline never used
timescaledb/prometheus/grafana/kafka-ui) for the other practice projects under
`Data_Engineering/` that do use them. The two copies of the five shared
services are independent from this point on: a config change in one does not
reach the other. That is a real, ongoing cost of this decision, not a one-time
migration tax, and the next person tuning `MEM_CLICKHOUSE` or a ClickHouse
`config.d` file needs to remember there are now two files, not one.

Rejected: **auto-clone the sibling repo from `make bootstrap` if it's
missing.** Keeps one source of truth for the platform layer, but still means
this repo cannot run without cloning and publishing a second repo, which is
the actual complaint ("it only makes sense for this project to be
self-contained"). Also does not fix the false submodule claim; it just makes
the manual step scripted instead of removed.

Rejected: **a real git submodule**, which is what the removed comment already
(incorrectly) claimed existed. Still one source of truth, still git-native,
but submodules are exactly the kind of friction a portfolio repo should not
hand a reviewer who is skimming ten repos in an afternoon: a `git clone`
that silently leaves `data-engineering-shared-infra/` empty until
`--recurse-submodules` or `submodule update --init` is remembered.

## What this actually touched

`docker-compose.yml` (the five services' definitions, verbatim from the
sibling repo's compose file, plus a renamed network so the two stacks cannot
collide if both are ever run at once: `naijapay_data_network`, not
`shared_data_network`), `.env.example` (platform image pins, credentials,
ports, memory ceilings — all previously only in the sibling repo's `.env`),
`Makefile` (`bootstrap`/`preflight`/`verify-images`/`up`/`ch` no longer
delegate to `-C $(INFRA)`), `scripts/preflight.sh` and
`scripts/verify-images.sh` (new, adapted from the sibling repo's, scoped down
to the five images/ports this repo actually needs), `scripts/check-env.sh`,
`scripts/urls.sh`, `scripts/demo.sh` (INFRA_ROOT sourcing and delegation
removed), `config/clickhouse/config.d/*.xml`, `config/clickhouse/users.d/limits.xml`,
`init-scripts/01-create-databases.sh` (copied in), and the README (Quickstart
is a single clone now; the architecture diagram and the memory budget section
no longer reference Grafana, which was never actually wired up here).

Container names (`dp_postgres`, `dp_kafka`, `dp_clickhouse`, ...) are
unchanged on purpose, so `scripts/create-topics.sh` and `scripts/demo.sh`'s
existing `docker exec dp_clickhouse ...` calls needed no edits.

## What is not verified

Every file above was written from the sibling repo's actual, working
definitions, not reconstructed from memory, and the compose file parses as
valid YAML with the right ten services, network and volumes. That is as far
as this could be checked: nothing here has run against a live Docker daemon.
Before trusting this ADR, run, in order: `make bootstrap` (on a machine that
does not already have a `.env` — an existing one predates these vars and
needs regenerating, see the note below), `make preflight`, `make build`,
`make demo`. If any platform container fails to start, the most likely cause
is a variable this port didn't carry over correctly from the sibling repo's
`.env`; diff against `data-engineering-shared-infra/.env` for that service.

`config/clickhouse/config.d/low-memory.xml` was copied as it stood in the
sibling repo at the time, including two lines (`listen_host` set to
`0.0.0.0`, and the removal of explicit `background_pool_size` /
`background_schedule_pool_size` caps) that were still under investigation
there for whether they undo the ClickHouse OOM fix this budget depends on.
Whatever that investigation concludes needs to be applied to this repo's copy
too; the two files do not sync themselves.

An existing `.env` from before this ADR will not pick up the ~25 new platform
keys automatically: `make bootstrap` only writes `.env` from `.env.example`
when `.env` does not already exist. Move the old one aside
(`mv .env .env.pre-vendor.bak`) and re-run `make bootstrap` rather than
editing it by hand; every value in it is a local-only development credential,
not a real secret worth preserving.

ADRs 0001 through 0005 still describe `INFRA_ROOT` and the two-repo layout in
places. They are left alone on purpose, same rule 0005 states for the MinIO
ADRs it supersedes: they record what was decided when it was decided. This
ADR supersedes them on where the platform layer's definitions live.


## Update, 2026-09-16: the container names still collided

The network rename above (`naijapay_data_network`, not `shared_data_network`)
was only half the isolation fix. Docker enforces container-name uniqueness
host-wide, not per compose project, so keeping `dp_postgres` / `dp_redis` /
`dp_seaweedfs` / `dp_s3_init` / `dp_kafka` / `dp_clickhouse` identical to
`data-engineering-shared-infra`'s own compose file meant only one of the two
stacks could ever have those containers running at a time. Whichever stack
started first claimed the names; the other's `docker compose up` (or
`--force-recreate`) failed with `Conflict. The container name "/dp_seaweedfs"
is already in use by container "..."`.

This surfaced in practice: `data-engineering-shared-infra`'s own
`docker compose up -d --force-recreate clickhouse` failed because this repo's
vendored `dp_seaweedfs` was already running under that name. That directly
contradicts this ADR's stated goal ("a renamed network so the two stacks
cannot collide if both are ever run at once") — the goal was right, the
implementation only did half of it.

Fixed by renaming all six vendored platform containers to the `np_` prefix
already used by this repo's own Airflow containers (`np_postgres`,
`np_redis`, `np_seaweedfs`, `np_s3_init`, `np_kafka`, `np_clickhouse`), in
`docker-compose.yml`, `Makefile` (`ch` target, `ps` target's now-redundant
second filter), `scripts/demo.sh`, `scripts/create-topics.sh`, and
`scripts/capture_seaweedfs_restart.sh`. This repo's containers and
`data-engineering-shared-infra`'s containers now use fully disjoint names, so
the two stacks can run simultaneously without either one blocking the
other's container creation. Not yet re-verified against a live Docker daemon
past a YAML parse and `bash -n`/`make -n` check; anyone with a currently
running `dp_*`-named stack from before this fix needs `make down` (or
`docker compose down`) once to remove the old containers before `make up`
creates the new `np_*`-named ones.
