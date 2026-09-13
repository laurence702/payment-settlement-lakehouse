# 0005. SeaweedFS replaces MinIO as the object store

Status: accepted (2026-09-11). Supersedes the memory figures in 0003.

## What happened

The MinIO image pins in this repo were wrong twice.

The first time they named `RELEASE.2026-05-20T09-04-27Z` and a matching `mc`
tag, neither of which had ever existed. `make verify-images` caught that.

The second time, on 2026-09-10, they were "corrected" to
`RELEASE.2025-10-15T17-29-55Z`, described in `.env` as the last real community
release. That is also not a real image. It is a real GitHub release with real
source and binaries, but it was never pushed to Docker Hub:

* `hub.docker.com/v2/repositories/minio/minio/tags/RELEASE.2025-10-15T17-29-55Z`
  returns 404.
* A `name=RELEASE.2025-1` tag filter on that repository returns `count: 0`.
* `minio/minio:latest` still points at `RELEASE.2025-09-07T16-13-09Z`,
  pushed 2025-09-07.

So `make verify-images` would have failed on it. It was not run after the edit.
That is the more useful lesson than either bad tag: a check that exists is not
a check that ran, and the one file in this repo that is allowed to contain a
version was the file that went unverified.

## The actual situation, checked rather than assumed

MinIO did not merely go quiet. In October 2025 it **stopped publishing free
Docker images altogether**. The last one is `RELEASE.2025-09-07T16-13-09Z`.

The release that followed it, `RELEASE.2025-10-15T17-29-55Z`, was a security
fix for privilege escalation via session policy bypass. It exists only as
source and binaries. So the newest MinIO image anyone can pull is permanently
behind a known privilege-escalation fix, with no upgrade path that does not
involve building the server yourself.

The web console had already been stripped from the community edition in
mid-2025, which is why the README's "browse the raw/staged/marts buckets"
instruction had stopped working.

For a repository whose entire premise is that every version is a pinned image
tag in one `.env` file, that combination is not a stopgap. There is nothing
left to pin.

## Decision

**SeaweedFS 4.46**, pinned as `chrislusf/seaweedfs:4.46`, verified on Docker
Hub on 2026-09-11 as pushed 2026-09-08. Releases 4.41 through 4.46 shipped
between 2026-08-06 and 2026-09-08.

The reasons that actually decided it, in order:

1. **ClickHouse ships first-party documentation for SeaweedFS as an S3 target.**
   One of this pipeline's three object-store consumers has an officially tested
   path. Garage has no equivalent.
2. **The bootstrap keeps its current shape.** Static credentials come from
   environment variables, and buckets are created over the S3 API, so the old
   `minio-init` container becomes `s3-init` with the same four `mb` calls and
   the same `service_completed_successfully` gate. Garage needs a
   `layout assign` / `layout apply` step, generates rather than accepts key
   pairs, and wants bucket administration over its own RPC rather than S3.
3. **Release cadence.** Weekly to fortnightly, which is the exact opposite of
   the problem being solved.

Rejected: **Garage v2.4.1**, which is the better pure fit for the memory
ceiling and has the smaller image by a wide margin. It lost on the bootstrap
tax above, and on not supporting object versioning, which closes the door on
an Iceberg or time-travel demonstration later.

Rejected: **staying on MinIO**, because "stay" now means frozen on a Sept 2025
image one security fix behind its own final release, forever.

## What this actually cost

**Nineteen files, not the five this ADR previously claimed.** The old list
missed `config/clickhouse/config.d/minio.xml` in the platform repo, this
project's own compose env block, `.env`, the DAG, the dbt profile and project
files, `serve.py`'s named collection, both READMEs, both Makefiles, the demo
script and two test docstrings.

Almost all of that was renaming `MINIO_*` to `S3_*`. **That rename is the real
finding.** Naming the variables after the vendor is what turned a
protocol-level swap into a nineteen-file change. They are now `S3_ENDPOINT`,
`S3_ACCESS_KEY` and `S3_SECRET_KEY`, the ClickHouse named collection is
`s3_lakehouse` rather than `minio_lakehouse`, and the compose service is
`seaweedfs`. The next swap should be one line in `.env` plus one compose
service.

The claim in the previous draft that held up: nothing in `ingest.py`,
`transform_spark.py`, the dbt SQL or the ClickHouse `s3()` calls changed.
pyarrow's `S3FileSystem`, DuckDB `httpfs` and ClickHouse all speak plain S3 and
none of them noticed. Putting an S3 API in the middle is what made this a
rename rather than a rewrite.

## What was given up

**Anonymous read on `lakehouse-marts`.** The old init container ran
`mc anonymous set download local/lakehouse-marts`. Credentials now come from
`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`, which SeaweedFS turns into a
single static Admin identity, and that mechanism has no anonymous slot. Nothing
in the pipeline ever read the marts bucket unauthenticated, so this is a
capability lost rather than a regression. Restoring it means mounting an
`-s3.config` file with an identity named `anonymous` scoped to
`["Read:lakehouse-marts", "List:lakehouse-marts"]`, at the cost of the
credentials then living in two files instead of one.

**A 400 MB init image.** `amazon/aws-cli:2.36.43` replaces `minio/mc` so that
no MinIO artifact is left in the stack. That is a fat image to create four
buckets. It runs once and exits, and it gates on the S3 API itself rather than
on a server-internal RPC, which is what `depends_on` downstream actually needs
to be true. Note that aws-cli v2 sends upload checksums by default and some
S3-compatible servers reject them; `create-bucket` and `ls` are unaffected, but
do not assume `aws s3 cp` works here without testing it.

## The number that is not verified

`MEM_S3=384m`, up from MinIO's `320m`, is an **estimate**. SeaweedFS runs
master, volume, filer and the S3 gateway in one Go process, and Go's garbage
collector grows toward whatever ceiling it is given rather than settling at a
floor. The total budget therefore reads 5.24 GB rather than 0003's 5.18 GB,
leaving about 0.76 GB for the docker daemon.

This has not been measured under load. Run `make mem` during a Spark parquet
multipart write before treating 384m as real. If the container is OOM-killed
mid-write, take the difference off `MEM_CLICKHOUSE`, which has headroom, and
not off the Airflow scheduler, which is the binding constraint in 0003.

## Migration note

The volume changed from `minio_data` to `seaweedfs_data`. The old volume is
orphaned, not migrated. Every byte in it is regenerable synthetic data, so the
correct action is to re-run the pipeline and then delete the old volume:

```bash
docker volume ls | grep minio_data
docker volume rm <name>
```

`config/clickhouse/config.d/minio.xml` has been emptied and superseded by
`s3.xml` in the same directory. Delete it.

ADRs 0001 through 0004 still say MinIO. They are left alone on purpose: they
record what was decided when it was decided. This ADR supersedes them on the
identity of the object store and on the memory figures in 0003.

## Two things changed that are not part of the swap

**`make preflight` now runs `verify-images` itself.** The pin check already
existed and was already correct. It simply was not run on the day it mattered.
Leaving it as a separate target that a human has to remember is the same design
that produced the bug, so it is now part of the one command that runs before
anything starts.

**`tests/test_config.py` is new.** The suite was 29 tests and not one of them
imported `config`. Every setting this swap renamed could have been left half
finished and the suite would still have been green, because the object-store
configuration layer had no coverage at all. It now has seven assertions
covering the environment reads, the `host:port` versus `http://host:port`
split that pyarrow and DuckDB disagree about, the bucket names that are baked
into object prefixes, and a direct check that no `minio_*` field survives on
`Settings`. That last one is the test that would have caught this change going
in half done.
