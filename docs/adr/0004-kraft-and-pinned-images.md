# 0004. KRaft, and one place for every version pin

Status: accepted (2026-09-09)

## Context

The original compose ran `confluentinc/cp-zookeeper:7.5.0` alongside Kafka, used
`:latest` for six images, carried an obsolete `version: '3.8'` key, and hardcoded
`/Users/laurence` into three bind mounts on a machine whose home is
`/Users/ikenna`.

## Decision

**Zookeeper is gone.** Kafka 4.x removed Zookeeper support entirely, so the old
topology was not merely redundant, it was a dead end. Kafka runs as a single
node in KRaft mode with combined `broker,controller` roles.

**Two client listeners.** `INTERNAL` on 19092 advertised as `kafka:19092` for
containers, `EXTERNAL` on 9092 advertised as `localhost:9092` for tools on the
Mac. A single listener cannot be correct for both, and getting this wrong is the
most common reason a local Kafka appears to work from inside Docker and hangs
from outside it.

**Every tag is pinned, and every pin lives in `.env`.** No image tag appears in
any compose file. `make verify-images` checks each pin resolves against the
registry, without pulling, so a bad tag fails in seconds rather than eight
minutes into a pull.

**No hardcoded paths.** `INFRA_ROOT` and `PROJECT_ROOT` are computed by
`make bootstrap` on the machine that runs it. `make preflight` greps for
`/Users/laurence` and fails if it ever comes back.

**Topic auto-creation is off.** Topics are created explicitly with declared
partition counts. Auto-create is convenient until a typo in a producer silently
creates `naijapay.transacton.v1` with one partition and nobody notices.

## Consequences

* Upgrading anything is a one-line `.env` edit followed by `make verify-images`.
* `CLUSTER_ID` is fixed in `.env` rather than generated at startup, so the Kafka
  log directory survives a container recreate.
* The Airflow image's pip constraint URL is derived at build time from the base
  image's own Python version, rather than hardcoded to `constraints-3.12.txt`.
  A hardcoded constraints file works right up until the base image moves to
  3.13, and then fails with an unresolvable numpy that looks like a network
  problem.
