#!/usr/bin/env bash
# Publish payment-settlement-lakehouse (and its sibling platform repo) to GitHub.
#
# Run from ~/development/Data_Engineering/payment-settlement-lakehouse
# Nothing here is destructive until STEP 3. Steps 0-2 only read.

set -euo pipefail

OWNER="laurence702"
REPO="payment-settlement-lakehouse"
INFRA="../data-engineering-shared-infra"

# ---------------------------------------------------------------------------
# STEP 0 — secret check. Do this before the repo is public, not after.
# ---------------------------------------------------------------------------
echo "== .env must be ignored and must never have been committed =="
git check-ignore -v .env || { echo "FAIL: .env is not ignored"; exit 1; }

echo "== has .env ever been committed? (want: no output) =="
git log --all --oneline -- .env || true

echo "== scan full history for anything that looks like a live secret =="
git grep -nIE '(AKIA[0-9A-Z]{16}|sk_live_|pk_live_|-----BEGIN [A-Z ]*PRIVATE KEY|ghp_[A-Za-z0-9]{36})' \
  $(git rev-list --all) -- 2>/dev/null | head -20 || echo "clean"

echo "== every GENERATE_ME placeholder still a placeholder in .env.example =="
grep -nE 'GENERATE_ME|_local_only' .env.example || true

read -rp "History clean? [y/N] " ok; [ "$ok" = "y" ] || exit 1

# ---------------------------------------------------------------------------
# STEP 1 — the README tells readers to clone the sibling repo. If that repo
# is private or missing, the quickstart is broken on arrival. Publish it first.
# ---------------------------------------------------------------------------
if [ -d "$INFRA/.git" ]; then
  ( cd "$INFRA"
    git remote get-url origin >/dev/null 2>&1 || \
      gh repo create "$OWNER/data-engineering-shared-infra" \
        --public --source=. --remote=origin --push \
        --description "Local data platform for the payment-settlement-lakehouse pipeline. Postgres, Kafka (KRaft), ClickHouse, SeaweedFS, Redis, Prometheus and Grafana, every service behind a Compose profile with a memory limit, every image tag pinned in one place."
    gh repo edit "$OWNER/data-engineering-shared-infra" \
      --add-topic docker-compose --add-topic kafka --add-topic clickhouse \
      --add-topic seaweedfs --add-topic data-engineering --add-topic local-development
  )
else
  echo "WARN: $INFRA has no git repo. The README's clone instruction will 404."
fi

# ---------------------------------------------------------------------------
# STEP 2 — tests must be green before the badge goes on a public README.
# ---------------------------------------------------------------------------
make lint
make test

# ---------------------------------------------------------------------------
# STEP 3 — publish. First write that leaves the machine.
# ---------------------------------------------------------------------------
git remote get-url origin >/dev/null 2>&1 || \
  gh repo create "$OWNER/$REPO" --public --source=. --remote=origin --push

gh repo edit "$OWNER/$REPO" \
  --description "Settlement reconciliation for Nigerian card and bank-transfer payments. Kafka to Spark to dbt/DuckDB to ClickHouse, orchestrated by Airflow, built to run inside a 6 GB VM. 35 tests, 5 ADRs, and a known-gaps section." \
  --homepage "https://keonix.dev" \
  --enable-issues \
  --enable-wiki=false \
  --add-topic data-engineering \
  --add-topic airflow \
  --add-topic dbt \
  --add-topic pyspark \
  --add-topic duckdb \
  --add-topic clickhouse \
  --add-topic kafka \
  --add-topic lakehouse \
  --add-topic medallion-architecture \
  --add-topic parquet \
  --add-topic elt \
  --add-topic data-pipeline \
  --add-topic data-quality \
  --add-topic fintech \
  --add-topic payments \
  --add-topic reconciliation \
  --add-topic nigeria \
  --add-topic docker-compose \
  --add-topic python

echo
echo "Published: https://github.com/$OWNER/$REPO"
echo
echo "Two things gh cannot do, do them in the browser:"
echo "  1. Pin the repo:  github.com/$OWNER  ->  Customize your pins"
echo "  2. Check the tests badge goes green:  github.com/$OWNER/$REPO/actions"
