#!/usr/bin/env bash
# One command, start to dashboard. Written to be watchable: it says what it is
# doing and why each wait exists, because a script that prints nothing for four
# minutes is indistinguishable from a hung one.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

[ -f .env ] || { echo "no .env. Run: make bootstrap"; exit 1; }
set -a; . ./.env; set +a

step() { printf '\n\033[1m==> %s\033[0m\n' "$1"; }

step "preflight"
make -s preflight

step "starting platform and airflow (~5.5 GB, this is the whole budget)"
COMPOSE_PROFILES=core,stream,warehouse,airflow \
  docker compose --env-file .env up -d

step "waiting for airflow api-server to pass its healthcheck"
for i in $(seq 1 60); do
  s=$(docker inspect -f '{{.State.Health.Status}}' np_airflow_apiserver 2>/dev/null || echo starting)
  [ "$s" = healthy ] && { echo "  healthy after ${i}0s"; break; }
  [ "$i" = 60 ] && { echo "  api-server never became healthy"; docker logs --tail 40 np_airflow_apiserver; exit 1; }
  printf '  %s (%d/60)\r' "$s" "$i"; sleep 10
done

step "creating kafka topics"
./scripts/create-topics.sh

step "unpausing and triggering the DAG"
docker exec np_airflow_scheduler airflow dags unpause naijapay_pipeline >/dev/null
RUN_ID="demo__$(date -u +%Y%m%dT%H%M%S)"
docker exec np_airflow_scheduler airflow dags trigger naijapay_pipeline --run-id "$RUN_ID"

step "waiting for the run to finish (generate, ingest, spark, dbt, clickhouse)"
echo "  follow along at http://localhost:${PORT_AIRFLOW}"
# Ask the CLI to filter by state rather than parsing a column out of its
# table output. Three things were wrong with doing it the other way:
#
#   -d is not a flag. `dags list-runs` takes dag_id positionally, so argparse
#   rejected it and exited 2, which pipefail turned into the whole script
#   dying on the first poll with no message.
#
#   `| head -1` under `set -o pipefail` can kill the script on SIGPIPE.
#
#   `{print $3}` assumed a column position in output nobody promised to keep.
run_state() {
  local out
  out=$(docker exec np_airflow_scheduler \
          airflow dags list-runs naijapay_pipeline --state failed -o plain 2>/dev/null || true)
  case "$out" in *"$RUN_ID"*) printf 'failed'; return ;; esac
  out=$(docker exec np_airflow_scheduler \
          airflow dags list-runs naijapay_pipeline --state success -o plain 2>/dev/null || true)
  case "$out" in *"$RUN_ID"*) printf 'success'; return ;; esac
  printf 'running'
}

for i in $(seq 1 120); do
  state=$(run_state)
  case "$state" in
    success) echo "  succeeded after ~$((i*10))s"; break ;;
    failed)
      echo "  FAILED. Per-task state:"
      docker exec np_airflow_scheduler \
        airflow tasks states-for-dag-run naijapay_pipeline "$RUN_ID" || true
      echo
      echo "  Open http://localhost:${PORT_AIRFLOW} and click the red task, then Logs."
      exit 1 ;;
    *) printf '  running (%d/120)\r' "$i"; sleep 10 ;;
  esac
done

step "what landed in ClickHouse"
docker exec np_clickhouse clickhouse-client \
  --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery <<SQL
SELECT table, formatReadableQuantity(sum(rows)) AS rows
FROM system.parts WHERE database = '${CLICKHOUSE_DB}' AND active
GROUP BY table ORDER BY table FORMAT PrettyCompactMonoBlock;

SELECT
    reconciliation_bucket,
    count()                                        AS transactions,
    round(sum(outstanding_net_kobo)/100/1e6, 2)    AS outstanding_ngn_millions
FROM ${CLICKHOUSE_DB}.mart_settlement_reconciliation
GROUP BY reconciliation_bucket
ORDER BY transactions DESC FORMAT PrettyCompactMonoBlock;
SQL

cat <<MSG

Done.

  Airflow      http://localhost:${PORT_AIRFLOW}   (${AIRFLOW_ADMIN_USER} / ${AIRFLOW_ADMIN_PASSWORD})
  Files        http://localhost:${PORT_S3_UI}/buckets/   (SeaweedFS filer UI: raw/staged/marts)
  ClickHouse   http://localhost:${PORT_CLICKHOUSE_HTTP}/play

MSG
