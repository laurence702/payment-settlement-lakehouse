#!/usr/bin/env bash
# Bring the whole stack up from empty volumes, ready to accept a DAG run.
#
# Shared by `make e2e` and the benchmark runner so both start from the same
# known state: no Kafka topics, objects or warehouse tables left over from an
# earlier run. Leftover state can fail (or pass) a run on data that has nothing
# to do with the code under test; see the note in scripts/verify.sh.
#
# Usage: scripts/stack-fresh.sh            reset volumes, start, wait, create topics
#        scripts/stack-fresh.sh --keep-data  start without removing volumes
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

[ -f .env ] || { echo "no .env. Run: make bootstrap"; exit 2; }
set -a; . ./.env; set +a

export COMPOSE_PROFILES=core,stream,warehouse,airflow,ops

if [ "${1:-}" != "--keep-data" ]; then
  docker compose --env-file .env down --volumes --remove-orphans > /dev/null 2>&1 || true
fi
docker compose --env-file .env up -d --build > /dev/null

for i in $(seq 1 60); do
  s=$(docker inspect -f '{{.State.Health.Status}}' np_airflow_apiserver 2>/dev/null || echo starting)
  [ "$s" = healthy ] && break
  [ "$i" = 60 ] && { echo "airflow api-server never became healthy" >&2; exit 1; }
  sleep 10
done

./scripts/create-topics.sh > /dev/null
docker exec np_airflow_scheduler airflow dags unpause settlement_pipeline > /dev/null
echo "stack ready: Airflow on http://localhost:${PORT_AIRFLOW}"
