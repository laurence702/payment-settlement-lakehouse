#!/usr/bin/env bash
# Verification check. Validates full pipeline operation with an exit code
# and records structured execution evidence to disk.
#
# PASS means all of:
#   1. scripts/demo.sh exits 0 (stack up, DAG run succeeded)
#   2. no np_* container restarted or was OOM-killed during the run
#   3. no one-shot init container exited non-zero
#   4. mart_settlement_reconciliation has rows in ClickHouse
#   5. settlement variance is zero for NGN and non-zero for some USD rows
#
# Evidence lands in verify-report/ (gitignored; CI uploads it as an artifact).
# KEEP_UP=1 leaves the stack running afterwards for poking at.
# FRESH=0 reuses existing volumes. The default starts from empty volumes so
# that rows found in ClickHouse can only have come from this run.
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

REPORT=verify-report
rm -rf "$REPORT"; mkdir -p "$REPORT"
SUMMARY="$REPORT/summary.md"
fail=0
pass() { echo "- PASS $1" | tee -a "$SUMMARY"; }
bad()  { echo "- FAIL $1" | tee -a "$SUMMARY"; fail=1; }

[ -f .env ] || { echo "no .env. Run: make bootstrap"; exit 2; }
set -a; . ./.env; set +a

{
  echo "# verify $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo
  echo "commit: $(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
  echo "MEM_S3=${MEM_S3:-?} MEM_AIRFLOW_SCHEDULER=${MEM_AIRFLOW_SCHEDULER:-?} SPARK_DRIVER_MEMORY=${SPARK_DRIVER_MEMORY:-?}"
  echo
  echo "## checks"
} > "$SUMMARY"

# A background sampler, so a memory spike that ends in an OOM kill is on
# record even though the container is gone by the time anyone looks.
( while true; do
    date -u +%H:%M:%S
    docker stats --no-stream --format '{{.Name}} {{.MemUsage}} {{.MemPerc}}' 2>/dev/null
    sleep 15
  done ) > "$REPORT/mem-samples.txt" 2>&1 &
SAMPLER=$!
trap 'kill $SAMPLER 2>/dev/null' EXIT

if [ "${FRESH:-1}" = 1 ]; then
  COMPOSE_PROFILES=core,stream,warehouse,airflow \
    docker compose --env-file .env down -v --remove-orphans > "$REPORT/reset.log" 2>&1 || true
  echo "state: fresh volumes" >> "$SUMMARY"
else
  echo "state: reused volumes (FRESH=0); row counts may include earlier runs" >> "$SUMMARY"
fi

# 1. the end-to-end run
if ./scripts/demo.sh > "$REPORT/demo.log" 2>&1; then
  pass "demo.sh completed (DAG run succeeded)"
else
  bad "demo.sh exited non-zero. Tail of $REPORT/demo.log:"
  tail -25 "$REPORT/demo.log" | sed 's/^/    /' >> "$SUMMARY"
fi

# 2 and 3. container health, from docker's own record rather than a log grep
docker ps -a --filter "name=np_" \
  --format '{{.Names}}' | sort > "$REPORT/containers.txt"
container_fail=0
printf '%-28s %-10s %-8s %-6s %s\n' NAME STATUS RESTARTS OOM EXIT > "$REPORT/container-state.txt"
while read -r c; do
  [ -n "$c" ] || continue
  read -r status restarts oom code < <(docker inspect -f \
    '{{.State.Status}} {{.RestartCount}} {{.State.OOMKilled}} {{.State.ExitCode}}' "$c")
  printf '%-28s %-10s %-8s %-6s %s\n' "$c" "$status" "$restarts" "$oom" "$code" >> "$REPORT/container-state.txt"
  unhealthy=0
  if [ "$restarts" != 0 ] || [ "$oom" = true ]; then
    bad "$c restarted $restarts time(s), OOMKilled=$oom"; unhealthy=1
  fi
  if [ "$status" = exited ] && [ "$code" != 0 ]; then
    bad "$c exited with code $code"; unhealthy=1
  fi
  if [ "$unhealthy" = 1 ]; then
    container_fail=1
    docker logs --tail 200 "$c" > "$REPORT/logs-$c.txt" 2>&1
  fi
done < "$REPORT/containers.txt"
if [ ! -s "$REPORT/containers.txt" ]; then
  bad "no np_* containers found; the stack never started"
elif [ "$container_fail" = 0 ]; then
  pass "no container restarts, OOM kills or failed init containers"
fi

# Kernel view of any OOM kill. Needs CAP_SYSLOG; fine on CI, skipped locally
# if sudo would prompt.
if sudo -n true 2>/dev/null; then
  sudo -n dmesg -T 2>/dev/null | grep -iE 'oom-kill|out of memory' > "$REPORT/dmesg-oom.txt" || true
fi

# 4. the output exists
rows=$(docker exec np_clickhouse clickhouse-client \
  --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" \
  --query "SELECT count() FROM ${CLICKHOUSE_DB}.mart_settlement_reconciliation" 2>"$REPORT/clickhouse-err.txt" || echo "")
if [[ "$rows" =~ ^[0-9]+$ ]] && [ "$rows" -gt 0 ]; then
  pass "mart_settlement_reconciliation has $rows rows"
else
  bad "mart_settlement_reconciliation empty or unreadable (got '${rows}'; see $REPORT/clickhouse-err.txt)"
fi

# 5. the reconciliation says something true. NGN settles at the charged amount,
# so its variance must be exactly zero; USD settles at a later FX rate, so a
# run where every USD variance is zero means settlement FX is being reused.
variance=$(docker exec np_clickhouse clickhouse-client \
  --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --format TSV \
  --query "SELECT
             countIf(currency = 'NGN' AND is_settled AND settlement_variance_kobo != 0),
             countIf(currency = 'USD' AND is_settled AND settlement_variance_kobo != 0)
           FROM ${CLICKHOUSE_DB}.mart_settlement_reconciliation" 2>>"$REPORT/clickhouse-err.txt" || echo "")
read -r ngn_off usd_drift <<< "${variance:-x x}"
if [[ "$ngn_off" =~ ^[0-9]+$ ]] && [[ "$usd_drift" =~ ^[0-9]+$ ]]; then
  if [ "$ngn_off" -eq 0 ] && [ "$usd_drift" -gt 0 ]; then
    pass "variance: NGN settled rows all zero, $usd_drift USD settled rows show FX drift"
  else
    bad "variance: $ngn_off NGN settled rows non-zero (want 0), $usd_drift USD rows with FX drift (want > 0)"
  fi
else
  bad "variance check could not read the mart (got '${variance}')"
fi

# Task logs for the latest DAG run, copied out before teardown removes them.
latest_run=$(docker exec np_airflow_scheduler sh -c \
  'ls -1td /opt/airflow/logs/dag_id=naijapay_pipeline/run_id=* 2>/dev/null | head -1' 2>/dev/null || true)
if [ -n "$latest_run" ]; then
  docker cp "np_airflow_scheduler:$latest_run" "$REPORT/airflow-task-logs" > /dev/null 2>&1 \
    && echo "- task logs: $REPORT/airflow-task-logs/" >> "$SUMMARY"
fi

docker stats --no-stream --format 'table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}' > "$REPORT/mem-final.txt" 2>&1

{
  echo
  echo "## containers"
  echo '```'
  cat "$REPORT/container-state.txt"
  echo '```'
  echo
  [ "$fail" = 0 ] && echo "RESULT: PASS" || echo "RESULT: FAIL"
} >> "$SUMMARY"

if [ "${KEEP_UP:-0}" != 1 ]; then
  COMPOSE_PROFILES=core,stream,warehouse,airflow \
    docker compose --env-file .env down --remove-orphans > /dev/null 2>&1 || true
fi

echo
cat "$SUMMARY"
exit "$fail"
