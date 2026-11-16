#!/usr/bin/env bash
# Verification check. Validates full pipeline operation with an exit code
# and records structured execution evidence to disk.
#
# PASS means all of:
#   1. scripts/demo.sh exits 0 (stack up, DAG run succeeded)
#   2. no np_* container restarted or was OOM-killed during the run
#   3. no one-shot init container exited non-zero
#   4. mart_settlement_reconciliation has rows in ClickHouse
#   5. an analyst decision made through the ops console survives a second,
#      full pipeline run (status still CLOSED, exactly one audit row)
#
# Evidence lands in verify-report/ (gitignored; CI uploads it as an artifact).
# KEEP_UP=1 leaves the stack running afterwards for poking at.
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

# 0. Start from empty volumes. A done-check that inherits Kafka topics, object
#    store contents or ClickHouse tables from earlier runs can fail (or pass) on
#    data that has nothing to do with the code under test: on 2026-11-16 leftover
#    events from earlier runs produced 650 negative settlement lags that a clean
#    run does not. VERIFY_KEEP_DATA=1 skips this when you want to inspect state.
if [ "${VERIFY_KEEP_DATA:-0}" != 1 ]; then
  COMPOSE_PROFILES=core,stream,warehouse,airflow,ops \
    docker compose --env-file .env down --volumes --remove-orphans > "$REPORT/reset.log" 2>&1 || true
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

# 5. the ops loop. An analyst's decision must survive a pipeline re-run, which
#    is the guarantee the two-phase case state exists to give (docs/adr/0007).
#    Close one break through the console API, run the whole pipeline again, and
#    check the decision is still there and was not duplicated.
OPS="http://127.0.0.1:${PORT_OPS:-8090}"
json_get() {
  # Walk a JSON document by keys and list indexes; a list prints its length.
  python3 -c 'import json, sys
d = json.load(sys.stdin)
for k in sys.argv[1:]:
    d = d[int(k)] if isinstance(d, list) else d[k]
print(len(d) if isinstance(d, list) else d)' "$@"
}

ops_ready=0
for _ in $(seq 1 30); do
  curl -fsS "$OPS/readyz" >/dev/null 2>&1 && { ops_ready=1; break; }
  sleep 2
done

if [ "$fail" != 0 ]; then
  echo "- SKIP ops loop (the pipeline run already failed)" | tee -a "$SUMMARY"
elif [ "$ops_ready" != 1 ]; then
  bad "ops console never became ready at $OPS"
else
  CASE_ID=$(curl -fsS "$OPS/api/cases?limit=1" | json_get items 0 case_id 2>/dev/null || echo "")
  if [[ ! "$CASE_ID" =~ ^[0-9a-f]{64}$ ]]; then
    bad "ops console has no break to triage after the first run"
  else
    KEY="verify-$(date -u +%Y%m%dT%H%M%S)"
    code=$(curl -sS -o "$REPORT/ops-close.json" -w '%{http_code}' -X POST \
      "$OPS/api/cases/$CASE_ID/actions" \
      -H "Idempotency-Key: $KEY" -H "X-Analyst: verify-bot" -H "Content-Type: application/json" \
      -d '{"action": "CLOSE", "notes": "closed by make verify to prove it survives a re-run"}')
    if [ "$code" != 201 ]; then
      bad "closing a case through the console returned HTTP $code"
    else
      RERUN_ID="verify_rerun__$(date -u +%Y%m%dT%H%M%S)"
      # Same events, read again from the start of the topics by a new consumer
      # group, so the marts are rebuilt from identical input.
      docker exec np_airflow_scheduler airflow dags trigger settlement_pipeline \
        --run-id "$RERUN_ID" \
        --conf '{"skip_generate": true, "consumer_group_suffix": "verify-rerun"}' \
        > "$REPORT/ops-rerun-trigger.log" 2>&1
      rerun_state=running
      for i in $(seq 1 120); do
        failed=$(docker exec np_airflow_scheduler airflow dags list-runs settlement_pipeline \
                   --state failed -o plain 2>/dev/null || true)
        done_ok=$(docker exec np_airflow_scheduler airflow dags list-runs settlement_pipeline \
                   --state success -o plain 2>/dev/null || true)
        case "$failed" in *"$RERUN_ID"*) rerun_state=failed; break ;; esac
        case "$done_ok" in *"$RERUN_ID"*) rerun_state=success; break ;; esac
        sleep 10
      done
      if [ "$rerun_state" != success ]; then
        bad "second pipeline run ended as '$rerun_state'"
      else
        curl -fsS "$OPS/api/cases/$CASE_ID" > "$REPORT/ops-after-rerun.json"
        status=$(json_get case effective_status < "$REPORT/ops-after-rerun.json")
        actions=$(json_get actions < "$REPORT/ops-after-rerun.json")
        if [ "$status" = CLOSED ] && [ "$actions" = 1 ]; then
          pass "analyst decision survived a full pipeline re-run (status CLOSED, 1 audit row)"
        else
          bad "decision did not survive the re-run (status '$status', audit rows '$actions')"
        fi
      fi
    fi
  fi
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
  COMPOSE_PROFILES=core,stream,warehouse,airflow,ops \
    docker compose --env-file .env down --remove-orphans > /dev/null 2>&1 || true
fi

echo
cat "$SUMMARY"
exit "$fail"
