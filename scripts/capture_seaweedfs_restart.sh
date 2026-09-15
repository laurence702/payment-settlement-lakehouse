#!/usr/bin/env bash
# capture_seaweedfs_restart.sh
#
# Watches dp_seaweedfs's container StartedAt timestamp. The moment it
# changes (i.e. the container restarted), snapshots docker inspect state,
# docker events, raw docker logs, container stats, and Colima VM memory
# and dmesg -- into one timestamped report file, no manual copy-paste.
#
# Also keeps a rolling 5s sample of "docker stats" + "free -h" in the
# background, so the report includes the state leading UP TO the
# restart, not just after it.
#
# Does NOT trigger the DAG itself -- start this running first, then
# trigger naijapay_pipeline from the Airflow UI as usual. Stops after
# TIMEOUT_SECS (default 360s / 6 min) or Ctrl+C.
#
# Usage: bash scripts/capture_seaweedfs_restart.sh [timeout_secs]

set -uo pipefail

REPORT="seaweedfs_restart_capture_$(date +%Y%m%dT%H%M%S).log"
CONTAINER="dp_seaweedfs"
TIMEOUT_SECS="${1:-360}"
SAMPLES_FILE="$(mktemp)"

log() { echo "[$(date -u +%H:%M:%S)] $*" | tee -a "$REPORT"; }

cleanup() {
  [ -n "${SAMPLER_PID:-}" ] && kill "$SAMPLER_PID" 2>/dev/null
  rm -f "$SAMPLES_FILE"
}
trap cleanup EXIT

# background sampler: every 5s, append a stats+memory snapshot, keep only
# the last 6 samples (~30s of history) so the report shows the run-up
sampler() {
  while true; do
    {
      echo "@@@ sample $(date -u +%H:%M:%S) @@@"
      docker stats --no-stream 2>&1
      colima ssh -- free -h 2>&1
    } >> "$SAMPLES_FILE"
    # trim to last 6 sample blocks (marker-delimited)
    awk '/^@@@ sample/{n++} {print > "'"$SAMPLES_FILE"'.tmp"}' "$SAMPLES_FILE" 2>/dev/null
    tail -c 20000 "$SAMPLES_FILE" > "$SAMPLES_FILE.trim" 2>/dev/null && mv "$SAMPLES_FILE.trim" "$SAMPLES_FILE"
    rm -f "$SAMPLES_FILE.tmp"
    sleep 5
  done
}
sampler &
SAMPLER_PID=$!

log "=== Watching ${CONTAINER} for restarts (timeout ${TIMEOUT_SECS}s). Trigger the DAG now. ==="

BASELINE_START=$(docker inspect "$CONTAINER" --format '{{.State.StartedAt}}' 2>/dev/null)
log "Baseline StartedAt=${BASELINE_START}"

START_TS=$(date +%s)
RESTART_COUNT=0

while true; do
  ELAPSED=$(( $(date +%s) - START_TS ))
  if [ "$ELAPSED" -ge "$TIMEOUT_SECS" ]; then
    log "Timeout reached, stopping."
    break
  fi

  CURRENT_START=$(docker inspect "$CONTAINER" --format '{{.State.StartedAt}}' 2>/dev/null)
  if [ -n "$CURRENT_START" ] && [ "$CURRENT_START" != "$BASELINE_START" ]; then
    RESTART_COUNT=$((RESTART_COUNT + 1))
    log "*** RESTART #${RESTART_COUNT}: ${BASELINE_START} -> ${CURRENT_START} ***"

    NOW_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    {
      echo "--- docker inspect State ---"
      docker inspect "$CONTAINER" --format '{{json .State}}'
      echo
      echo "--- docker events, last 30s, this container only ---"
      docker events --since 30s --until "$NOW_UTC" --filter container="$CONTAINER" 2>&1
      echo
      echo "--- docker logs, last 20s, raw (no keyword filter) ---"
      docker logs "$CONTAINER" --since 20s --timestamps 2>&1
      echo
      echo "--- stats/memory samples leading up to and after the restart ---"
      cat "$SAMPLES_FILE" 2>/dev/null
      echo
      echo "--- Colima VM dmesg tail (trying sudo, since plain dmesg needs CAP_SYSLOG) ---"
      colima ssh -- sudo dmesg -T 2>&1 | tail -40
      echo "=================================================="
    } >> "$REPORT"
    BASELINE_START="$CURRENT_START"
  fi

  sleep 1
done

log "=== Done. ${RESTART_COUNT} restart(s) captured in ${REPORT} ==="
