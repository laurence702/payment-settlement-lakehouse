#!/usr/bin/env bash
# Browser-driven end-to-end test: reset the stack, run the pipeline from the
# Airflow UI with Playwright, leave screenshots in e2e-report/.
#
# E2E_EVENTS (default 5000) sets the generated transaction count.
# E2E_KEEP_DATA=1 skips the volume reset; KEEP_UP=1 leaves the stack running.
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

[ -x .venv/bin/playwright ] || { echo "Playwright missing. Run: make e2e-setup"; exit 2; }

rm -rf e2e-report
if [ "${E2E_KEEP_DATA:-0}" = 1 ]; then ./scripts/stack-fresh.sh --keep-data; else ./scripts/stack-fresh.sh; fi || exit 1

.venv/bin/python -m pytest e2e -m e2e -p no:cacheprovider
status=$?

if [ "${KEEP_UP:-0}" != 1 ]; then
  COMPOSE_PROFILES=core,stream,warehouse,airflow,ops \
    docker compose --env-file .env down --remove-orphans > /dev/null 2>&1 || true
fi
exit "$status"
