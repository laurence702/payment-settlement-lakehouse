#!/usr/bin/env bash
# Guards this project's .env against the two ways it has actually broken.
#
# Compose reads ../data-engineering-shared-infra/.env first and this file
# second, and later wins. So copying a platform value here to silence an
# "unset variable" warning does not mirror it, it overrides it, and the two
# diverge silently. That shipped once as Airflow authenticating to postgres as
# a role that did not exist.
#
# The other failure mode is junk on a line: compose rejects any key containing
# a space, and the error names a line number with no other context.
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
fail=0
ok()  { printf '  \033[32mok\033[0m    %s\n' "$1"; }
bad() { printf '  \033[31mFAIL\033[0m  %s\n' "$1"; fail=1; }

[ -f .env ] || { bad ".env missing. Run: make bootstrap"; exit 1; }

# 1. Every meaningful line must be KEY=VALUE. Catches pasted prose.
if junk=$(grep -nvE '^[[:space:]]*(#|$)' .env | grep -vE '^[0-9]+:[A-Za-z_][A-Za-z0-9_]*=' ); then
  bad "line(s) in .env are not KEY=VALUE:"; echo "$junk" | sed 's/^/          /'
else
  ok "every .env line is a valid KEY=VALUE"
fi

# 2. No platform-owned key may appear here. This file is read LAST, so a copy
#    is an override, and the two silently diverge the moment one changes.
leaked=$(grep -nE '^[[:space:]]*(POSTGRES_|S3_|CLICKHOUSE_|KAFKA_|REDIS_|MINIO_|SEAWEEDFS_|AWSCLI_|PORT_S3|MEM_S3|GRAFANA_|PROMETHEUS_|TIMESCALE_)' .env || true)
if [ -n "$leaked" ]; then
  bad "platform key(s) copied into this project's .env, which OVERRIDES the platform:"
  echo "$leaked" | sed 's/^/          /'
  echo "          These belong in ../data-engineering-shared-infra/.env and nowhere else."
  echo "          They reach this project through env_file: and the --env-file pair on COMPOSE."
else
  ok "no platform keys leaked into this project's .env"
fi

# 3. Secrets must have been generated, not left as the committed placeholder.
#    Both must also be identical across every Airflow container; a mismatch
#    surfaces only as "Invalid auth token: Signature verification failed".
for v in AIRFLOW_JWT_SECRET AIRFLOW_FERNET_KEY; do
  val=$(grep -E "^${v}=" .env | head -1 | cut -d= -f2-)
  if [ "$val" = "GENERATE_ME" ] || [ -z "$val" ]; then
    bad "${v} is not set. Run: make bootstrap"
  else
    ok "${v} is set"
  fi
done

# 4. The paths this project cannot run without.
for v in PROJECT_ROOT INFRA_ROOT; do
  val=$(grep -E "^${v}=" .env | head -1 | cut -d= -f2- | tr -d '"')
  if [ -z "$val" ];      then bad "${v} is unset in .env. Run: make bootstrap"
  elif [ ! -d "$val" ];  then bad "${v}=${val} does not exist"
  else                        ok "${v} resolves"; fi
done

echo
[ "$fail" -eq 0 ] || { echo "project .env check FAILED"; exit 1; }
echo "project .env check passed"
