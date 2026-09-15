#!/usr/bin/env bash
# Every port is read from .env, never printed as a literal. A hardcoded URL
# here is the same bug as a hardcoded image tag in a compose file.
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
set -a; . ./.env; set +a

printf '\n'
printf '  %-11s http://localhost:%s\n'            "Airflow"    "${PORT_AIRFLOW}"
printf '  %-11s %s / %s\n'                        ""           "${AIRFLOW_ADMIN_USER}" "${AIRFLOW_ADMIN_PASSWORD}"
printf '  %-11s http://localhost:%s/buckets/\n'   "Files"      "${PORT_S3_UI}"
printf '  %-11s http://localhost:%s/play\n'       "ClickHouse" "${PORT_CLICKHOUSE_HTTP}"
printf '  %-11s localhost:%s  (S3 API, no UI)\n'  "S3"         "${PORT_S3_API}"
printf '\n'
