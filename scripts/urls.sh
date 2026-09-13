#!/usr/bin/env bash
# Every port is read from .env, never printed as a literal. A hardcoded URL
# here is the same bug as a hardcoded image tag in a compose file.
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
INFRA="${INFRA:-../data-engineering-shared-infra}"
set -a; . "$INFRA/.env"; . ./.env; set +a

printf '\n'
printf '  %-11s http://localhost:%s\n'            "Airflow"    "${PORT_AIRFLOW}"
printf '  %-11s %s / %s\n'                        ""           "${AIRFLOW_ADMIN_USER}" "${AIRFLOW_ADMIN_PASSWORD}"
printf '  %-11s http://localhost:%s/buckets/\n'   "Files"      "${PORT_S3_UI}"
printf '  %-11s http://localhost:%s/play\n'       "ClickHouse" "${PORT_CLICKHOUSE_HTTP}"
printf '  %-11s localhost:%s  (S3 API, no UI)\n'  "S3"         "${PORT_S3_API}"
printf '\n'
printf '  not running by default:\n'
printf '  %-11s http://localhost:%s  make -C %s up-ui\n'      "Kafka UI"   "${PORT_KAFKA_UI}"   "$INFRA"
printf '  %-11s http://localhost:%s  make -C %s up-observe\n' "Grafana"    "${PORT_GRAFANA}"    "$INFRA"
printf '  %-11s http://localhost:%s  make -C %s up-observe\n' "Prometheus" "${PORT_PROMETHEUS}" "$INFRA"
printf '\n'
