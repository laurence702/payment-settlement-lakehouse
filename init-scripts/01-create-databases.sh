#!/bin/bash
# Runs once, on first initialisation of the postgres data volume only.
# If you change this file later, you must `make nuke-postgres` for it to re-run.
set -euo pipefail

# airflow   - Airflow 3 metadata DB. Sharing this postgres instead of running a
#             second postgres container saves ~380 MB, which matters at 6 GB.
# metastore - reserved for a future Iceberg/Hive-style catalog.
# analytics - scratch space for ad-hoc SQL that should not live in a mart.
for db in airflow metastore analytics; do
  echo "creating database if absent: ${db}"
  if ! psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
        -tAc "SELECT 1 FROM pg_database WHERE datname = '${db}'" | grep -q 1; then
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
        -c "CREATE DATABASE ${db}"
  fi
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
      -c "GRANT ALL PRIVILEGES ON DATABASE ${db} TO ${POSTGRES_USER}"
done
echo "init-scripts complete"
