# naijapay-lakehouse
#
# The platform (kafka, seaweedfs, clickhouse, postgres) lives in ../data-engineering-shared-infra
# and is pulled in through the compose `include:`. INFRA points at it.

SHELL := /bin/bash

# INFRA must be assigned BEFORE COMPOSE. COMPOSE uses := (simple expansion),
# so $(INFRA) is resolved on the spot; declared the other way round it expands
# to an empty string and compose is handed `--env-file /.env`.
INFRA ?= ../data-engineering-shared-infra

# Platform env first, this project's second: compose reads them left to right
# and later wins, so local keys override platform ones. Naming ./.env is not
# optional, because passing --env-file at all disables the implicit load.
# Drop the platform file and ${POSTGRES_USER} in docker-compose.yml
# interpolates to empty, and Airflow reaches postgres as nobody.
COMPOSE := docker compose --env-file $(INFRA)/.env --env-file .env
# COMPOSE_PROFILES rather than --profile flags: flag placement relative to the
# subcommand changed across compose releases, and `docker compose --profile x
# build` is rejected outright by some of them. The env var works everywhere.
ALL_PROFILES := core,stream,warehouse,airflow

.DEFAULT_GOAL := help
.PHONY: help bootstrap verify-images preflight build up down stop ps mem logs demo \
        test test-fast test-spark test-dbt lint venv dag-trigger dag-logs \
        airflow-shell dbt-shell ch clean

help:  ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
	  | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'
	@echo
	@echo "  First run:  make bootstrap && make build && make demo"

bootstrap:  ## Create .env files and write absolute paths into them
	@if [ ! -f .env ]; then cp .env.example .env; echo "created .env"; fi
	@if sed --version >/dev/null 2>&1; then SEDI=(-i); else SEDI=(-i ''); fi; \
	  here="$$(pwd)"; infra="$$(cd $(INFRA) && pwd)"; \
	  sed "$${SEDI[@]}" "s|^PROJECT_ROOT=.*|PROJECT_ROOT=\"$${here}\"|" .env; \
	  sed "$${SEDI[@]}" "s|^INFRA_ROOT=.*|INFRA_ROOT=\"$${infra}\"|" .env; \
	  sed "$${SEDI[@]}" "s|^AIRFLOW_UID=.*|AIRFLOW_UID=$$(id -u)|" .env; \
	  echo "PROJECT_ROOT=\"$${here}\""; echo "INFRA_ROOT=\"$${infra}\""; echo "AIRFLOW_UID=$$(id -u)"
	@if grep -q '^AIRFLOW_FERNET_KEY=GENERATE_ME' .env 2>/dev/null; then \
	  k=$$(python3 -c 'import base64,os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())'); \
	  if sed --version >/dev/null 2>&1; then sed -i "s|^AIRFLOW_FERNET_KEY=.*|AIRFLOW_FERNET_KEY=$$k|" .env; \
	  else sed -i '' "s|^AIRFLOW_FERNET_KEY=.*|AIRFLOW_FERNET_KEY=$$k|" .env; fi; \
	  echo "generated AIRFLOW_FERNET_KEY"; fi
	@if grep -q '^AIRFLOW_JWT_SECRET=GENERATE_ME' .env 2>/dev/null; then \
	  k=$$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))'); \
	  if sed --version >/dev/null 2>&1; then sed -i "s|^AIRFLOW_JWT_SECRET=.*|AIRFLOW_JWT_SECRET=$$k|" .env; \
	  else sed -i '' "s|^AIRFLOW_JWT_SECRET=.*|AIRFLOW_JWT_SECRET=$$k|" .env; fi; \
	  echo "generated AIRFLOW_JWT_SECRET"; fi
	@set -a; . ./.env; set +a; [ -d "$$PROJECT_ROOT" ] && [ -d "$$INFRA_ROOT" ] \
	  && echo "  .env sources cleanly and both paths resolve" \
	  || { echo "  ERROR: .env does not source cleanly"; exit 1; }
	@$(MAKE) --no-print-directory -C $(INFRA) bootstrap

verify-images:  ## Check every pinned image tag resolves (delegates to the platform repo)
	@$(MAKE) --no-print-directory -C $(INFRA) verify-images

preflight:  ## Check this project's .env, then docker/memory/disk/ports/pins
	@./scripts/check-env.sh
	@echo
	@$(MAKE) --no-print-directory -C $(INFRA) preflight

build:  ## Build the airflow image (installs pyspark, dbt venv, duckdb extensions)
	@COMPOSE_PROFILES=core,airflow $(COMPOSE) build

up:  ## Start core + kafka + clickhouse + airflow. ~5.2 GB. See the budget in help.
	@$(MAKE) --no-print-directory -C $(INFRA) preflight
	@COMPOSE_PROFILES=$(ALL_PROFILES) $(COMPOSE) up -d
	@echo
	@./scripts/urls.sh
	@$(MAKE) --no-print-directory ps

demo:  ## Full end-to-end run: start everything, trigger the DAG, wait, report
	@./scripts/demo.sh

stop:  ## Stop everything, keep data
	@COMPOSE_PROFILES=$(ALL_PROFILES) $(COMPOSE) stop

down:  ## Remove containers, keep volumes
	@COMPOSE_PROFILES=$(ALL_PROFILES) $(COMPOSE) down --remove-orphans

ps:  ## What is running
	@docker ps --filter "name=np_" --filter "name=dp_" \
	  --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'

mem:  ## Memory use against the 6 GB budget
	@docker stats --no-stream --format 'table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}'
	@docker stats --no-stream --format '{{.MemUsage}}' | awk -F'/' \
	  '{gsub(/[A-Za-z ]/,"",$$1); if ($$1 ~ /^[0-9.]+$$/) s+=$$1} END {printf "\n  total in use: %.2f GiB of 6.00 GiB\n", s/1024}'

logs:  ## Tail airflow logs (S=service to narrow)
	@COMPOSE_PROFILES=airflow $(COMPOSE) logs -f --tail=100 $(S)

dag-trigger:  ## Trigger the pipeline DAG
	@docker exec np_airflow_scheduler airflow dags trigger naijapay_pipeline

dag-logs:  ## Follow the most recent DAG run
	@docker exec np_airflow_scheduler airflow dags list-runs -d naijapay_pipeline | head -5

airflow-shell:  ## Shell inside the airflow scheduler
	@docker exec -it np_airflow_scheduler bash

dbt-shell:  ## dbt CLI inside the container, against the object store
	@docker exec -it -w /opt/airflow/dbt/naijapay np_airflow_scheduler \
	  /home/airflow/dbt-venv/bin/dbt $(ARGS)

ch:  ## clickhouse-client
	@$(MAKE) --no-print-directory -C $(INFRA) ch

# ---------------------------------------------------------------------------
# Tests. None of these need docker, kafka, seaweedfs or clickhouse.
# ---------------------------------------------------------------------------

venv:  ## Local venv for running the tests outside docker
	@python3 -m venv .venv
	@.venv/bin/pip install -q --upgrade pip
	@.venv/bin/pip install -q pytest ruff pyarrow "pyspark==4.2.0" \
	  "dbt-core==1.12.4" "dbt-duckdb==1.11.0" "duckdb==1.5.5"
	@echo "ready: source .venv/bin/activate"

test-fast:  ## Unit tests. No JVM, no dbt. Under a second.
	@.venv/bin/python -m pytest -m "not slow"

test-spark:  ## Spark transform tests. Starts a JVM.
	@.venv/bin/python -m pytest -m slow tests/test_transform_spark.py

test-dbt:  ## Build and test every dbt model against local parquet. No object store.
	@DBT_BIN=$$(pwd)/.venv/bin/dbt .venv/bin/python -m pytest -m slow tests/test_dbt.py

test:  ## Everything
	@DBT_BIN=$$(pwd)/.venv/bin/dbt .venv/bin/python -m pytest

lint:  ## ruff
	@.venv/bin/ruff check src tests dags
	@.venv/bin/ruff format --check src tests dags

clean:  ## Remove local build and test artefacts
	@rm -rf .pytest_cache .ruff_cache dbt/naijapay/target dbt/naijapay/logs \
	  dbt/naijapay/dbt_packages *.duckdb *.duckdb.wal
	@find . -name __pycache__ -type d -prune -exec rm -rf {} +
