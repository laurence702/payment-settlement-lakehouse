# payment-settlement-lakehouse
#
# The platform (kafka, seaweedfs, clickhouse, postgres, redis) is defined in
# this repo's own docker-compose.yml, not pulled in from a sibling repo.
# One clone, one .env, no INFRA path to resolve.

SHELL := /bin/bash

COMPOSE := docker compose --env-file .env
# COMPOSE_PROFILES rather than --profile flags: flag placement relative to the
# subcommand changed across compose releases, and `docker compose --profile x
# build` is rejected outright by some of them. The env var works everywhere.
ALL_PROFILES := core,stream,warehouse,airflow,ops

.DEFAULT_GOAL := help
.PHONY: chaos-retry chaos-generate benchmark e2e e2e-setup profile help bootstrap verify-images preflight build up down stop ps mem logs demo verify \
        test test-fast test-spark test-dbt lint venv dag-trigger dag-logs \
        airflow-shell dbt-shell ch ops-up ops-down test-ops clean

help:  ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
	  | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'
	@echo
	@echo "  First run:  make bootstrap && make build && make demo"

bootstrap:  ## Create .env and write absolute paths / generated secrets into it
	@if [ ! -f .env ]; then cp .env.example .env; echo "created .env"; fi
	@if sed --version >/dev/null 2>&1; then SEDI=(-i); else SEDI=(-i ''); fi; \
	  here="$$(pwd)"; \
	  sed "$${SEDI[@]}" "s|^PROJECT_ROOT=.*|PROJECT_ROOT=\"$${here}\"|" .env; \
	  sed "$${SEDI[@]}" "s|^AIRFLOW_UID=.*|AIRFLOW_UID=$$(id -u)|" .env; \
	  echo "PROJECT_ROOT=\"$${here}\""; echo "AIRFLOW_UID=$$(id -u)"
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
	@set -a; . ./.env; set +a; [ -d "$$PROJECT_ROOT" ] \
	  && echo "  .env sources cleanly and PROJECT_ROOT resolves" \
	  || { echo "  ERROR: .env does not source cleanly"; exit 1; }
	@echo "bootstrap complete. Next: make preflight"

verify-images:  ## Check every pinned image tag resolves
	@./scripts/verify-images.sh

profile:  ## Apply a resource profile to .env: make profile PROFILE=small|medium|large (see config/profiles)
	@./scripts/apply-profile.sh "$(PROFILE)"

preflight:  ## Check this project's .env, then docker/memory/disk/ports/pins
	@./scripts/check-env.sh
	@echo
	@./scripts/preflight.sh

build:  ## Build the airflow image (installs pyspark, dbt venv, duckdb extensions)
	@COMPOSE_PROFILES=core,airflow,ops $(COMPOSE) build

up:  ## Start core + kafka + clickhouse + airflow. ~5.5 GB. See the budget in help.
	@$(MAKE) --no-print-directory preflight
	@COMPOSE_PROFILES=$(ALL_PROFILES) $(COMPOSE) up -d
	@echo
	@./scripts/urls.sh
	@$(MAKE) --no-print-directory ps

demo:  ## Full end-to-end run: start everything, trigger the DAG, wait, report
	@./scripts/demo.sh

e2e-setup:  ## Install Playwright and its Chromium into .venv (one time, ~100 MB download)
	@.venv/bin/pip install -q -r requirements/e2e.txt
	@.venv/bin/playwright install chromium

e2e:  ## Run the pipeline end to end through Airflow's UI with Playwright (E2E_EVENTS=5000)
	@./scripts/e2e.sh

benchmark:  ## Repeat full runs and record timings and memory: make benchmark EVENTS="50000 500000" RUNS=3 PROFILE=small
	@.venv/bin/python scripts/benchmark.py --events $(or $(EVENTS),50000) --runs $(or $(RUNS),1) --profile $(or $(PROFILE),custom)

chaos-retry:  ## Stop ClickHouse mid-run and prove load_clickhouse retries with backoff and recovers
	@.venv/bin/python scripts/chaos_retry.py $(ARGS)

chaos-generate:  ## Kill generate_events mid-publish and prove the retry adds no duplicates (ADR 0009)
	@.venv/bin/python scripts/chaos_generate.py $(ARGS)

verify:  ## The done-check: full run + restart/OOM/row-count asserts. Exit 0 = pass. Evidence in verify-report/
	@./scripts/verify.sh

stop:  ## Stop everything, keep data
	@COMPOSE_PROFILES=$(ALL_PROFILES) $(COMPOSE) stop

down:  ## Remove containers, keep volumes
	@COMPOSE_PROFILES=$(ALL_PROFILES) $(COMPOSE) down --remove-orphans

ps:  ## What is running
	@docker ps --filter "name=np_" \
	  --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'

mem:  ## Memory use against the 6 GB budget
	@docker stats --no-stream --format 'table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}'
	@docker stats --no-stream --format '{{.MemUsage}}' | awk -F'/' \
	  '{gsub(/[A-Za-z ]/,"",$$1); if ($$1 ~ /^[0-9.]+$$/) s+=$$1} END {printf "\n  total in use: %.2f GiB of 6.00 GiB\n", s/1024}'

logs:  ## Tail airflow logs (S=service to narrow)
	@COMPOSE_PROFILES=airflow $(COMPOSE) logs -f --tail=100 $(S)

dag-trigger:  ## Trigger the pipeline DAG (pass ARGS to customize, e.g. ARGS='--conf "{\"event_count\": 500000}"')
	@docker exec np_airflow_scheduler airflow dags trigger settlement_pipeline $(ARGS)

dag-logs:  ## Follow the most recent DAG run
	@docker exec np_airflow_scheduler airflow dags list-runs -d settlement_pipeline | head -5

airflow-shell:  ## Shell inside the airflow scheduler
	@docker exec -it np_airflow_scheduler bash

dbt-shell:  ## dbt CLI inside the container, against the object store
	@docker exec -it -w /opt/airflow/dbt/settlement np_airflow_scheduler \
	  /home/airflow/dbt-venv/bin/dbt $(ARGS)

ch:  ## clickhouse-client shell
	@docker exec -it np_clickhouse clickhouse-client \
	  --user $$(grep '^CLICKHOUSE_USER=' .env | cut -d= -f2) \
	  --password $$(grep '^CLICKHOUSE_PASSWORD=' .env | cut -d= -f2)

# ---------------------------------------------------------------------------
# Tests. None of these need docker, kafka, seaweedfs or clickhouse.
# ---------------------------------------------------------------------------

venv:  ## Local venv for running the tests outside docker
	@python3 -m venv .venv
	@.venv/bin/pip install -q --upgrade pip
	@.venv/bin/pip install -q pytest ruff pyarrow "pyspark==4.2.0" \
	  "dbt-core==1.12.4" "dbt-duckdb==1.11.0" "duckdb==1.5.5" \
	  -r requirements/ops.txt "httpx==0.28.1"
	@echo "ready: source .venv/bin/activate"

test-fast:  ## Unit tests. No JVM, no dbt. Under a second.
	@.venv/bin/python -m pytest -m "not slow"

# Postgres on a throwaway port with its data on tmpfs, stopped when the tests
# finish (--rm removes the container on stop). The port stays clear of the
# stack's own Postgres on PORT_POSTGRES.
TEST_PG_PORT := 55432
test-ops:  ## Ops store tests against a real Postgres (throwaway container)
	@docker stop np_test_pg >/dev/null 2>&1 || true
	@docker run -d --rm --name np_test_pg -e POSTGRES_USER=dataeng \
	  -e POSTGRES_PASSWORD=dataeng_local_only -p 127.0.0.1:$(TEST_PG_PORT):5432 \
	  --tmpfs /var/lib/postgresql/data postgres:16.10-bookworm >/dev/null
	@for i in $$(seq 1 30); do \
	  docker exec np_test_pg pg_isready -U dataeng >/dev/null 2>&1 && break; sleep 1; done
	@POSTGRES_HOST=127.0.0.1 POSTGRES_PORT=$(TEST_PG_PORT) OPS_REQUIRE_POSTGRES=1 \
	  .venv/bin/python -m pytest tests/test_ops_store_contract.py tests/test_ops_app.py tests/test_publish_ledger.py; \
	  rc=$$?; docker stop np_test_pg >/dev/null 2>&1; exit $$rc

ops-up:  ## Start the analyst console (needs the core profile; see docs/adr/0007)
	@COMPOSE_PROFILES=core,ops $(COMPOSE) up -d
	@set -a; . ./.env; set +a; echo "console: http://127.0.0.1:$${PORT_OPS}"

ops-down:  ## Stop the analyst console
	@COMPOSE_PROFILES=ops $(COMPOSE) stop ops-console

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
	@rm -rf .pytest_cache .ruff_cache dbt/*/target dbt/*/logs \
	  dbt/*/dbt_packages *.duckdb *.duckdb.wal
	@find . -name __pycache__ -type d -prune -exec rm -rf {} +
