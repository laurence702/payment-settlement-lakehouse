# syntax=docker/dockerfile:1
ARG AIRFLOW_BASE_IMAGE=apache/airflow:3.3.1
FROM ${AIRFLOW_BASE_IMAGE}

ARG AIRFLOW_VERSION=3.3.1

USER root

# PySpark 4.x requires a JRE 17+. headless keeps this to roughly 180 MB.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      openjdk-17-jre-headless \
      procps \
      curl \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# Resolve JAVA_HOME from the installed java rather than hardcoding an arch path,
# so this image builds on both Apple Silicon and x86 without edits.
RUN JH="$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" \
 && echo "JAVA_HOME=${JH}" > /etc/profile.d/java.sh \
 && ln -sfn "${JH}" /opt/java-home
ENV JAVA_HOME=/opt/java-home

USER airflow

# --- Airflow's own dependencies ---------------------------------------------
# The constraint URL is DERIVED from the base image's actual Python version.
# Hardcoding constraints-3.12.txt works right up until the base image moves to
# 3.13, at which point the build fails with an unresolvable numpy. Computing it
# means the pin can never drift from the interpreter it is constraining.
COPY requirements/airflow.txt /tmp/airflow.txt
RUN PYV="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')" \
 && CONSTRAINTS="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYV}.txt" \
 && echo "using constraints: ${CONSTRAINTS}" \
 && pip install --no-cache-dir --constraint "${CONSTRAINTS}" -r /tmp/airflow.txt

# --- dbt in an ISOLATED virtualenv ------------------------------------------
# dbt-core and Airflow disagree on jinja2, click and protobuf. Installing dbt
# into the Airflow environment is the most common way to end up with an image
# that imports fine but will not schedule. A separate venv is what Astronomer's
# Cosmos does, for exactly this reason. Tasks shell out to it.
COPY requirements/dbt.txt /tmp/dbt.txt
RUN python -m venv /home/airflow/dbt-venv \
 && /home/airflow/dbt-venv/bin/pip install --no-cache-dir --upgrade pip \
 && /home/airflow/dbt-venv/bin/pip install --no-cache-dir -r /tmp/dbt.txt \
 && /home/airflow/dbt-venv/bin/dbt --version

# Pre-download the DuckDB extensions into the image's extension cache.
# DuckDB fetches httpfs from extensions.duckdb.org on first connect. Without
# this, every DAG run depends on outbound internet and fails with an opaque
# HTTP 403 behind a proxy. Fetch once, at build time.
RUN /home/airflow/dbt-venv/bin/python -c "import duckdb; c=duckdb.connect(); [c.execute(q) for q in ('INSTALL httpfs','INSTALL parquet','LOAD httpfs','LOAD parquet')]; print('duckdb extensions cached', duckdb.__version__)"

# Fail the BUILD, not the first DAG run, if the two environments got tangled.
RUN python -c "import airflow, pyspark, confluent_kafka, clickhouse_connect; print('airflow env ok')" \
 && /home/airflow/dbt-venv/bin/python -c "import duckdb; print('dbt env ok', duckdb.__version__)" \
 && ! python -c "import dbt" 2>/dev/null && echo "confirmed: dbt is NOT in the airflow env"

ENV DBT_VENV=/home/airflow/dbt-venv \
    PYTHONPATH=/opt/airflow/src \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=60"
