# syntax=docker/dockerfile:1
# The analyst console. Deliberately not built on the Airflow image: it needs
# none of Spark, the JVM or dbt, and a slim image is what keeps it inside its
# memory ceiling.
FROM python:3.12.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN useradd --system --uid 10001 --no-create-home ops

WORKDIR /app
COPY requirements/ops.txt /tmp/ops.txt
RUN pip install --no-cache-dir -r /tmp/ops.txt

# Only the package, not the whole repository.
COPY src/settlement /app/settlement
ENV PYTHONPATH=/app

USER ops
EXPOSE 8080
CMD ["python", "-m", "settlement.ops.serve_console"]
