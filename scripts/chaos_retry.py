#!/usr/bin/env python3
"""Prove the retry policy works by taking ClickHouse away while the DAG needs it.

The pipeline code is not modified for this. A run is triggered, ClickHouse is
stopped as soon as dbt_build starts, and it is started again after --outage-s
seconds. load_clickhouse must fail, back off, retry, and succeed once the
warehouse is back, and the run must end in success with that task's try number
above 1. If the retry policy were absent the run would stop at the first failure.

    python scripts/chaos_retry.py --events 20000 --outage-s 150

Needs a running stack (make up) with the DAG unpaused. The report is printed and
written to chaos-report/ (gitignored).
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from e2e.airflow import AirflowClient  # noqa: E402
from e2e.stack import Stack  # noqa: E402

TARGET_TASK = "load_clickhouse"
TRIGGER_AFTER = "dbt_build"


def docker(*args: str) -> None:
    subprocess.run(["docker", *args], check=True, capture_output=True)


def task_state(client: AirflowClient, run_id: str, task_id: str):
    return next((t for t in client.tasks(run_id) if t.task_id == task_id), None)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--events", type=int, default=20_000)
    parser.add_argument("--outage-s", type=float, default=150.0)
    parser.add_argument("--timeout-s", type=float, default=1800.0)
    args = parser.parse_args()

    stack = Stack.from_env_file()
    client = AirflowClient(stack.airflow_url, stack.airflow_user, stack.airflow_password)
    if not client.is_healthy():
        print("Airflow is not answering; start the stack first", file=sys.stderr)
        return 2

    run_id = f"chaos_retry__{datetime.now(UTC):%Y%m%dT%H%M%S}"
    client.trigger(run_id, {"event_count": args.events, "consumer_group_suffix": run_id[-24:]})
    print(f"triggered {run_id}; waiting for {TRIGGER_AFTER} to start", flush=True)

    deadline = time.monotonic() + args.timeout_s
    while time.monotonic() < deadline:
        task = task_state(client, run_id, TRIGGER_AFTER)
        if task and task.state in ("running", "success"):
            break
        time.sleep(2)
    else:
        print("dbt_build never started", file=sys.stderr)
        return 1

    stopped_at = time.monotonic()
    docker("stop", "np_clickhouse")
    print("ClickHouse stopped", flush=True)

    events: list[str] = []
    restarted = False
    last = None
    while time.monotonic() < deadline:
        if not restarted and time.monotonic() - stopped_at >= args.outage_s:
            docker("start", "np_clickhouse")
            restarted = True
            events.append(f"{time.monotonic() - stopped_at:.0f}s ClickHouse started again")
            print(events[-1], flush=True)
        task = task_state(client, run_id, TARGET_TASK)
        seen = (task.state, task.try_number) if task else None
        if seen != last and seen:
            events.append(
                f"{time.monotonic() - stopped_at:.0f}s {TARGET_TASK} {seen[0]} try {seen[1]}"
            )
            print(events[-1], flush=True)
            last = seen
        if client.run_state(run_id) in ("success", "failed"):
            break
        time.sleep(3)

    if not restarted:
        docker("start", "np_clickhouse")
    final = client.run_state(run_id)
    target = task_state(client, run_id, TARGET_TASK)
    ok = final == "success" and target is not None and target.try_number > 1
    report = {
        "run_id": run_id,
        "outage_s": args.outage_s,
        "run_state": final,
        "target_task_tries": target.try_number if target else None,
        "timeline": events,
        "retry_policy_worked": ok,
    }
    out = ROOT / "chaos-report"
    out.mkdir(exist_ok=True)
    (out / f"{run_id}.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
