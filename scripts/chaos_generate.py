#!/usr/bin/env python3
"""Kill the generator mid-publish and prove the retry adds no duplicates.

The pipeline code is not modified for this. A run is triggered with a seed no
other run uses. Once the publish ledger shows --kill-after-chunks committed
chunks, the generate_events task process is killed with SIGKILL: no abort, no
cleanup, the chunk it was sending is left in an open Kafka transaction. Airflow
retries the task; the retry fences the dead producer, skips the ledgered chunks
and publishes the rest.

It passes when all of these hold:
  * the run ends in success and generate_events needed more than one try,
  * read_committed consumers see every expected event_id exactly as often as
    the generator emits it, in both topics,
  * the raw Parquet layer holds no event_id more often than expected,
  * the ledger lists every chunk once.

The read_uncommitted count is reported too: when it exceeds the committed
count, the kill landed inside a transaction and the aborted events are visible
only to a consumer that ignores transactions.

    python scripts/chaos_generate.py --events 200000 --chunk-size 25000

Needs a running stack (make up) with the DAG unpaused. The report is printed and
written to chaos-report/ (gitignored).
"""

from __future__ import annotations

import argparse
import json
import random
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from e2e.airflow import AirflowClient  # noqa: E402
from e2e.stack import Stack, read_env  # noqa: E402

TASK = "generate_events"
SCHEDULER = "np_airflow_scheduler"
DAYS = 14

# Kills the task process for one task instance. Under the LocalExecutor the
# supervisor is titled "airflow worker -- LocalExecutor: <ti id>" and the task
# process it forks "airflow worker -- <ti id>". Only the task process is killed,
# so the supervisor records the failure and Airflow schedules the retry.
_KILL = r"""
import os, signal, sys
target = "airflow worker -- " + sys.argv[1]
hits = []
for pid in os.listdir("/proc"):
    if not pid.isdigit():
        continue
    try:
        cmd = open(f"/proc/{pid}/cmdline", "rb").read().replace(b"\0", b" ").decode().strip()
    except OSError:
        continue
    if cmd == target:
        hits.append(int(pid))
for pid in hits:
    os.kill(pid, signal.SIGKILL)
print(len(hits), " ".join(map(str, hits)))
"""


def sh(*args: str, stdin: str | None = None) -> str:
    return subprocess.run(
        list(args), input=stdin, check=True, capture_output=True, text=True
    ).stdout.strip()


def ledger_rows(run_id: str) -> int:
    env = read_env()
    try:
        out = _ledger_query(env, run_id)
    except subprocess.CalledProcessError:
        return 0  # the table does not exist until generate_events runs migrations
    return int(out or 0)


def _ledger_query(env: dict, run_id: str) -> str:
    return sh(
        "docker",
        "exec",
        "np_postgres",
        "psql",
        "-U",
        env.get("POSTGRES_USER", "dataeng"),
        "-d",
        env.get("OPS_DB", "settlement_ops"),
        "-tAc",
        f"SELECT count(*) FROM pipeline.publish_ledger WHERE run_key LIKE '{run_id}|%'",
    )


def ledger_duplicates(run_id: str) -> int:
    # The primary key makes this zero by construction; it is checked anyway so
    # the report states it from the data, not from the schema.
    env = read_env()
    out = sh(
        "docker",
        "exec",
        "np_postgres",
        "psql",
        "-U",
        env.get("POSTGRES_USER", "dataeng"),
        "-d",
        env.get("OPS_DB", "settlement_ops"),
        "-tAc",
        "SELECT count(*) - count(DISTINCT chunk_start) FROM pipeline.publish_ledger "
        f"WHERE run_key LIKE '{run_id}|%'",
    )
    return int(out or 0)


def task_state(client: AirflowClient, run_id: str):
    return next((t for t in client.tasks(run_id) if t.task_id == TASK), None)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--events", type=int, default=200_000, help="transactions")
    parser.add_argument("--chunk-size", type=int, default=25_000)
    parser.add_argument("--kill-after-chunks", type=int, default=2)
    parser.add_argument(
        "--kill-delay-s",
        type=float,
        default=0.0,
        help="wait this long after the kill point, so the kill lands while the next "
        "chunk is being sent rather than generated",
    )
    parser.add_argument("--timeout-s", type=float, default=2400.0)
    args = parser.parse_args()
    n_chunks = -(-args.events // args.chunk_size)
    if not 0 < args.kill_after_chunks < n_chunks:
        parser.error("--kill-after-chunks must leave at least one chunk to publish")

    stack = Stack.from_env_file()
    client = AirflowClient(stack.airflow_url, stack.airflow_user, stack.airflow_password)
    if not client.is_healthy():
        print("Airflow is not answering; start the stack first", file=sys.stderr)
        return 2

    run_id = f"chaos_generate__{datetime.now(UTC):%Y%m%dT%H%M%S}"
    # A seed far from the default and from other runs, so no other run's chunks
    # (seeded seed + chunk_start) share event_ids with this one.
    seed = random.SystemRandom().randrange(10**9, 2 * 10**9)
    client.trigger(
        run_id,
        {
            "event_count": args.events,
            "chunk_size": args.chunk_size,
            "days": DAYS,
            "seed": seed,
            "consumer_group_suffix": run_id[-24:],
        },
    )
    print(f"triggered {run_id} (seed {seed}, {n_chunks} chunks)", flush=True)

    deadline = time.monotonic() + args.timeout_s
    started = time.monotonic()
    timeline: list[str] = []

    def note(msg: str) -> None:
        timeline.append(f"{time.monotonic() - started:.0f}s {msg}")
        print(timeline[-1], flush=True)

    while time.monotonic() < deadline:
        done = ledger_rows(run_id) if task_state(client, run_id) else 0
        if done >= args.kill_after_chunks:
            break
        if client.run_state(run_id) in ("success", "failed"):
            print("run finished before the kill point; use more events", file=sys.stderr)
            return 1
        time.sleep(0.5)
    else:
        print("ledger never reached the kill point", file=sys.stderr)
        return 1

    time.sleep(args.kill_delay_s)
    ti = client._request(
        "GET", f"/api/v2/dags/settlement_pipeline/dagRuns/{run_id}/taskInstances/{TASK}"
    )
    killed = sh("docker", "exec", "-i", SCHEDULER, "python", "-", ti["id"], stdin=_KILL)
    chunks_at_kill = ledger_rows(run_id)
    note(f"SIGKILL sent with {chunks_at_kill} chunks ledgered; pids killed: {killed}")
    if killed.startswith("0 "):
        print(f"no process for task instance {ti['id']}; nothing killed", file=sys.stderr)
        return 1

    last = None
    while time.monotonic() < deadline:
        t = task_state(client, run_id)
        seen = (t.state, t.try_number) if t else None
        if seen and seen != last:
            note(f"{TASK} {seen[0]} try {seen[1]}")
            last = seen
        if client.run_state(run_id) in ("success", "failed"):
            break
        time.sleep(3)

    final = client.run_state(run_id)
    note(f"run {final}")
    gen = task_state(client, run_id)
    counts = json.loads(
        sh(
            "docker",
            "exec",
            "-i",
            SCHEDULER,
            "python",
            "-",
            str(args.events),
            str(DAYS),
            str(seed),
            str(args.chunk_size),
            stdin=(ROOT / "scripts" / "chaos_generate_check.py").read_text(),
        )
    )
    ledgered = ledger_rows(run_id)
    dup_ledger = ledger_duplicates(run_id)

    ok = (
        final == "success"
        and gen is not None
        and gen.try_number > 1
        and ledgered == n_chunks
        and dup_ledger == 0
        and all(c["kafka_read_committed"]["exact"] for c in counts.values())
        and all(c["raw_parquet"]["extra"] == 0 for c in counts.values())
    )
    report = {
        "run_id": run_id,
        "transactions": args.events,
        "chunk_size": args.chunk_size,
        "chunks": n_chunks,
        "seed": seed,
        "chunks_ledgered_at_kill": chunks_at_kill,
        "kill_delay_s": args.kill_delay_s,
        "killed_inside_open_transaction": any(
            c["kafka_read_uncommitted_found"] > c["kafka_read_committed"]["found"]
            for c in counts.values()
        ),
        "run_state": final,
        "generate_tries": gen.try_number if gen else None,
        "ledger_rows": ledgered,
        "ledger_duplicate_chunks": dup_ledger,
        "counts": counts,
        "timeline": timeline,
        "no_duplicates_after_retry": ok,
    }
    out = ROOT / "chaos-report"
    out.mkdir(exist_ok=True)
    (out / f"{run_id}.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
