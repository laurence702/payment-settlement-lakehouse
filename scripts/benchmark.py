#!/usr/bin/env python3
"""Run the full pipeline repeatedly at a given scale and record what it took.

Each run starts from empty volumes, triggers the Airflow DAG through the REST
API, follows it to a terminal state while sampling every container's memory,
then writes one JSON record. A run that fails is recorded as a failure with the
failing tasks and any OOM kills; it is never retried or dropped, because a
benchmark that only keeps its successes measures luck.

    python scripts/benchmark.py --events 50000 --runs 3
    python scripts/benchmark.py --events 500000 1000000 --runs 2 --profile large

Records and the summary go to benchmark-runs/<timestamp>/ (gitignored). Raw
records stay local; copy numbers into docs only from a summary this script
wrote, with its run id.
"""

from __future__ import annotations

import argparse
import json
import re
import statistics
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from e2e.airflow import AirflowClient, AirflowError  # noqa: E402
from e2e.stack import Stack, read_env  # noqa: E402

_UNITS = {"B": 1, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3, "kB": 1000, "MB": 1000**2}
_MEM = re.compile(r"^([\d.]+)\s*([A-Za-z]+)")


def _to_mib(text: str) -> float:
    match = _MEM.match(text.strip())
    if not match:
        return 0.0
    return float(match.group(1)) * _UNITS.get(match.group(2), 1) / 1024**2


def _sh(*cmd: str, timeout: int = 60) -> str:
    return subprocess.run(
        cmd, capture_output=True, text=True, timeout=timeout, check=False, cwd=ROOT
    ).stdout


@dataclass
class MemorySampler:
    """Polls `docker stats` and keeps each container's peak usage."""

    interval_s: float = 5.0
    peaks_mib: dict[str, float] = field(default_factory=dict)
    _stop: threading.Event = field(default_factory=threading.Event)
    _thread: threading.Thread | None = None

    def _loop(self) -> None:
        while not self._stop.is_set():
            out = _sh("docker", "stats", "--no-stream", "--format", "{{.Name}}|{{.MemUsage}}")
            for line in out.splitlines():
                name, _, usage = line.partition("|")
                if name.startswith("np_"):
                    used = _to_mib(usage.split("/")[0])
                    self.peaks_mib[name] = max(self.peaks_mib.get(name, 0.0), used)
            self._stop.wait(self.interval_s)

    def start(self) -> None:
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self) -> dict[str, float]:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=30)
        return {k: round(v, 1) for k, v in sorted(self.peaks_mib.items())}


def container_health() -> dict[str, dict]:
    """Restart count, OOM flag and exit code for every np_* container, from docker."""
    names = _sh("docker", "ps", "-a", "--filter", "name=np_", "--format", "{{.Names}}").split()
    health = {}
    for name in sorted(names):
        fmt = "{{.State.Status}} {{.RestartCount}} {{.State.OOMKilled}} {{.State.ExitCode}}"
        parts = _sh("docker", "inspect", "-f", fmt, name).split()
        if len(parts) == 4:
            health[name] = {
                "status": parts[0],
                "restarts": int(parts[1]),
                "oom_killed": parts[2] == "true",
                "exit_code": int(parts[3]),
            }
    return health


def mart_rows(env: dict[str, str]) -> int | None:
    out = _sh(
        "docker", "exec", "np_clickhouse", "clickhouse-client",
        "--user", env["CLICKHOUSE_USER"], "--password", env["CLICKHOUSE_PASSWORD"],
        "--query", f"SELECT count() FROM {env['CLICKHOUSE_DB']}.mart_settlement_reconciliation",
    )  # fmt: skip
    return int(out.strip()) if out.strip().isdigit() else None


def vm_info() -> dict:
    fmt = "{{.MemTotal}} {{.NCPU}}"
    mem, cpus = (_sh("docker", "info", "--format", fmt).split() + ["0", "0"])[:2]
    return {"memory_gib": round(int(mem) / 1024**3, 2), "cpus": int(cpus)}


def run_once(args, env: dict[str, str], stack: Stack, events: int, index: int, out: Path) -> dict:
    run_id = f"bench_{events}_{index}_{datetime.now(UTC):%Y%m%dT%H%M%S}"
    record: dict = {
        "run_id": run_id,
        "events": events,
        "profile": args.profile,
        "started_at": datetime.now(UTC).isoformat(),
        "vm": vm_info(),
        "limits": {k: v for k, v in env.items() if k.startswith(("MEM_", "SPARK_", "S3_GO"))},
    }

    if not args.no_reset:
        reset = subprocess.run(
            ["./scripts/stack-fresh.sh"], cwd=ROOT, capture_output=True, text=True
        )
        if reset.returncode != 0:
            record.update(outcome="stack_failed_to_start", detail=reset.stderr[-1500:])
            (out / f"{run_id}.json").write_text(json.dumps(record, indent=2))
            return record

    client = AirflowClient(stack.airflow_url, stack.airflow_user, stack.airflow_password)
    sampler = MemorySampler()
    sampler.start()
    began = time.monotonic()
    try:
        client.trigger(run_id, {"event_count": events, "consumer_group_suffix": run_id[-24:]})
        state = client.wait_for_run(run_id, timeout_s=args.timeout_s, poll_s=5)
    except AirflowError as exc:
        state = "timeout" if "still not finished" in str(exc) else "api_error"
        record["detail"] = str(exc)
    wall_s = time.monotonic() - began
    record["peak_memory_mib"] = sampler.stop()

    try:
        tasks = client.tasks(run_id)
    except AirflowError:
        tasks = []
    health = container_health()
    record.update(
        outcome=state,
        wall_seconds=round(wall_s, 1),
        tasks={t.task_id: {"state": t.state, "seconds": t.duration_s, "tries": t.try_number}
               for t in tasks},
        failed_tasks=[t.task_id for t in tasks if t.state not in ("success", None)],
        containers=health,
        oom_killed=sorted(n for n, h in health.items() if h["oom_killed"]),
        restarted=sorted(n for n, h in health.items() if h["restarts"] > 0),
        mart_rows=mart_rows(env) if state == "success" else None,
        finished_at=datetime.now(UTC).isoformat(),
    )  # fmt: skip
    (out / f"{run_id}.json").write_text(json.dumps(record, indent=2))
    return record


def summarise(records: list[dict], out: Path, args) -> str:
    lines = [
        f"# Benchmark summary ({datetime.now(UTC):%Y-%m-%d %H:%M UTC})",
        "",
        f"profile: `{args.profile}`  runs per size: {args.runs}  reset between runs: "
        f"{'no' if args.no_reset else 'yes'}",
    ]
    if records:
        vm = records[0]["vm"]
        lines.append(f"VM: {vm['memory_gib']} GiB memory, {vm['cpus']} CPUs")
    header = "| events | run id | outcome | wall s | failed tasks | OOM | restarts | mart rows |"
    lines += ["", header, "|---|---|---|---|---|---|---|---|"]
    for r in records:
        lines.append(
            f"| {r['events']:,} | {r['run_id']} | {r['outcome']} | {r.get('wall_seconds', '')} "
            f"| {', '.join(r.get('failed_tasks', [])) or '-'} "
            f"| {', '.join(r.get('oom_killed', [])) or '-'} "
            f"| {', '.join(r.get('restarted', [])) or '-'} | {r.get('mart_rows') or '-'} |"
        )
    for events in sorted({r["events"] for r in records}):
        group = [r for r in records if r["events"] == events]
        ok = [r for r in group if r["outcome"] == "success"]
        lines += ["", f"## {events:,} events: {len(ok)}/{len(group)} succeeded"]
        if not ok:
            continue
        walls = [r["wall_seconds"] for r in ok]
        lines.append(
            f"wall seconds: min {min(walls)}, median {statistics.median(walls)}, max {max(walls)}"
        )
        task_ids = list(ok[0]["tasks"])
        lines += ["", "| task | median s | max s |", "|---|---|---|"]
        for t in task_ids:
            secs = [r["tasks"][t]["seconds"] for r in ok if r["tasks"].get(t, {}).get("seconds")]
            if secs:
                lines.append(f"| {t} | {statistics.median(secs):.1f} | {max(secs):.1f} |")
        peaks: dict[str, float] = {}
        for r in ok:
            for name, mib in r["peak_memory_mib"].items():
                peaks[name] = max(peaks.get(name, 0.0), mib)
        lines += ["", "| container | peak MiB (max across runs) |", "|---|---|"]
        for name, mib in sorted(peaks.items(), key=lambda kv: -kv[1]):
            lines.append(f"| {name} | {mib:.0f} |")
    text = "\n".join(lines) + "\n"
    (out / "summary.md").write_text(text)
    return text


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--events", type=int, nargs="+", default=[50_000])
    parser.add_argument("--runs", type=int, default=1, help="runs per event count")
    parser.add_argument("--profile", default="custom", help="label recorded with each run")
    parser.add_argument("--timeout-s", type=float, default=4 * 3600)
    parser.add_argument("--no-reset", action="store_true", help="skip the volume reset")
    args = parser.parse_args()

    env = read_env()
    stack = Stack.from_env_file()
    out = ROOT / "benchmark-runs" / datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
    out.mkdir(parents=True)
    records: list[dict] = []
    for events in args.events:
        for index in range(1, args.runs + 1):
            print(f"== {events:,} events, run {index}/{args.runs}", flush=True)
            record = run_once(args, env, stack, events, index, out)
            records.append(record)
            print(f"   {record['outcome']} in {record.get('wall_seconds', '?')}s", flush=True)
            summarise(records, out, args)
    print(summarise(records, out, args))
    print(f"records: {out}")
    return 0 if all(r["outcome"] == "success" for r in records) else 1


if __name__ == "__main__":
    sys.exit(main())
