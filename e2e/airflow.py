"""Minimal Airflow 3 REST client for the end-to-end test and the benchmark runner.

Standard library only, so the benchmark can run from any Python without the
project's virtualenv. Authentication is the API token exchange that the FAB auth
manager exposes at /auth/token; the token is fetched once and refreshed when the
server answers 401, because a 1M-event run outlives a default token lifetime.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from dataclasses import dataclass

DAG_ID = "settlement_pipeline"
TERMINAL_STATES = {"success", "failed"}


class AirflowError(RuntimeError):
    pass


@dataclass(frozen=True)
class TaskRecord:
    task_id: str
    state: str | None
    duration_s: float | None
    try_number: int


class AirflowClient:
    def __init__(self, base_url: str, username: str, password: str) -> None:
        self._base = base_url.rstrip("/")
        self._username = username
        self._password = password
        self._token: str | None = None

    def _request(self, method: str, path: str, body: dict | None = None, retry: bool = True):
        if self._token is None:
            self._token = self._fetch_token()
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(f"{self._base}{path}", data=data, method=method)
        req.add_header("Authorization", f"Bearer {self._token}")
        req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read() or b"null")
        except urllib.error.HTTPError as exc:
            if exc.code == 401 and retry:
                self._token = None
                return self._request(method, path, body, retry=False)
            detail = exc.read().decode(errors="replace")[:500]
            raise AirflowError(f"{method} {path} -> HTTP {exc.code}: {detail}") from exc

    def _fetch_token(self) -> str:
        payload = json.dumps({"username": self._username, "password": self._password}).encode()
        req = urllib.request.Request(
            f"{self._base}/auth/token",
            data=payload,
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read())["access_token"]
        except (urllib.error.URLError, KeyError) as exc:
            raise AirflowError(f"could not obtain an API token from {self._base}: {exc}") from exc

    def is_healthy(self) -> bool:
        try:
            with urllib.request.urlopen(f"{self._base}/api/v2/monitor/health", timeout=5) as resp:
                return resp.status == 200
        except (urllib.error.URLError, OSError):
            return False

    def unpause(self) -> None:
        self._request("PATCH", f"/api/v2/dags/{DAG_ID}", {"is_paused": False})

    def trigger(self, run_id: str, conf: dict) -> None:
        self._request(
            "POST",
            f"/api/v2/dags/{DAG_ID}/dagRuns",
            {"dag_run_id": run_id, "logical_date": None, "conf": conf},
        )

    def run_state(self, run_id: str) -> str:
        return self._request("GET", f"/api/v2/dags/{DAG_ID}/dagRuns/{run_id}")["state"]

    def tasks(self, run_id: str) -> list[TaskRecord]:
        body = self._request("GET", f"/api/v2/dags/{DAG_ID}/dagRuns/{run_id}/taskInstances")
        return [
            TaskRecord(t["task_id"], t["state"], t.get("duration"), t.get("try_number", 0))
            for t in body["task_instances"]
        ]

    def wait_for_run(self, run_id: str, timeout_s: float, poll_s: float = 10.0) -> str:
        """Block until the run is success or failed; raise on timeout."""
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            state = self.run_state(run_id)
            if state in TERMINAL_STATES:
                return state
            time.sleep(poll_s)
        raise AirflowError(f"run {run_id} still not finished after {timeout_s:.0f}s")
