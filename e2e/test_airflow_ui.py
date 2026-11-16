"""Run the whole pipeline end to end through Airflow's web UI.

A browser logs in, opens the DAG, fills the trigger dialog the way an operator
would, and submits it. The run is then followed through the REST API (the UI is
the thing under test, but polling a status page for minutes is not), and the
test asserts every task succeeded exactly once and that the analyst console
received the breaks the run produced.

Needs a running stack. `make e2e` resets volumes, starts one and runs this.
Screenshots land in e2e-report/ (gitignored).
"""

from __future__ import annotations

import json
import os
import time
import urllib.request

import pytest
from playwright.sync_api import Page, sync_playwright

from e2e.airflow import DAG_ID, AirflowClient
from e2e.stack import ROOT, Stack

pytestmark = pytest.mark.e2e

REPORT = ROOT / "e2e-report"
EVENTS = int(os.getenv("E2E_EVENTS", "5000"))
RUN_TIMEOUT_S = float(os.getenv("E2E_TIMEOUT_S", "1500"))
EXPECTED_TASKS = {
    "preflight",
    "generate_events",
    "ingest_to_raw",
    "spark_raw_to_staged",
    "dbt_build",
    "load_clickhouse",
    "serving_quality_checks",
    "sync_ops_cases",
}


@pytest.fixture(scope="module")
def stack() -> Stack:
    return Stack.from_env_file()


@pytest.fixture(scope="module")
def airflow(stack: Stack) -> AirflowClient:
    client = AirflowClient(stack.airflow_url, stack.airflow_user, stack.airflow_password)
    if not client.is_healthy():
        pytest.skip(f"Airflow is not answering at {stack.airflow_url}; run `make e2e`")
    return client


@pytest.fixture()
def page():
    REPORT.mkdir(exist_ok=True)
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        context = browser.new_context(viewport={"width": 1400, "height": 1000})
        yield context.new_page()
        browser.close()


def _login(page: Page, stack: Stack) -> None:
    page.goto(f"{stack.airflow_url}/")
    page.fill("input[name=username]", stack.airflow_user)
    page.fill("input[name=password]", stack.airflow_password)
    page.click("button[type=submit], input[type=submit]")
    page.wait_for_url(lambda url: "/auth/" not in url)


def _trigger_from_ui(page: Page, stack: Stack, run_id: str, suffix: str) -> None:
    page.goto(f"{stack.airflow_url}/dags/{DAG_ID}")
    page.get_by_role("button", name="Trigger").first.click()
    dialog = page.get_by_role("dialog")
    dialog.locator("[name=element_event_count]").fill(str(EVENTS))
    dialog.locator("[name=element_consumer_group_suffix]").fill(suffix)
    dialog.get_by_role("button", name="Advanced Options").click()
    dialog.locator("[name=dagRunId]").fill(run_id)
    dialog.get_by_role("button", name="Trigger").last.click()
    page.wait_for_url(f"**/runs/{run_id}")


def _get_json(url: str):
    with urllib.request.urlopen(url, timeout=15) as resp:
        return json.loads(resp.read())


def test_pipeline_runs_end_to_end_from_the_airflow_ui(
    page: Page, stack: Stack, airflow: AirflowClient
) -> None:
    stamp = time.strftime("%Y%m%dT%H%M%S", time.gmtime())
    run_id = f"e2e_ui__{stamp}"

    _login(page, stack)
    _trigger_from_ui(page, stack, run_id, suffix=f"e2e-{stamp}")
    page.screenshot(path=str(REPORT / "01-triggered.png"))

    state = airflow.wait_for_run(run_id, timeout_s=RUN_TIMEOUT_S)

    page.goto(f"{stack.airflow_url}/dags/{DAG_ID}/runs/{run_id}")
    page.wait_for_load_state("networkidle")
    page.screenshot(path=str(REPORT / "02-finished.png"))

    tasks = {t.task_id: t for t in airflow.tasks(run_id)}
    summary = {t: (r.state, r.try_number) for t, r in sorted(tasks.items())}
    (REPORT / "tasks.json").write_text(json.dumps(summary, indent=2))

    assert state == "success", f"run ended as {state}: {summary}"
    assert set(tasks) == EXPECTED_TASKS
    assert all(t.state == "success" for t in tasks.values()), summary
    assert all(t.try_number == 1 for t in tasks.values()), f"a task needed a retry: {summary}"

    # The last task of the DAG feeds the console; if its queue is empty the
    # pipeline ran but the last mile did not.
    assert _get_json(f"{stack.ops_url}/readyz")
    queue = _get_json(f"{stack.ops_url}/api/cases?limit=1")
    assert queue["items"], "run succeeded but the ops console has no breaks to triage"
