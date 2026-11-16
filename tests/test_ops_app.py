from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from ops_memory_store import MemoryCaseStore
from test_ops_store_contract import brk

from settlement.ops.app import create_app


@pytest.fixture
def store() -> MemoryCaseStore:
    s = MemoryCaseStore()
    s.sync_breaks(
        iter(
            [
                brk(1, exposure=150_000),
                brk(2, break_type="overpaid", exposure=2_550),
                brk(3, break_type="missing_settlement", exposure=9_850_000),
            ]
        )
    )
    return s


@pytest.fixture
def client(store) -> TestClient:
    return TestClient(create_app(store), follow_redirects=False)


def headers(key: str, analyst: str = "ada") -> dict[str, str]:
    return {"Idempotency-Key": key, "X-Analyst": analyst}


def test_queue_page_lists_cases_largest_exposure_first(client):
    html = client.get("/").text
    table = html.split("<h2>Cases</h2>")[1]
    assert table.index("98,500.00") < table.index("1,500.00") < table.index("25.50")
    assert "Active cases" in html


def test_queue_page_rejects_unknown_filters(client):
    assert client.get("/?view=nonsense").status_code == 422
    assert client.get("/?break_type=nonsense").status_code == 422
    assert client.get("/?page=0").status_code == 422


def test_case_page_shows_only_allowed_actions(client):
    case = brk(1)
    html = client.get(f"/cases/{case.case_id}").text
    assert 'value="CLOSE"' in html
    assert 'value="REOPEN"' not in html
    assert "No actions recorded." in html


def test_unknown_or_malformed_case_ids_are_404(client):
    assert client.get("/cases/" + "0" * 64).status_code == 404
    assert client.get("/cases/not-a-case-id").status_code == 404
    assert client.get("/api/cases/' OR 1=1 --").status_code == 404


def test_form_post_records_the_action_and_redirects(client):
    case = brk(1)
    r = client.post(
        f"/cases/{case.case_id}/actions",
        data={
            "action": "CLOSE",
            "analyst": "ada",
            "notes": "bank confirmed",
            "idempotency_key": "form-key-0001",
        },
    )
    assert r.status_code == 303
    assert r.headers["location"] == f"/cases/{case.case_id}"
    page = client.get(f"/cases/{case.case_id}").text
    assert "bank confirmed" in page and "CLOSED" in page


def test_a_double_submitted_form_records_once(client, store):
    case = brk(1)
    data = {
        "action": "ESCALATE",
        "analyst": "ada",
        "notes": "needs tier 2",
        "idempotency_key": "form-key-0002",
    }
    assert client.post(f"/cases/{case.case_id}/actions", data=data).status_code == 303
    assert client.post(f"/cases/{case.case_id}/actions", data=data).status_code == 303
    assert len(store.list_actions(case.case_id)) == 1


def test_notes_are_escaped_in_the_page(client):
    case = brk(1)
    client.post(
        f"/cases/{case.case_id}/actions",
        data={
            "action": "CLOSE",
            "analyst": "ada",
            "notes": "<script>alert(1)</script>",
            "idempotency_key": "form-key-0003",
        },
    )
    page = client.get(f"/cases/{case.case_id}").text
    assert "<script>alert(1)</script>" not in page
    assert "&lt;script&gt;" in page


def test_api_creates_then_replays(client):
    case = brk(1)
    body = {"action": "CLOSE", "notes": "paid"}
    first = client.post(
        f"/api/cases/{case.case_id}/actions", json=body, headers=headers("api-key-0001")
    )
    again = client.post(
        f"/api/cases/{case.case_id}/actions", json=body, headers=headers("api-key-0001")
    )
    assert first.status_code == 201 and first.json()["replayed"] is False
    assert again.status_code == 200 and again.json()["replayed"] is True
    assert first.json()["action"]["action_id"] == again.json()["action"]["action_id"]


def test_api_invalid_transition_is_409(client):
    case = brk(1)
    r = client.post(
        f"/api/cases/{case.case_id}/actions",
        json={"action": "REOPEN", "notes": "no"},
        headers=headers("api-key-0002"),
    )
    assert r.status_code == 409


def test_api_reused_key_for_another_request_is_409(client):
    a, b = brk(1), brk(2)
    client.post(
        f"/api/cases/{a.case_id}/actions",
        json={"action": "START_REVIEW"},
        headers=headers("api-key-0003"),
    )
    r = client.post(
        f"/api/cases/{b.case_id}/actions",
        json={"action": "START_REVIEW"},
        headers=headers("api-key-0003"),
    )
    assert r.status_code == 409


def test_api_missing_note_is_422(client):
    case = brk(1)
    r = client.post(
        f"/api/cases/{case.case_id}/actions",
        json={"action": "CLOSE"},
        headers=headers("api-key-0004"),
    )
    assert r.status_code == 422


@pytest.mark.parametrize(
    ("hdrs", "body"),
    [
        ({"X-Analyst": "ada"}, {"action": "CLOSE", "notes": "x"}),  # no idempotency key
        ({"Idempotency-Key": "api-key-0005"}, {"action": "CLOSE", "notes": "x"}),  # no analyst
        ({"Idempotency-Key": "short", "X-Analyst": "ada"}, {"action": "CLOSE", "notes": "x"}),
        (
            {"Idempotency-Key": "api-key-0005", "X-Analyst": "ada"},
            {"action": "DELETE", "notes": "x"},
        ),
        (
            {"Idempotency-Key": "api key with spaces", "X-Analyst": "ada"},
            {"action": "CLOSE", "notes": "x"},
        ),
    ],
)
def test_api_rejects_malformed_requests(client, hdrs, body):
    case = brk(1)
    assert (
        client.post(f"/api/cases/{case.case_id}/actions", json=body, headers=hdrs).status_code
        == 422
    )


def test_api_lists_with_filters_and_a_bounded_page(client):
    r = client.get("/api/cases", params={"break_type": "overpaid"})
    assert r.json()["total"] == 1
    assert client.get("/api/cases", params={"limit": 1000}).status_code == 422


def test_a_decision_made_in_the_console_survives_a_pipeline_rerun(client, store):
    """The headline guarantee, through the public interface."""
    case = brk(1, exposure=150_000)
    client.post(
        f"/api/cases/{case.case_id}/actions",
        json={"action": "CLOSE", "notes": "resolved with bank"},
        headers=headers("api-key-0006"),
    )
    store.sync_breaks(iter([case, brk(2), brk(3)]))
    detail = client.get(f"/api/cases/{case.case_id}").json()
    assert detail["case"]["effective_status"] == "CLOSED"
    assert len(detail["actions"]) == 1


def test_health_endpoints(client):
    assert client.get("/healthz").json() == {"status": "ok"}
    assert client.get("/readyz").json() == {"status": "ready"}
