"""Analyst console: a triage queue, a case page and an action endpoint.

Server-rendered HTML for people, a small JSON API for automation. Both go
through the same `record_action`, so the rules cannot differ between them.

There is no authentication. The console binds to localhost and takes an analyst
name per request. That is a deliberate limit of a local portfolio system, listed
under Known limitations in the README.
"""

from __future__ import annotations

import re
import uuid
from pathlib import Path
from typing import Annotated, Literal

from fastapi import Depends, FastAPI, Form, Header, HTTPException, Query, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from settlement.ops.lifecycle import (
    Action,
    InvalidTransition,
    MissingNote,
    allowed_actions,
)
from settlement.ops.models import QueueFilter
from settlement.ops.money import format_ngn
from settlement.ops.service import (
    CaseNotFound,
    CaseStore,
    IdempotencyConflict,
    record_action,
)

TEMPLATES = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))
TEMPLATES.env.filters["ngn"] = format_ngn

PAGE_SIZE = 25
_CASE_ID = re.compile(r"^[0-9a-f]{64}$")
_KEY = re.compile(r"^[A-Za-z0-9._:-]{8,128}$")

View = Literal["active", "closed", "cleared", "all"]
BreakType = Literal[
    "missing_settlement",
    "partial_settlement",
    "short_paid",
    "overpaid",
    "reversed_after_settlement",
]


class ActionBody(BaseModel):
    action: Action
    notes: str = Field(default="", max_length=2000)


def _valid_case_id(case_id: str) -> str:
    if not _CASE_ID.fullmatch(case_id):
        raise HTTPException(status_code=404, detail="case not found")
    return case_id


def _valid_key(key: str) -> str:
    if not _KEY.fullmatch(key):
        raise HTTPException(
            status_code=422, detail="idempotency key must be 8 to 128 URL-safe characters"
        )
    return key


def _apply(store: CaseStore, case_id, action, analyst, notes, key):
    try:
        return record_action(
            store,
            case_id=case_id,
            action=action,
            analyst=analyst,
            notes=notes,
            idempotency_key=key,
        )
    except CaseNotFound:
        raise HTTPException(status_code=404, detail="case not found") from None
    except (InvalidTransition, IdempotencyConflict) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None
    except (MissingNote, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from None


def get_store(request: Request) -> CaseStore:
    return request.app.state.store


Store = Annotated[CaseStore, Depends(get_store)]


def create_app(store: CaseStore) -> FastAPI:
    app = FastAPI(title="Settlement ops console", docs_url="/api/docs", redoc_url=None)

    app.state.store = store

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz")
    def readyz(s: Store) -> dict[str, str]:
        s.summary()
        return {"status": "ready"}

    # --- Pages -------------------------------------------------------------

    @app.get("/", response_class=HTMLResponse)
    def queue(
        request: Request,
        s: Store,
        view: View = "active",
        break_type: BreakType | None = None,
        page: Annotated[int, Query(ge=1, le=10_000)] = 1,
    ) -> HTMLResponse:
        cases, total = s.list_cases(
            QueueFilter(
                view=view,
                break_type=break_type,
                limit=PAGE_SIZE,
                offset=(page - 1) * PAGE_SIZE,
            )
        )
        return TEMPLATES.TemplateResponse(
            request,
            "queue.html",
            {
                "cases": cases,
                "total": total,
                "summary": s.summary(),
                "view": view,
                "break_type": break_type,
                "page": page,
                "pages": max(1, -(-total // PAGE_SIZE)),
            },
        )

    def render_case(
        request: Request,
        s: CaseStore,
        case_id: str,
        *,
        status_code: int = 200,
        error: str | None = None,
        analyst: str = "",
        notes: str = "",
        selected: str = "",
    ) -> HTMLResponse:
        case = s.get_case(_valid_case_id(case_id))
        if case is None:
            raise HTTPException(status_code=404, detail="case not found")
        return TEMPLATES.TemplateResponse(
            request,
            "case.html",
            {
                "case": case,
                "actions": s.list_actions(case_id),
                # Declared order, so the safest action is the default.
                "allowed": [a.value for a in Action if a in allowed_actions(case.analyst_status)],
                # A fresh key per render: a double submit or a browser retry of
                # this form replays instead of recording the action twice.
                "idempotency_key": uuid.uuid4().hex,
                "error": error,
                "analyst": analyst,
                "notes": notes,
                "selected": selected,
            },
            status_code=status_code,
        )

    @app.get("/cases/{case_id}", response_class=HTMLResponse)
    def case_page(request: Request, case_id: str, s: Store) -> HTMLResponse:
        return render_case(request, s, case_id)

    @app.post("/cases/{case_id}/actions", response_model=None)
    def submit_action(
        request: Request,
        case_id: str,
        s: Store,
        action: Annotated[Action, Form()],
        analyst: Annotated[str, Form(min_length=1, max_length=64)],
        idempotency_key: Annotated[str, Form()],
        notes: Annotated[str, Form(max_length=2000)] = "",
    ) -> Response:
        try:
            _apply(s, _valid_case_id(case_id), action, analyst, notes, _valid_key(idempotency_key))
        except HTTPException as exc:
            if exc.status_code == 404:
                raise
            # Show the problem on the form the analyst is looking at, keeping
            # what they typed, instead of a bare JSON error page.
            return render_case(
                request,
                s,
                case_id,
                status_code=exc.status_code,
                error=str(exc.detail),
                analyst=analyst,
                notes=notes,
                selected=action.value,
            )
        # 303 so a browser refresh of the result page does not resubmit.
        return RedirectResponse(f"/cases/{case_id}", status_code=303)

    # --- JSON API ----------------------------------------------------------

    @app.get("/api/cases")
    def api_cases(
        s: Store,
        view: View = "active",
        break_type: BreakType | None = None,
        limit: Annotated[int, Query(ge=1, le=200)] = 50,
        offset: Annotated[int, Query(ge=0)] = 0,
    ) -> dict:
        items, total = s.list_cases(
            QueueFilter(view=view, break_type=break_type, limit=limit, offset=offset)
        )
        return {"total": total, "items": items}

    @app.get("/api/cases/{case_id}")
    def api_case(case_id: str, s: Store) -> dict:
        case = s.get_case(_valid_case_id(case_id))
        if case is None:
            raise HTTPException(status_code=404, detail="case not found")
        return {"case": case, "actions": s.list_actions(case_id)}

    @app.post("/api/cases/{case_id}/actions", status_code=201)
    def api_action(
        case_id: str,
        body: ActionBody,
        s: Store,
        response: Response,
        idempotency_key: Annotated[str, Header()],
        x_analyst: Annotated[str, Header(min_length=1, max_length=64)],
    ) -> dict:
        result = _apply(
            s,
            _valid_case_id(case_id),
            body.action,
            x_analyst,
            body.notes,
            _valid_key(idempotency_key),
        )
        if result.replayed:
            response.status_code = 200
        return {"action": result.action, "replayed": result.replayed}

    return app
