#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Tests for the UI routes."""

import json
import uuid
from collections.abc import AsyncGenerator
from typing import Any

import httpx
import pytest

from conftest import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeCohortRepository,
    FakeCohortVersionRepository,
    FakeEvaluationRepository,
    FakeReplayRepository,
    FakeSessionNodeRepository,
    FakeSessionRepository,
    FakeTagRepository,
    FakeTaskRepository,
    build_payload_store,
    override_idempotency,
)
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    authorize_with_task,
    get_evaluation_service,
    get_session_node_service,
    get_session_service,
    get_tag_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.evaluation_service import EvaluationService
from kitaru.server.application.services.session_node_service import (
    SessionNodeService,
)
from kitaru.server.application.services.session_service import SessionService
from kitaru.server.application.services.tag_service import TagService
from kitaru.server.domain.account import Account

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


@pytest.fixture
def tag_repository() -> FakeTagRepository:
    """Provide the fake tag repository backing the app."""
    return FakeTagRepository()


@pytest.fixture
def evaluation_repository() -> FakeEvaluationRepository:
    """Provide the fake evaluation repository backing the app."""
    return FakeEvaluationRepository()


@pytest.fixture
def session_repository(
    tag_repository: FakeTagRepository,
    evaluation_repository: FakeEvaluationRepository,
) -> FakeSessionRepository:
    """Provide the tag- and evaluation-aware fake session repository for the app."""
    return FakeSessionRepository(tags=tag_repository, evaluations=evaluation_repository)


@pytest.fixture
def node_repository() -> FakeSessionNodeRepository:
    """Provide the fake session node repository backing the app."""
    return FakeSessionNodeRepository()


@pytest.fixture
def cohort_repository() -> FakeCohortRepository:
    """Provide the fake cohort repository backing the app."""
    return FakeCohortRepository()


@pytest.fixture
def cohort_version_repository(
    cohort_repository: FakeCohortRepository,
    session_repository: FakeSessionRepository,
) -> FakeCohortVersionRepository:
    """Provide the fake cohort version repository backing the app.

    The repository is wired to the fake session repository.
    """
    return FakeCohortVersionRepository(
        cohorts=cohort_repository, sessions=session_repository
    )


@pytest.fixture
def task_repository() -> FakeTaskRepository:
    """Provide the fake task repository backing the app."""
    return FakeTaskRepository()


@pytest.fixture
def agent_repository() -> FakeAgentRepository:
    """Provide the fake agent repository backing the app."""
    return FakeAgentRepository()


@pytest.fixture
def agent_version_repository(
    agent_repository: FakeAgentRepository,
) -> FakeAgentVersionRepository:
    """Provide the fake agent version repository backing the app."""
    return FakeAgentVersionRepository(agent_repository)


@pytest.fixture
async def client(
    session_repository: FakeSessionRepository,
    node_repository: FakeSessionNodeRepository,
    tag_repository: FakeTagRepository,
    evaluation_repository: FakeEvaluationRepository,
    task_repository: FakeTaskRepository,
    agent_version_repository: FakeAgentVersionRepository,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with fake-backed session services."""
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )
    payload_store = build_payload_store().store
    session_service = SessionService(
        repository=session_repository,
        task_repository=task_repository,
        agent_version_repository=agent_version_repository,
        replay_repository=FakeReplayRepository(),
        payload_store=payload_store,
    )
    node_service = SessionNodeService(
        repository=node_repository,
        session_repository=session_repository,
        task_repository=task_repository,
        payload_store=payload_store,
    )
    tag_service = TagService(repository=tag_repository)
    evaluation_service = EvaluationService(
        repository=evaluation_repository, session_repository=session_repository
    )
    app.dependency_overrides[get_session_service] = lambda: session_service
    app.dependency_overrides[get_session_node_service] = lambda: node_service
    app.dependency_overrides[get_tag_service] = lambda: tag_service
    app.dependency_overrides[get_evaluation_service] = lambda: evaluation_service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    app.dependency_overrides[authorize_with_task] = lambda: AuthContext(account=ACCOUNT)
    override_idempotency(app, ACCOUNT)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


def _session_body(**overrides: object) -> dict[str, object]:
    body: dict[str, object] = {
        "agent_id": str(uuid.uuid4()),
        "origin": "recorded",
        "inputs": {"prompt": "hi"},
        "outputs": None,
        "metadata": {},
    }
    body.update(overrides)
    return body


async def test_list_sessions_with_evaluations(client: httpx.AsyncClient) -> None:
    """List sessions with each session's evaluations attached."""
    scored = (
        await client.post("/api/v1/sessions", json=_session_body(status="completed"))
    ).json()
    unscored = (await client.post("/api/v1/sessions", json=_session_body())).json()
    await client.post(
        f"/api/v1/sessions/{scored['id']}/evaluations",
        json={"evaluations": [{"name": "accuracy", "score": 0.9}]},
    )

    response = await client.get("/api/v1/ui/sessions")
    assert response.status_code == 200
    items = response.json()["items"]

    by_id = {item["session"]["id"]: item for item in items}
    assert by_id[scored["id"]]["session"] == scored
    assert [
        (evaluation["name"], evaluation["score"])
        for evaluation in by_id[scored["id"]]["evaluations"]
    ] == [("accuracy", 0.9)]
    assert by_id[unscored["id"]]["evaluations"] == []


async def test_list_sessions_with_evaluations_walks_pages(
    client: httpx.AsyncClient,
) -> None:
    """Walk every page of sessions with evaluations without duplicates or gaps."""
    sessions = [
        (
            await client.post(
                "/api/v1/sessions", json=_session_body(status="completed")
            )
        ).json()
        for _ in range(3)
    ]
    names = ["accuracy", "relevance", "coherence"]
    for session, name in zip(sessions, names, strict=True):
        await client.post(
            f"/api/v1/sessions/{session['id']}/evaluations",
            json={"evaluations": [{"name": name, "score": 0.5}]},
        )

    collected: dict[str, list[str]] = {}
    cursor = None
    while True:
        params: dict[str, Any] = {"size": 2}
        if cursor is not None:
            params["cursor"] = cursor
        response = await client.get("/api/v1/ui/sessions", params=params)
        assert response.status_code == 200
        page = response.json()
        for item in page["items"]:
            collected[item["session"]["id"]] = [
                evaluation["name"] for evaluation in item["evaluations"]
            ]
        cursor = page["next_cursor"]
        if cursor is None:
            break

    assert set(collected) == {session["id"] for session in sessions}
    for session, name in zip(sessions, names, strict=True):
        assert collected[session["id"]] == [name]


async def test_list_sessions_with_evaluations_applies_filter(
    client: httpx.AsyncClient,
) -> None:
    """Filter sessions with evaluations down to the matching session."""
    await client.post("/api/v1/sessions", json=_session_body(origin="recorded"))
    matching = (
        await client.post("/api/v1/sessions", json=_session_body(origin="imported"))
    ).json()

    filter_expression = {"field": "origin", "op": "eq", "value": "imported"}
    response = await client.get(
        "/api/v1/ui/sessions", params={"filter": json.dumps(filter_expression)}
    )
    assert response.status_code == 200
    items = response.json()["items"]
    assert len(items) == 1
    assert items[0]["session"]["id"] == matching["id"]


async def test_get_session_with_evaluations(client: httpx.AsyncClient) -> None:
    """Get a session with its evaluations attached."""
    created = (
        await client.post("/api/v1/sessions", json=_session_body(status="completed"))
    ).json()
    await client.post(
        f"/api/v1/sessions/{created['id']}/evaluations",
        json={
            "evaluations": [
                {"name": "accuracy", "score": 0.9},
                {"name": "verdict", "value": "good"},
            ]
        },
    )

    response = await client.get(f"/api/v1/ui/sessions/{created['id']}")
    assert response.status_code == 200
    body = response.json()
    assert body["session"] == {
        **created,
        "inputs": {"prompt": "hi"},
        "outputs": None,
    }
    assert {evaluation["name"] for evaluation in body["evaluations"]} == {
        "accuracy",
        "verdict",
    }


async def test_get_session_with_evaluations_not_found(
    client: httpx.AsyncClient,
) -> None:
    """Observe HTTP 404 for a missing session."""
    response = await client.get(f"/api/v1/ui/sessions/{uuid.uuid4()}")
    assert response.status_code == 404
