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
"""Tests for the session routes."""

import json
import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime

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
    create_agent,
    create_agent_task,
    create_agent_version,
    create_cohort,
    create_cohort_version,
    create_session,
    local_settings,
)
from kitaru.api_models.v1.session import SessionStatus
from kitaru.server.adapters.auth.auth_service import AuthService
from kitaru.server.adapters.auth.jwt import TaskSubject
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    authorize_with_task,
    get_auth_service,
    get_evaluation_service,
    get_session_node_service,
    get_session_service,
    get_tag_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext, GrantKind
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
    """Provide the fake session repository backing the app, tag- and
    evaluation-aware."""
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
    """Provide the fake cohort version repository wired to the session
    repository backing the app."""
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
    session_service = SessionService(
        repository=session_repository,
        task_repository=task_repository,
        agent_version_repository=agent_version_repository,
        replay_repository=FakeReplayRepository(),
    )
    node_service = SessionNodeService(
        repository=node_repository,
        session_repository=session_repository,
        task_repository=task_repository,
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


async def test_create_session(client: httpx.AsyncClient) -> None:
    """Create a session."""
    response = await client.post("/api/v1/sessions", json=_session_body())
    assert response.status_code == 201
    body = response.json()
    assert body["origin"] == "recorded"
    assert body["status"] == "in_progress"
    assert body["owner_id"] == str(ACCOUNT.id)


async def test_create_session_duplicate_external_id(
    client: httpx.AsyncClient,
) -> None:
    """Observe HTTP 409 for a duplicated imported_from and external id pair."""
    await client.post(
        "/api/v1/sessions",
        json=_session_body(
            origin="imported", imported_from="langsmith", external_id="run-1"
        ),
    )
    response = await client.post(
        "/api/v1/sessions",
        json=_session_body(
            origin="imported", imported_from="langsmith", external_id="run-1"
        ),
    )
    assert response.status_code == 409


async def test_get_session(client: httpx.AsyncClient) -> None:
    """Get a session by id."""
    created = (await client.post("/api/v1/sessions", json=_session_body())).json()
    response = await client.get(f"/api/v1/sessions/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created


async def test_get_session_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a missing session."""
    response = await client.get(f"/api/v1/sessions/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_list_sessions_filters_by_origin_and_status(
    client: httpx.AsyncClient,
) -> None:
    """Filter sessions by origin and status."""
    await client.post("/api/v1/sessions", json=_session_body(origin="recorded"))
    await client.post(
        "/api/v1/sessions",
        json=_session_body(origin="imported", status="completed"),
    )

    filter_expression = {"field": "origin", "op": "eq", "value": "imported"}
    response = await client.get(
        "/api/v1/sessions", params={"filter": json.dumps(filter_expression)}
    )
    assert response.status_code == 200
    items = response.json()["items"]
    assert len(items) == 1
    assert items[0]["origin"] == "imported"

    filter_expression = {"field": "status", "op": "eq", "value": "completed"}
    response = await client.get(
        "/api/v1/sessions", params={"filter": json.dumps(filter_expression)}
    )
    assert response.status_code == 200
    items = response.json()["items"]
    assert len(items) == 1
    assert items[0]["status"] == "completed"


async def test_create_pending_import_session(client: httpx.AsyncClient) -> None:
    """A placeholder response carries its pending-import status and external id."""
    response = await client.post(
        "/api/v1/sessions",
        json=_session_body(
            origin="replay", status="pending_import", external_id="run-1"
        ),
    )
    assert response.status_code == 201
    body = response.json()
    assert body["status"] == "pending_import"
    assert body["external_id"] == "run-1"


async def test_list_sessions_filters_by_pending_import_status(
    client: httpx.AsyncClient,
) -> None:
    """Filter sessions by the pending-import status."""
    await client.post(
        "/api/v1/sessions",
        json=_session_body(
            origin="replay", status="pending_import", external_id="run-1"
        ),
    )
    await client.post("/api/v1/sessions", json=_session_body())

    filter_expression = {"field": "status", "op": "eq", "value": "pending_import"}
    response = await client.get(
        "/api/v1/sessions", params={"filter": json.dumps(filter_expression)}
    )
    assert response.status_code == 200
    items = response.json()["items"]
    assert len(items) == 1
    assert items[0]["status"] == "pending_import"


async def test_list_sessions_filters_by_imported_from_and_external_id(
    client: httpx.AsyncClient,
) -> None:
    """Filter sessions by imported_from and external id together."""
    await client.post(
        "/api/v1/sessions",
        json=_session_body(
            origin="imported", imported_from="langsmith", external_id="run-1"
        ),
    )
    await client.post(
        "/api/v1/sessions",
        json=_session_body(
            origin="imported", imported_from="langsmith", external_id="run-2"
        ),
    )

    filter_expression = {
        "and": [
            {"field": "imported_from", "op": "eq", "value": "langsmith"},
            {"field": "external_id", "op": "eq", "value": "run-2"},
        ]
    }
    response = await client.get(
        "/api/v1/sessions", params={"filter": json.dumps(filter_expression)}
    )
    assert response.status_code == 200
    items = response.json()["items"]
    assert len(items) == 1
    assert items[0]["external_id"] == "run-2"


async def test_list_sessions_filters_by_tag(client: httpx.AsyncClient) -> None:
    """Filter sessions linked to a tag through tag_link."""
    tagged = (await client.post("/api/v1/sessions", json=_session_body())).json()
    await client.post("/api/v1/sessions", json=_session_body())

    tag = (await client.post("/api/v1/tags", json={"name": "smoke-test"})).json()
    await client.post(
        f"/api/v1/tags/{tag['id']}/links",
        json={"resource_type": "session", "resource_id": tagged["id"]},
    )

    filter_expression = {"field": "tag", "op": "eq", "value": "smoke-test"}
    response = await client.get(
        "/api/v1/sessions", params={"filter": json.dumps(filter_expression)}
    )
    assert response.status_code == 200
    items = response.json()["items"]
    assert [item["id"] for item in items] == [tagged["id"]]


async def test_list_sessions_filters_by_cohort_version_id(
    client: httpx.AsyncClient,
    cohort_repository: FakeCohortRepository,
    cohort_version_repository: FakeCohortVersionRepository,
) -> None:
    """Filter sessions by cohort version membership."""
    member = (await client.post("/api/v1/sessions", json=_session_body())).json()
    await client.post("/api/v1/sessions", json=_session_body())

    cohort = await create_cohort(cohort_repository, ACCOUNT.id, uuid.uuid4())
    version = await create_cohort_version(
        cohort_version_repository, ACCOUNT.id, cohort.id, [uuid.UUID(member["id"])]
    )

    response = await client.get(
        "/api/v1/sessions",
        params={
            "filter": json.dumps(
                {
                    "field": "cohort_version_id",
                    "op": "eq",
                    "value": str(version.id),
                }
            )
        },
    )
    assert response.status_code == 200
    items = response.json()["items"]
    assert [item["id"] for item in items] == [member["id"]]


async def test_list_sessions_filters_by_date_bounds(
    client: httpx.AsyncClient,
) -> None:
    """Filter sessions by started_at ordered comparisons."""
    await client.post(
        "/api/v1/sessions",
        json=_session_body(started_at="2026-01-01T00:00:00Z"),
    )
    await client.post(
        "/api/v1/sessions",
        json=_session_body(started_at="2026-06-01T00:00:00Z"),
    )

    filter_expression = {
        "field": "started_at",
        "op": "ge",
        "value": "2026-03-01T00:00:00Z",
    }
    response = await client.get(
        "/api/v1/sessions", params={"filter": json.dumps(filter_expression)}
    )
    assert response.status_code == 200
    items = response.json()["items"]
    assert len(items) == 1
    assert items[0]["started_at"].startswith("2026-06-01")

    filter_expression = {
        "field": "started_at",
        "op": "le",
        "value": "2026-03-01T00:00:00Z",
    }
    response = await client.get(
        "/api/v1/sessions", params={"filter": json.dumps(filter_expression)}
    )
    assert response.status_code == 200
    items = response.json()["items"]
    assert len(items) == 1
    assert items[0]["started_at"].startswith("2026-01-01")


async def test_list_sessions_filters_by_cost_bounds(
    client: httpx.AsyncClient,
) -> None:
    """Filter sessions by cost ordered comparisons, applied after node rollups."""
    cheap = (await client.post("/api/v1/sessions", json=_session_body())).json()
    pricey = (await client.post("/api/v1/sessions", json=_session_body())).json()
    await client.post(
        f"/api/v1/sessions/{cheap['id']}/nodes",
        json={
            "nodes": [
                {
                    "index": 0,
                    "node_type": "llm_call",
                    "name": "call",
                    "status": "completed",
                    "cost": "1.00",
                    "inputs": None,
                    "outputs": None,
                    "attributes": None,
                }
            ]
        },
    )
    await client.post(
        f"/api/v1/sessions/{pricey['id']}/nodes",
        json={
            "nodes": [
                {
                    "index": 0,
                    "node_type": "llm_call",
                    "name": "call",
                    "status": "completed",
                    "cost": "9.00",
                    "inputs": None,
                    "outputs": None,
                    "attributes": None,
                }
            ]
        },
    )

    filter_expression = {"field": "cost", "op": "ge", "value": "5.00"}
    response = await client.get(
        "/api/v1/sessions", params={"filter": json.dumps(filter_expression)}
    )
    assert response.status_code == 200
    items = response.json()["items"]
    assert [item["id"] for item in items] == [pricey["id"]]

    filter_expression = {"field": "cost", "op": "le", "value": "5.00"}
    response = await client.get(
        "/api/v1/sessions", params={"filter": json.dumps(filter_expression)}
    )
    assert response.status_code == 200
    items = response.json()["items"]
    assert [item["id"] for item in items] == [cheap["id"]]


async def test_list_sessions_filters_by_has_evaluation(
    client: httpx.AsyncClient,
) -> None:
    """Filter sessions by whether they have a stored evaluation."""
    scored = (await client.post("/api/v1/sessions", json=_session_body())).json()
    unscored = (await client.post("/api/v1/sessions", json=_session_body())).json()
    await client.post(
        f"/api/v1/sessions/{scored['id']}/evaluations",
        json={"evaluations": [{"name": "accuracy", "score": 0.9}]},
    )

    response = await client.get(
        "/api/v1/sessions",
        params={
            "filter": json.dumps({"field": "has_evaluation", "op": "eq", "value": True})
        },
    )
    assert response.status_code == 200
    assert [item["id"] for item in response.json()["items"]] == [scored["id"]]

    response = await client.get(
        "/api/v1/sessions",
        params={
            "filter": json.dumps(
                {"field": "has_evaluation", "op": "eq", "value": False}
            )
        },
    )
    assert response.status_code == 200
    assert [item["id"] for item in response.json()["items"]] == [unscored["id"]]


async def test_merge_session_evaluations(client: httpx.AsyncClient) -> None:
    """Merge manual evaluations into a session."""
    created = (await client.post("/api/v1/sessions", json=_session_body())).json()
    response = await client.post(
        f"/api/v1/sessions/{created['id']}/evaluations",
        json={
            "evaluations": [
                {"name": "accuracy", "score": 0.9},
                {"name": "verdict", "value": "good"},
            ]
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert [item["name"] for item in body] == ["accuracy", "verdict"]
    assert body[0]["score"] == 0.9
    assert body[0]["evaluator_version_id"] is None
    assert body[0]["evaluator_name"] is None
    assert body[1]["value"] == "good"


async def test_merge_session_evaluations_carries_passed(
    client: httpx.AsyncClient,
) -> None:
    """Merge the optional pass flag, leaving it null when omitted."""
    created = (await client.post("/api/v1/sessions", json=_session_body())).json()
    response = await client.post(
        f"/api/v1/sessions/{created['id']}/evaluations",
        json={
            "evaluations": [
                {"name": "accuracy", "score": 0.9, "passed": True},
                {"name": "verdict", "value": "bad", "passed": False},
                {"name": "latency", "score": 1.0},
            ]
        },
    )
    assert response.status_code == 200
    assert [item["passed"] for item in response.json()] == [True, False, None]


async def test_merge_session_evaluations_overwrites_matching_name(
    client: httpx.AsyncClient,
) -> None:
    """Resending a name overwrites its score, value, and data type."""
    created = (await client.post("/api/v1/sessions", json=_session_body())).json()
    first = (
        await client.post(
            f"/api/v1/sessions/{created['id']}/evaluations",
            json={"evaluations": [{"name": "accuracy", "score": 0.5}]},
        )
    ).json()
    second = (
        await client.post(
            f"/api/v1/sessions/{created['id']}/evaluations",
            json={"evaluations": [{"name": "accuracy", "value": "high"}]},
        )
    ).json()
    assert second[0]["id"] == first[0]["id"]
    assert second[0]["value"] == "high"
    assert second[0]["score"] is None


async def test_merge_session_evaluations_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a missing session."""
    response = await client.post(
        f"/api/v1/sessions/{uuid.uuid4()}/evaluations",
        json={"evaluations": [{"name": "accuracy", "score": 0.9}]},
    )
    assert response.status_code == 404


async def test_merge_session_evaluations_rejects_duplicate_name(
    client: httpx.AsyncClient,
) -> None:
    """Observe HTTP 422 when the request names the same evaluation twice."""
    created = (await client.post("/api/v1/sessions", json=_session_body())).json()
    response = await client.post(
        f"/api/v1/sessions/{created['id']}/evaluations",
        json={
            "evaluations": [
                {"name": "accuracy", "score": 0.9},
                {"name": "accuracy", "score": 0.1},
            ]
        },
    )
    assert response.status_code == 422


async def test_update_session_clears_outputs_with_explicit_null(
    client: httpx.AsyncClient,
) -> None:
    """Clear outputs with an explicit null passed alongside status."""
    created = (
        await client.post(
            "/api/v1/sessions", json=_session_body(outputs={"answer": 42})
        )
    ).json()
    response = await client.patch(
        f"/api/v1/sessions/{created['id']}",
        json={"status": "completed", "outputs": None},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["outputs"] is None
    assert body["status"] == "completed"


async def test_update_session_omitted_outputs_unchanged(
    client: httpx.AsyncClient,
) -> None:
    """Leave outputs unchanged when the update omits them."""
    created = (
        await client.post(
            "/api/v1/sessions", json=_session_body(outputs={"answer": 42})
        )
    ).json()
    response = await client.patch(
        f"/api/v1/sessions/{created['id']}", json={"name": "renamed"}
    )
    assert response.status_code == 200
    body = response.json()
    assert body["outputs"] == {"answer": 42}
    assert body["name"] == "renamed"


async def test_update_session_rejects_system_prompt(
    client: httpx.AsyncClient,
) -> None:
    """Reject system prompts on the session update API."""
    created = (await client.post("/api/v1/sessions", json=_session_body())).json()
    response = await client.patch(
        f"/api/v1/sessions/{created['id']}",
        json={"system_prompt": "Replacement policy."},
    )

    assert response.status_code == 422


async def test_update_session_status_cannot_be_cleared(
    client: httpx.AsyncClient,
) -> None:
    """Observe HTTP 422 when the update clears the status with an explicit null."""
    created = (await client.post("/api/v1/sessions", json=_session_body())).json()
    response = await client.patch(
        f"/api/v1/sessions/{created['id']}", json={"status": None}
    )
    assert response.status_code == 422


async def test_update_session_rejects_terminal_back_to_in_progress(
    client: httpx.AsyncClient,
) -> None:
    """Observe HTTP 409 when the update moves a terminal session back to
    in_progress."""
    created = (await client.post("/api/v1/sessions", json=_session_body())).json()
    await client.patch(f"/api/v1/sessions/{created['id']}", json={"status": "failed"})
    response = await client.patch(
        f"/api/v1/sessions/{created['id']}", json={"status": "in_progress"}
    )
    assert response.status_code == 409


async def test_update_session_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a missing session."""
    response = await client.patch(
        f"/api/v1/sessions/{uuid.uuid4()}", json={"name": "x"}
    )
    assert response.status_code == 404


async def test_delete_session(client: httpx.AsyncClient) -> None:
    """Delete a session."""
    created = (await client.post("/api/v1/sessions", json=_session_body())).json()
    response = await client.delete(f"/api/v1/sessions/{created['id']}")
    assert response.status_code == 204
    response = await client.get(f"/api/v1/sessions/{created['id']}")
    assert response.status_code == 404


async def test_delete_session_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a missing session."""
    response = await client.delete(f"/api/v1/sessions/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_create_session_conflicts_when_task_not_running(
    session_repository: FakeSessionRepository,
    node_repository: FakeSessionNodeRepository,
    evaluation_repository: FakeEvaluationRepository,
    task_repository: FakeTaskRepository,
    account: Account,
    auth_service: AuthService,
) -> None:
    """Observe HTTP 409 when the token's task is not running."""
    task = await create_agent_task(task_repository, uuid.uuid4())
    client = _build_task_scoped_app(
        session_repository,
        node_repository,
        task_repository,
        evaluation_repository,
        auth_service,
    )
    async with client:
        token = _task_token(auth_service, account, task_id=task.id)
        response = await client.post(
            "/api/v1/sessions",
            json=_session_body(),
            headers={"Authorization": f"Bearer {token}"},
        )
        assert response.status_code == 409


async def test_create_session_links_the_agent_task_result_session(
    session_repository: FakeSessionRepository,
    node_repository: FakeSessionNodeRepository,
    evaluation_repository: FakeEvaluationRepository,
    task_repository: FakeTaskRepository,
    agent_repository: FakeAgentRepository,
    agent_version_repository: FakeAgentVersionRepository,
    account: Account,
    auth_service: AuthService,
) -> None:
    """Creating a session for a running agent task links it as the result session."""
    agent = await create_agent(agent_repository, ACCOUNT.id)
    version = await create_agent_version(
        agent_version_repository, agent_id=agent.id, owner_id=ACCOUNT.id
    )
    task = await create_agent_task(
        task_repository, uuid.uuid4(), agent_version_id=version.id
    )
    task.claim(uuid.uuid4(), datetime.now(UTC))
    task.start(datetime.now(UTC))
    await task_repository.update(task)
    client = _build_task_scoped_app(
        session_repository,
        node_repository,
        task_repository,
        evaluation_repository,
        auth_service,
        agent_version_repository=agent_version_repository,
    )
    async with client:
        token = _task_token(auth_service, account, task_id=task.id)
        response = await client.post(
            "/api/v1/sessions",
            json=_session_body(agent_id=None),
            headers={"Authorization": f"Bearer {token}"},
        )
    assert response.status_code == 201
    body = response.json()

    stored_task = await task_repository.get(task.id)
    assert str(stored_task.result_session_id) == body["id"]
    assert body["agent_id"] == str(agent.id)
    assert body["agent_version_id"] == str(version.id)


async def test_list_sessions_filters_by_filter_query_param(
    client: httpx.AsyncClient,
) -> None:
    """Filter sessions with a nested and/or filter expression."""
    by_cost = (await client.post("/api/v1/sessions", json=_session_body())).json()
    ingest = await client.post(
        f"/api/v1/sessions/{by_cost['id']}/nodes",
        json={
            "nodes": [
                {
                    "index": 0,
                    "node_type": "llm_call",
                    "name": "call",
                    "status": "completed",
                    "cost": "2.00",
                    "inputs": None,
                    "outputs": None,
                    "attributes": None,
                }
            ]
        },
    )
    assert ingest.status_code == 200
    await client.patch(
        f"/api/v1/sessions/{by_cost['id']}", json={"status": "completed"}
    )

    by_name = (
        await client.post("/api/v1/sessions", json=_session_body(name="web-two"))
    ).json()
    await client.patch(
        f"/api/v1/sessions/{by_name['id']}", json={"status": "completed"}
    )

    non_match = (
        await client.post("/api/v1/sessions", json=_session_body(name="other"))
    ).json()
    await client.patch(
        f"/api/v1/sessions/{non_match['id']}", json={"status": "completed"}
    )

    # Matches the or branch by name, but excluded because status is not
    # completed, proving the outer and short-circuits the match.
    await client.post("/api/v1/sessions", json=_session_body(name="web-three"))

    filter_expression = {
        "and": [
            {"field": "status", "op": "eq", "value": "completed"},
            {
                "or": [
                    {"field": "cost", "op": "ge", "value": 1.5},
                    {"field": "name", "op": "startswith", "value": "web"},
                ]
            },
        ]
    }
    response = await client.get(
        "/api/v1/sessions", params={"filter": json.dumps(filter_expression)}
    )
    assert response.status_code == 200
    ids = {item["id"] for item in response.json()["items"]}
    assert ids == {by_cost["id"], by_name["id"]}


async def test_list_sessions_malformed_filter_json(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for a filter query param that fails to parse as JSON."""
    response = await client.get("/api/v1/sessions", params={"filter": "{not-json"})
    assert response.status_code == 422


async def test_list_sessions_filter_unknown_field(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for a filter naming a field outside the allowlist."""
    filter_expression = {"field": "bogus", "op": "eq", "value": "x"}
    response = await client.get(
        "/api/v1/sessions", params={"filter": json.dumps(filter_expression)}
    )
    assert response.status_code == 422


async def test_list_sessions_filter_unsupported_operator(
    client: httpx.AsyncClient,
) -> None:
    """Observe HTTP 422 for an operator not allowed on the field."""
    filter_expression = {"field": "status", "op": "startswith", "value": "completed"}
    response = await client.get(
        "/api/v1/sessions", params={"filter": json.dumps(filter_expression)}
    )
    assert response.status_code == 422


async def test_list_sessions_filter_eq_with_null_value(
    client: httpx.AsyncClient,
) -> None:
    """Observe HTTP 422 for eq with a null value."""
    filter_expression = {"field": "name", "op": "eq", "value": None}
    response = await client.get(
        "/api/v1/sessions", params={"filter": json.dumps(filter_expression)}
    )
    assert response.status_code == 422


async def test_list_sessions_filter_nested_too_deep(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for a filter nested deeper than 5 levels."""
    filter_expression: dict[str, object] = {
        "field": "status",
        "op": "eq",
        "value": "completed",
    }
    for _ in range(5):
        filter_expression = {"and": [filter_expression]}
    response = await client.get(
        "/api/v1/sessions", params={"filter": json.dumps(filter_expression)}
    )
    assert response.status_code == 422


async def test_list_sessions_rejects_worker_and_task_credentials(
    session_repository: FakeSessionRepository,
    account: Account,
    auth_service: AuthService,
) -> None:
    """Observe HTTP 403 for a worker or task credential on an account-only route."""
    app = create_app(local_settings())
    app.dependency_overrides[get_session_service] = lambda: SessionService(
        repository=session_repository,
        task_repository=FakeTaskRepository(),
        agent_version_repository=FakeAgentVersionRepository(FakeAgentRepository()),
        replay_repository=FakeReplayRepository(),
    )
    app.dependency_overrides[get_auth_service] = lambda: auth_service
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        worker_token = auth_service.issue_worker_token(
            worker_id=uuid.uuid4(), account_id=account.id
        ).token
        response = await client.get(
            "/api/v1/sessions", headers={"Authorization": f"Bearer {worker_token}"}
        )
        assert response.status_code == 403

        task_token = auth_service.issue_task_token(
            TaskSubject(
                task_id=uuid.uuid4(),
                attempt=1,
                worker_id=uuid.uuid4(),
                account_id=account.id,
                job_id=uuid.uuid4(),
            ),
            timeout_seconds=3600,
        ).token
        response = await client.get(
            "/api/v1/sessions", headers={"Authorization": f"Bearer {task_token}"}
        )
        assert response.status_code == 403


def _build_task_scoped_app(
    session_repository: FakeSessionRepository,
    node_repository: FakeSessionNodeRepository,
    task_repository: FakeTaskRepository,
    evaluation_repository: FakeEvaluationRepository,
    auth_service: AuthService,
    agent_version_repository: FakeAgentVersionRepository | None = None,
) -> httpx.AsyncClient:
    """Build an app authenticating real task tokens, unlike the default client fixture.

    Args:
        session_repository: Fake session repository backing the app.
        node_repository: Fake session node repository backing the app.
        task_repository: Fake task repository backing the app.
        evaluation_repository: Fake evaluation repository backing the app.
        auth_service: Authentication service backing the app.
        agent_version_repository: Fake agent version repository backing the
            app, None builds an empty one.

    Returns:
        HTTP client routed to the app.
    """
    app = create_app(local_settings())
    app.dependency_overrides[get_session_service] = lambda: SessionService(
        repository=session_repository,
        task_repository=task_repository,
        agent_version_repository=agent_version_repository
        if agent_version_repository is not None
        else FakeAgentVersionRepository(FakeAgentRepository()),
        replay_repository=FakeReplayRepository(),
    )
    app.dependency_overrides[get_session_node_service] = lambda: SessionNodeService(
        repository=node_repository,
        session_repository=session_repository,
        task_repository=task_repository,
    )
    app.dependency_overrides[get_evaluation_service] = lambda: EvaluationService(
        repository=evaluation_repository, session_repository=session_repository
    )
    app.dependency_overrides[get_auth_service] = lambda: auth_service
    transport = httpx.ASGITransport(app=app)
    return httpx.AsyncClient(transport=transport, base_url="http://test")


def _task_token(
    auth_service: AuthService,
    account: Account,
    granted_session_id: uuid.UUID | None = None,
    task_id: uuid.UUID | None = None,
) -> str:
    """Mint a task token scoped to the given account for the task route tests."""
    grants: dict[GrantKind, frozenset[uuid.UUID]] = {}
    if granted_session_id is not None:
        grants[GrantKind.SESSION] = frozenset({granted_session_id})
    return auth_service.issue_task_token(
        TaskSubject(
            task_id=task_id if task_id is not None else uuid.uuid4(),
            attempt=1,
            worker_id=uuid.uuid4(),
            account_id=account.id,
            job_id=uuid.uuid4(),
            grants=grants,
        ),
        timeout_seconds=3600,
    ).token


async def test_get_session_denies_a_task_token_for_another_tasks_session(
    session_repository: FakeSessionRepository,
    node_repository: FakeSessionNodeRepository,
    evaluation_repository: FakeEvaluationRepository,
    task_repository: FakeTaskRepository,
    account: Account,
    auth_service: AuthService,
) -> None:
    """Observe HTTP 403 when a task token reads a session it does not own."""
    owner_task = await create_agent_task(task_repository, uuid.uuid4())
    session = await create_session(
        session_repository, uuid.uuid4(), agent_id=uuid.uuid4(), task_id=owner_task.id
    )
    client = _build_task_scoped_app(
        session_repository,
        node_repository,
        task_repository,
        evaluation_repository,
        auth_service,
    )
    async with client:
        token = _task_token(auth_service, account)
        response = await client.get(
            f"/api/v1/sessions/{session.id}",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert response.status_code == 403


async def test_update_session_denies_a_task_token_for_another_tasks_session(
    session_repository: FakeSessionRepository,
    node_repository: FakeSessionNodeRepository,
    evaluation_repository: FakeEvaluationRepository,
    task_repository: FakeTaskRepository,
    account: Account,
    auth_service: AuthService,
) -> None:
    """Observe HTTP 403 when a task token updates a session it does not own."""
    owner_task = await create_agent_task(task_repository, uuid.uuid4())
    session = await create_session(
        session_repository, uuid.uuid4(), agent_id=uuid.uuid4(), task_id=owner_task.id
    )
    client = _build_task_scoped_app(
        session_repository,
        node_repository,
        task_repository,
        evaluation_repository,
        auth_service,
    )
    async with client:
        token = _task_token(auth_service, account)
        response = await client.patch(
            f"/api/v1/sessions/{session.id}",
            json={"name": "renamed"},
            headers={"Authorization": f"Bearer {token}"},
        )
        assert response.status_code == 403


async def test_ingest_session_nodes_denies_a_task_token_for_another_tasks_session(
    session_repository: FakeSessionRepository,
    node_repository: FakeSessionNodeRepository,
    evaluation_repository: FakeEvaluationRepository,
    task_repository: FakeTaskRepository,
    account: Account,
    auth_service: AuthService,
) -> None:
    """Observe HTTP 403 for a task token ingesting nodes into an unowned session."""
    owner_task = await create_agent_task(task_repository, uuid.uuid4())
    session = await create_session(
        session_repository,
        uuid.uuid4(),
        agent_id=uuid.uuid4(),
        task_id=owner_task.id,
        status=SessionStatus.IN_PROGRESS,
    )
    client = _build_task_scoped_app(
        session_repository,
        node_repository,
        task_repository,
        evaluation_repository,
        auth_service,
    )
    async with client:
        token = _task_token(auth_service, account)
        response = await client.post(
            f"/api/v1/sessions/{session.id}/nodes",
            json={
                "nodes": [
                    {
                        "index": 0,
                        "node_type": "llm_call",
                        "name": "call",
                        "status": "completed",
                        "inputs": None,
                        "outputs": None,
                        "attributes": None,
                    }
                ]
            },
            headers={"Authorization": f"Bearer {token}"},
        )
        assert response.status_code == 403


async def test_get_session_allows_a_task_token_carrying_its_input_session_id(
    session_repository: FakeSessionRepository,
    node_repository: FakeSessionNodeRepository,
    evaluation_repository: FakeEvaluationRepository,
    task_repository: FakeTaskRepository,
    account: Account,
    auth_service: AuthService,
) -> None:
    """Allow a task token to read the session named in its input_session_id claim."""
    owner_task = await create_agent_task(task_repository, uuid.uuid4())
    session = await create_session(
        session_repository, uuid.uuid4(), agent_id=uuid.uuid4(), task_id=owner_task.id
    )
    client = _build_task_scoped_app(
        session_repository,
        node_repository,
        task_repository,
        evaluation_repository,
        auth_service,
    )
    async with client:
        token = _task_token(auth_service, account, granted_session_id=session.id)
        response = await client.get(
            f"/api/v1/sessions/{session.id}",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert response.status_code == 200
        assert response.json()["id"] == str(session.id)
