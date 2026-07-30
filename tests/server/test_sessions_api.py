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

import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime

import httpx
import pytest

from conftest import (
    FakeCohortRepository,
    FakeCohortVersionRepository,
    FakeEvaluationRepository,
    FakeSessionNodeRepository,
    FakeSessionRepository,
    FakeTagRepository,
    FakeTaskRepository,
    create_agent_task,
    create_cohort,
    create_cohort_version,
)
from kitaru.server.adapters.rest.dependencies import (
    authorize,
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
async def client(
    session_repository: FakeSessionRepository,
    node_repository: FakeSessionNodeRepository,
    tag_repository: FakeTagRepository,
    evaluation_repository: FakeEvaluationRepository,
    task_repository: FakeTaskRepository,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with fake-backed session services."""
    app = create_app(
        APISettings(DB_HOST="localhost", SECRET_ENCRYPTION_KEY="test-encryption-key")
    )
    session_service = SessionService(
        repository=session_repository, task_repository=task_repository
    )
    node_service = SessionNodeService(
        repository=node_repository, session_repository=session_repository
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
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


def _session_body(**overrides: object) -> dict[str, object]:
    body: dict[str, object] = {
        "agent_id": str(uuid.uuid4()),
        "origin": "recorded",
        "inputs": {"prompt": "hi"},
        "outputs": None,
        "expected": None,
        "metadata": {},
    }
    body.update(overrides)
    return body


async def test_create_session(client: httpx.AsyncClient) -> None:
    """Create a session."""
    response = await client.post("/v1/sessions", json=_session_body())
    assert response.status_code == 201
    body = response.json()
    assert body["origin"] == "recorded"
    assert body["status"] == "in_progress"
    assert body["owner_id"] == str(ACCOUNT.id)


async def test_create_session_duplicate_external_id(
    client: httpx.AsyncClient,
) -> None:
    """Observe HTTP 409 for a duplicated provider and external id pair."""
    await client.post(
        "/v1/sessions",
        json=_session_body(
            origin="imported", provider="langsmith", external_id="run-1"
        ),
    )
    response = await client.post(
        "/v1/sessions",
        json=_session_body(
            origin="imported", provider="langsmith", external_id="run-1"
        ),
    )
    assert response.status_code == 409


async def test_get_session(client: httpx.AsyncClient) -> None:
    """Get a session by id."""
    created = (await client.post("/v1/sessions", json=_session_body())).json()
    response = await client.get(f"/v1/sessions/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created


async def test_get_session_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a missing session."""
    response = await client.get(f"/v1/sessions/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_list_sessions_filters_by_origin_and_status(
    client: httpx.AsyncClient,
) -> None:
    """Filter sessions by origin and status."""
    await client.post("/v1/sessions", json=_session_body(origin="recorded"))
    await client.post(
        "/v1/sessions",
        json=_session_body(origin="imported", status="completed"),
    )

    response = await client.get("/v1/sessions", params={"origin": "imported"})
    assert response.status_code == 200
    items = response.json()["items"]
    assert len(items) == 1
    assert items[0]["origin"] == "imported"

    response = await client.get("/v1/sessions", params={"status": "completed"})
    assert response.status_code == 200
    items = response.json()["items"]
    assert len(items) == 1
    assert items[0]["status"] == "completed"


async def test_list_sessions_filters_by_provider_and_external_id(
    client: httpx.AsyncClient,
) -> None:
    """Filter sessions by provider and external id together."""
    await client.post(
        "/v1/sessions",
        json=_session_body(
            origin="imported", provider="langsmith", external_id="run-1"
        ),
    )
    await client.post(
        "/v1/sessions",
        json=_session_body(
            origin="imported", provider="langsmith", external_id="run-2"
        ),
    )

    response = await client.get(
        "/v1/sessions", params={"provider": "langsmith", "external_id": "run-2"}
    )
    assert response.status_code == 200
    items = response.json()["items"]
    assert len(items) == 1
    assert items[0]["external_id"] == "run-2"


async def test_list_sessions_filters_by_tag(client: httpx.AsyncClient) -> None:
    """Filter sessions linked to a tag through tag_link."""
    tagged = (await client.post("/v1/sessions", json=_session_body())).json()
    await client.post("/v1/sessions", json=_session_body())

    tag = (await client.post("/v1/tags", json={"name": "smoke-test"})).json()
    await client.post(
        f"/v1/tags/{tag['id']}/links",
        json={"resource_type": "session", "resource_id": tagged["id"]},
    )

    response = await client.get("/v1/sessions", params={"tag": "smoke-test"})
    assert response.status_code == 200
    items = response.json()["items"]
    assert [item["id"] for item in items] == [tagged["id"]]


async def test_list_sessions_filters_by_cohort_version_id(
    client: httpx.AsyncClient,
    cohort_repository: FakeCohortRepository,
    cohort_version_repository: FakeCohortVersionRepository,
) -> None:
    """Filter sessions by cohort version membership."""
    member = (await client.post("/v1/sessions", json=_session_body())).json()
    await client.post("/v1/sessions", json=_session_body())

    cohort = await create_cohort(cohort_repository, ACCOUNT.id, uuid.uuid4())
    version = await create_cohort_version(
        cohort_version_repository, ACCOUNT.id, cohort.id, [uuid.UUID(member["id"])]
    )

    response = await client.get(
        "/v1/sessions", params={"cohort_version_id": str(version.id)}
    )
    assert response.status_code == 200
    items = response.json()["items"]
    assert [item["id"] for item in items] == [member["id"]]


async def test_list_sessions_filters_by_date_bounds(
    client: httpx.AsyncClient,
) -> None:
    """Filter sessions by started_after/before and ended_after/before."""
    await client.post(
        "/v1/sessions",
        json=_session_body(started_at="2026-01-01T00:00:00Z"),
    )
    await client.post(
        "/v1/sessions",
        json=_session_body(started_at="2026-06-01T00:00:00Z"),
    )

    response = await client.get(
        "/v1/sessions", params={"started_after": "2026-03-01T00:00:00Z"}
    )
    assert response.status_code == 200
    items = response.json()["items"]
    assert len(items) == 1
    assert items[0]["started_at"].startswith("2026-06-01")

    response = await client.get(
        "/v1/sessions", params={"started_before": "2026-03-01T00:00:00Z"}
    )
    assert response.status_code == 200
    items = response.json()["items"]
    assert len(items) == 1
    assert items[0]["started_at"].startswith("2026-01-01")


async def test_list_sessions_filters_by_cost_bounds(
    client: httpx.AsyncClient,
) -> None:
    """Filter sessions by min_cost and max_cost, applied after node rollups."""
    cheap = (await client.post("/v1/sessions", json=_session_body())).json()
    pricey = (await client.post("/v1/sessions", json=_session_body())).json()
    await client.post(
        f"/v1/sessions/{cheap['id']}/nodes",
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
        f"/v1/sessions/{pricey['id']}/nodes",
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

    response = await client.get("/v1/sessions", params={"min_cost": "5.00"})
    assert response.status_code == 200
    items = response.json()["items"]
    assert [item["id"] for item in items] == [pricey["id"]]

    response = await client.get("/v1/sessions", params={"max_cost": "5.00"})
    assert response.status_code == 200
    items = response.json()["items"]
    assert [item["id"] for item in items] == [cheap["id"]]


async def test_list_sessions_filters_by_has_evaluation(
    client: httpx.AsyncClient,
) -> None:
    """Filter sessions by whether they have a stored evaluation."""
    scored = (await client.post("/v1/sessions", json=_session_body())).json()
    unscored = (await client.post("/v1/sessions", json=_session_body())).json()
    await client.post(
        f"/v1/sessions/{scored['id']}/evaluations",
        json={"evaluations": [{"name": "accuracy", "score": 0.9}]},
    )

    response = await client.get("/v1/sessions", params={"has_evaluation": "true"})
    assert response.status_code == 200
    assert [item["id"] for item in response.json()["items"]] == [scored["id"]]

    response = await client.get("/v1/sessions", params={"has_evaluation": "false"})
    assert response.status_code == 200
    assert [item["id"] for item in response.json()["items"]] == [unscored["id"]]


async def test_merge_session_evaluations(client: httpx.AsyncClient) -> None:
    """Merge manual evaluations into a session."""
    created = (await client.post("/v1/sessions", json=_session_body())).json()
    response = await client.post(
        f"/v1/sessions/{created['id']}/evaluations",
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
    created = (await client.post("/v1/sessions", json=_session_body())).json()
    response = await client.post(
        f"/v1/sessions/{created['id']}/evaluations",
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
    created = (await client.post("/v1/sessions", json=_session_body())).json()
    first = (
        await client.post(
            f"/v1/sessions/{created['id']}/evaluations",
            json={"evaluations": [{"name": "accuracy", "score": 0.5}]},
        )
    ).json()
    second = (
        await client.post(
            f"/v1/sessions/{created['id']}/evaluations",
            json={"evaluations": [{"name": "accuracy", "value": "high"}]},
        )
    ).json()
    assert second[0]["id"] == first[0]["id"]
    assert second[0]["value"] == "high"
    assert second[0]["score"] is None


async def test_merge_session_evaluations_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a missing session."""
    response = await client.post(
        f"/v1/sessions/{uuid.uuid4()}/evaluations",
        json={"evaluations": [{"name": "accuracy", "score": 0.9}]},
    )
    assert response.status_code == 404


async def test_merge_session_evaluations_rejects_duplicate_name(
    client: httpx.AsyncClient,
) -> None:
    """Observe HTTP 422 when the request names the same evaluation twice."""
    created = (await client.post("/v1/sessions", json=_session_body())).json()
    response = await client.post(
        f"/v1/sessions/{created['id']}/evaluations",
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
        await client.post("/v1/sessions", json=_session_body(outputs={"answer": 42}))
    ).json()
    response = await client.patch(
        f"/v1/sessions/{created['id']}",
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
        await client.post("/v1/sessions", json=_session_body(outputs={"answer": 42}))
    ).json()
    response = await client.patch(
        f"/v1/sessions/{created['id']}", json={"name": "renamed"}
    )
    assert response.status_code == 200
    body = response.json()
    assert body["outputs"] == {"answer": 42}
    assert body["name"] == "renamed"


async def test_update_session_status_cannot_be_cleared(
    client: httpx.AsyncClient,
) -> None:
    """Observe HTTP 422 when the update clears the status with an explicit null."""
    created = (await client.post("/v1/sessions", json=_session_body())).json()
    response = await client.patch(
        f"/v1/sessions/{created['id']}", json={"status": None}
    )
    assert response.status_code == 422


async def test_update_session_rejects_terminal_back_to_in_progress(
    client: httpx.AsyncClient,
) -> None:
    """Observe HTTP 409 when the update moves a terminal session back to
    in_progress."""
    created = (await client.post("/v1/sessions", json=_session_body())).json()
    await client.patch(f"/v1/sessions/{created['id']}", json={"status": "failed"})
    response = await client.patch(
        f"/v1/sessions/{created['id']}", json={"status": "in_progress"}
    )
    assert response.status_code == 409


async def test_update_session_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a missing session."""
    response = await client.patch(f"/v1/sessions/{uuid.uuid4()}", json={"name": "x"})
    assert response.status_code == 404


async def test_delete_session(client: httpx.AsyncClient) -> None:
    """Delete a session."""
    created = (await client.post("/v1/sessions", json=_session_body())).json()
    response = await client.delete(f"/v1/sessions/{created['id']}")
    assert response.status_code == 204
    response = await client.get(f"/v1/sessions/{created['id']}")
    assert response.status_code == 404


async def test_delete_session_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a missing session."""
    response = await client.delete(f"/v1/sessions/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_create_session_conflicts_when_task_not_running(
    client: httpx.AsyncClient, task_repository: FakeTaskRepository
) -> None:
    """Observe HTTP 409 when the named task is not running."""
    task = await create_agent_task(task_repository, uuid.uuid4())
    response = await client.post(
        "/v1/sessions", json=_session_body(task_id=str(task.id))
    )
    assert response.status_code == 409


async def test_create_session_links_the_agent_task_result_session(
    client: httpx.AsyncClient, task_repository: FakeTaskRepository
) -> None:
    """Creating a session for a running agent task links it as the result session."""
    task = await create_agent_task(task_repository, uuid.uuid4())
    task.claim(uuid.uuid4(), datetime.now(UTC))
    task.start(datetime.now(UTC))
    await task_repository.update(task)

    response = await client.post(
        "/v1/sessions", json=_session_body(task_id=str(task.id))
    )
    assert response.status_code == 201
    session_id = response.json()["id"]

    stored_task = await task_repository.get(task.id)
    assert str(stored_task.result_session_id) == session_id
