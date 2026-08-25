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
"""Tests for the nested session node routes."""

import uuid
from collections.abc import AsyncGenerator
from typing import Any

import httpx
import pytest

from conftest import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeReplayRepository,
    FakeSessionNodeRepository,
    FakeSessionRepository,
    FakeTaskRepository,
    build_payload_offload_service,
    override_idempotency,
)
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    authorize_with_task,
    get_session_node_service,
    get_session_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.session_node_service import (
    SessionNodeService,
)
from kitaru.server.application.services.session_service import SessionService
from kitaru.server.domain.account import Account

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


@pytest.fixture
def session_repository() -> FakeSessionRepository:
    """Provide the fake session repository backing the app."""
    return FakeSessionRepository()


@pytest.fixture
def node_repository() -> FakeSessionNodeRepository:
    """Provide the fake session node repository backing the app."""
    return FakeSessionNodeRepository()


@pytest.fixture
async def client(
    session_repository: FakeSessionRepository,
    node_repository: FakeSessionNodeRepository,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with fake-backed session services."""
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )
    payload_offload = build_payload_offload_service()
    session_service = SessionService(
        repository=session_repository,
        task_repository=FakeTaskRepository(),
        agent_version_repository=FakeAgentVersionRepository(FakeAgentRepository()),
        replay_repository=FakeReplayRepository(),
        payload_offload=payload_offload,
    )
    node_service = SessionNodeService(
        repository=node_repository,
        session_repository=session_repository,
        task_repository=FakeTaskRepository(),
        payload_offload=payload_offload,
    )
    app.dependency_overrides[get_session_service] = lambda: session_service
    app.dependency_overrides[get_session_node_service] = lambda: node_service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    app.dependency_overrides[authorize_with_task] = lambda: AuthContext(account=ACCOUNT)
    override_idempotency(app, ACCOUNT)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.fixture
async def session_id(client: httpx.AsyncClient) -> str:
    """Provide the id of a recorded, in-progress session."""
    created = (
        await client.post(
            "/api/v1/sessions",
            json={
                "agent_id": str(uuid.uuid4()),
                "origin": "recorded",
                "inputs": None,
                "outputs": None,
                "metadata": {},
            },
        )
    ).json()
    return created["id"]


def _node(index: int, **overrides: object) -> dict[str, object]:
    node: dict[str, object] = {
        "index": index,
        "node_type": "llm_call",
        "name": "call",
        "status": "completed",
        "inputs": None,
        "outputs": None,
        "attributes": None,
        "metadata": {},
    }
    node.update(overrides)
    return node


async def test_ingest_nodes(client: httpx.AsyncClient, session_id: str) -> None:
    """Ingest a batch of nodes and observe the stored rows in batch order."""
    response = await client.post(
        f"/api/v1/sessions/{session_id}/nodes",
        json={
            "nodes": [
                _node(
                    0,
                    input_text_selector="/q",
                    output_text_selector="/answer",
                    system_prompt_selector="/system",
                    reasoning="The greeting matches the request.",
                    inputs={"q": "hi", "system": "Follow policy."},
                    outputs={"answer": "hello"},
                ),
                _node(
                    1,
                    parent_index=0,
                    node_type="tool_call",
                    tool_name="search",
                    inputs={"q": "hi"},
                ),
            ]
        },
    )
    assert response.status_code == 200
    items = response.json()
    assert len(items) == 2
    assert items[1]["parent_id"] == items[0]["id"]
    assert items[1]["cache_key"] is not None
    assert items[0]["input_text_selector"] == "/q"
    assert items[0]["output_text_selector"] == "/answer"
    assert items[0]["system_prompt_selector"] == "/system"
    assert items[0]["reasoning"] == "The greeting matches the request."
    # Ingest responses populate payloads even without include_payloads.
    assert items[0]["inputs"] == {"q": "hi", "system": "Follow policy."}


async def test_ingest_nodes_unresolved_parent_index(
    client: httpx.AsyncClient, session_id: str
) -> None:
    """Observe HTTP 422 when a parent_index does not resolve."""
    response = await client.post(
        f"/api/v1/sessions/{session_id}/nodes",
        json={"nodes": [_node(1, parent_index=0)]},
    )
    assert response.status_code == 422


async def test_ingest_nodes_session_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 when no session has this id."""
    response = await client.post(
        f"/api/v1/sessions/{uuid.uuid4()}/nodes",
        json={"nodes": [_node(0)]},
    )
    assert response.status_code == 404


async def test_ingest_nodes_terminal_recorded_session_rejected(
    client: httpx.AsyncClient, session_id: str
) -> None:
    """Observe HTTP 409 when the session does not accept node ingestion."""
    await client.patch(f"/api/v1/sessions/{session_id}", json={"status": "completed"})
    response = await client.post(
        f"/api/v1/sessions/{session_id}/nodes", json={"nodes": [_node(0)]}
    )
    assert response.status_code == 409


async def test_list_nodes_ordered_by_index(
    client: httpx.AsyncClient, session_id: str
) -> None:
    """List nodes ordered by index ascending."""
    await client.post(
        f"/api/v1/sessions/{session_id}/nodes",
        json={"nodes": [_node(2), _node(0), _node(1)]},
    )
    response = await client.get(f"/api/v1/sessions/{session_id}/nodes")
    assert response.status_code == 200
    items = response.json()["items"]
    assert [item["index"] for item in items] == [0, 1, 2]


async def test_list_nodes_include_payloads_default_false(
    client: httpx.AsyncClient, session_id: str
) -> None:
    """Null inputs, outputs, and attributes by default."""
    await client.post(
        f"/api/v1/sessions/{session_id}/nodes",
        json={
            "nodes": [
                _node(
                    0,
                    input_text_selector="/q",
                    output_text_selector="/answer",
                    system_prompt_selector="/system",
                    reasoning="The greeting matches the request.",
                    inputs={"q": "hi", "system": "Follow policy."},
                    outputs={"answer": "hello"},
                    attributes={"k": 1},
                )
            ]
        },
    )
    response = await client.get(f"/api/v1/sessions/{session_id}/nodes")
    assert response.status_code == 200
    item = response.json()["items"][0]
    assert item["inputs"] is None
    assert item["attributes"] is None
    assert item["input_text_selector"] == "/q"
    assert item["output_text_selector"] == "/answer"
    assert item["system_prompt_selector"] == "/system"
    assert item["reasoning"] is None


async def test_list_nodes_include_payloads_true(
    client: httpx.AsyncClient, session_id: str
) -> None:
    """Populate reasoning, inputs, outputs, and attributes when requested."""
    await client.post(
        f"/api/v1/sessions/{session_id}/nodes",
        json={
            "nodes": [
                _node(
                    0,
                    input_text_selector="/q",
                    output_text_selector="/answer",
                    system_prompt_selector="/system",
                    reasoning="Visible reasoning.",
                    inputs={"q": "hi", "system": "Follow policy."},
                    outputs={"answer": "hello"},
                    attributes={"k": 1},
                )
            ]
        },
    )
    response = await client.get(
        f"/api/v1/sessions/{session_id}/nodes", params={"include_payloads": "true"}
    )
    assert response.status_code == 200
    item = response.json()["items"][0]
    assert item["input_text_selector"] == "/q"
    assert item["output_text_selector"] == "/answer"
    assert item["system_prompt_selector"] == "/system"
    assert item["reasoning"] == "Visible reasoning."
    assert item["inputs"] == {"q": "hi", "system": "Follow policy."}
    assert item["attributes"] == {"k": 1}


async def test_list_nodes_pagination_walks_pages(
    client: httpx.AsyncClient, session_id: str
) -> None:
    """Walk every page of nodes via next_cursor without duplicates or gaps."""
    await client.post(
        f"/api/v1/sessions/{session_id}/nodes",
        json={"nodes": [_node(index) for index in range(5)]},
    )

    collected: list[int] = []
    cursor = None
    while True:
        params: dict[str, Any] = {"size": 2}
        if cursor is not None:
            params["cursor"] = cursor
        response = await client.get(
            f"/api/v1/sessions/{session_id}/nodes", params=params
        )
        assert response.status_code == 200
        page = response.json()
        collected.extend(item["index"] for item in page["items"])
        cursor = page["next_cursor"]
        if cursor is None:
            break

    assert collected == [0, 1, 2, 3, 4]


async def test_get_session_with_nodes_returns_every_node_unpaginated(
    client: httpx.AsyncClient, session_id: str
) -> None:
    """Carry a whole session in one call, past the default page size."""
    await client.post(
        f"/api/v1/sessions/{session_id}/nodes",
        json={"nodes": [_node(index) for index in range(45)]},
    )

    response = await client.get(f"/api/v1/sessions/{session_id}/full")

    assert response.status_code == 200
    body = response.json()
    assert body["session"]["id"] == session_id
    assert [node["index"] for node in body["nodes"]] == list(range(45))


async def test_get_session_with_nodes_populates_payloads(
    client: httpx.AsyncClient, session_id: str
) -> None:
    """Populate inputs, outputs, and attributes without asking for them."""
    await client.post(
        f"/api/v1/sessions/{session_id}/nodes",
        json={"nodes": [_node(0, inputs={"q": "hi"}, attributes={"k": 1})]},
    )

    response = await client.get(f"/api/v1/sessions/{session_id}/full")

    assert response.status_code == 200
    node = response.json()["nodes"][0]
    assert node["inputs"] == {"q": "hi"}
    assert node["attributes"] == {"k": 1}


async def test_get_session_with_nodes_resolves_parent_indexes(
    client: httpx.AsyncClient, session_id: str
) -> None:
    """Resolve parent ids to their indexes across the whole session."""
    await client.post(
        f"/api/v1/sessions/{session_id}/nodes",
        json={"nodes": [_node(0), _node(1, parent_index=0)]},
    )

    response = await client.get(f"/api/v1/sessions/{session_id}/full")

    assert response.status_code == 200
    nodes = response.json()["nodes"]
    assert nodes[0]["parent_index"] is None
    assert nodes[1]["parent_index"] == 0


async def test_get_session_with_nodes_session_not_found(
    client: httpx.AsyncClient,
) -> None:
    """Report 404 for a session that does not exist."""
    response = await client.get(f"/api/v1/sessions/{uuid.uuid4()}/full")

    assert response.status_code == 404
