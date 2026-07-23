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
"""End-to-end session tests against PostgreSQL."""

import uuid
from collections.abc import AsyncGenerator
from decimal import Decimal

import httpx
import pytest

from conftest import db_settings, lifespan_client
from kitaru.hashing import tool_call_cache_key
from kitaru.server.domain.ids import uuid7


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app running its full lifespan."""
    # Under the none auth scheme every request runs as the account
    # bootstrapped at startup, which owns all created sessions.
    async with lifespan_client(db_settings()) as client:
        yield client


async def create_agent(client: httpx.AsyncClient, name: str = "support-bot") -> str:
    """Store an agent through the API.

    Args:
        client: HTTP client for the app.
        name: Agent name.

    Returns:
        Id of the created agent.
    """
    response = await client.post("/v1/agents", json={"name": name})
    assert response.status_code == 201
    return response.json()["id"]


async def create_session(client: httpx.AsyncClient, agent_id: str) -> str:
    """Store a recorded session through the API.

    Args:
        client: HTTP client for the app.
        agent_id: Id of the agent.

    Returns:
        Id of the created session.
    """
    response = await client.post(
        "/v1/sessions",
        json={
            "agent_id": agent_id,
            "origin": "recorded",
            "inputs": {"prompt": "hi"},
        },
    )
    assert response.status_code == 201
    return response.json()["id"]


async def test_record_flow_persists_across_requests(
    client: httpx.AsyncClient,
) -> None:
    """Record a session with node batches and finish it with rollups."""
    agent_id = await create_agent(client)
    session_id = await create_session(client, agent_id)

    root = {
        "id": str(uuid7()),
        "sequence": 0,
        "node_type": "span",
        "name": "run",
        "status": "completed",
    }
    chat = {
        "id": str(uuid7()),
        "sequence": 1,
        "parent_id": root["id"],
        "node_type": "llm_call",
        "name": "chat",
        "status": "completed",
        "tokens": {"input_tokens": 100, "output_tokens": 20},
        "cost": "0.5",
        "inputs": {"messages": ["hi"]},
        "outputs": {"content": "checking"},
    }
    response = await client.post(
        f"/v1/sessions/{session_id}/nodes", json={"nodes": [root, chat]}
    )
    assert response.status_code == 200

    # The second batch references parents from the first batch.
    weather = {
        "id": str(uuid7()),
        "sequence": 2,
        "parent_id": chat["id"],
        "node_type": "tool_call",
        "name": "get_weather",
        "status": "completed",
        "tool_name": "get_weather",
        "inputs": {"city": "Berlin"},
        "outputs": {"temp": 21},
    }
    second_chat = {
        "id": str(uuid7()),
        "sequence": 3,
        "parent_id": root["id"],
        "node_type": "llm_call",
        "name": "chat",
        "status": "completed",
        "tokens": {"input_tokens": 50, "output_tokens": 10},
        "cost": "0.25",
    }
    response = await client.post(
        f"/v1/sessions/{session_id}/nodes", json={"nodes": [weather, second_chat]}
    )
    assert response.status_code == 200
    assert [item["key"] for item in response.json()] == [
        "span:run/llm_call:chat/tool_call:get_weather",
        "span:run/llm_call:chat#2",
    ]

    response = await client.patch(
        f"/v1/sessions/{session_id}",
        json={
            "status": "completed",
            "outputs": {"answer": "sunny"},
            "ended_at": "2026-07-01T12:05:00Z",
        },
    )
    assert response.status_code == 200

    response = await client.get(f"/v1/sessions/{session_id}")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "completed"
    assert body["outputs"] == {"answer": "sunny"}
    assert Decimal(body["cost"]) == Decimal("0.75")
    assert body["tokens"] == {
        "input_tokens": 150,
        "output_tokens": 30,
        "cached_input_tokens": None,
        "reasoning_tokens": None,
    }
    assert body["llm_call_count"] == 2
    assert body["tool_call_count"] == 1

    response = await client.get(f"/v1/sessions/{session_id}/nodes")
    assert response.status_code == 200
    nodes = response.json()
    assert [item["key"] for item in nodes] == [
        "span:run",
        "span:run/llm_call:chat",
        "span:run/llm_call:chat/tool_call:get_weather",
        "span:run/llm_call:chat#2",
    ]
    assert all(item["inputs"] is None for item in nodes)
    assert nodes[2]["cache_key"] == tool_call_cache_key(
        "get_weather", {"city": "Berlin"}
    )

    response = await client.get(
        f"/v1/sessions/{session_id}/nodes", params={"include_payloads": True}
    )
    assert response.status_code == 200
    nodes = response.json()
    assert nodes[1]["inputs"] == {"messages": ["hi"]}
    assert nodes[2]["outputs"] == {"temp": 21}


async def test_duplicate_import_conflict(client: httpx.AsyncClient) -> None:
    """Translate the database constraint into HTTP 409."""
    agent_id = await create_agent(client)
    body = {
        "agent_id": agent_id,
        "origin": "imported",
        "status": "completed",
        "provider": "langfuse",
        "external_id": "lf-1",
    }
    response = await client.post("/v1/sessions", json=body)
    assert response.status_code == 201
    response = await client.post("/v1/sessions", json=body)
    assert response.status_code == 409
    assert response.json() == {
        "detail": "Session external id 'lf-1' is already registered for "
        "provider 'langfuse'"
    }


async def test_tag_filter(client: httpx.AsyncClient) -> None:
    """Filter sessions through a tag link."""
    agent_id = await create_agent(client)
    tagged_id = await create_session(client, agent_id)
    await create_session(client, agent_id)
    response = await client.post("/v1/tags", json={"name": "prod"})
    assert response.status_code == 201
    tag_id = response.json()["id"]
    response = await client.post(
        f"/v1/tags/{tag_id}/links",
        json={"resource_type": "session", "resource_id": tagged_id},
    )
    assert response.status_code == 201

    response = await client.get("/v1/sessions", params={"tag": "prod"})
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["items"][0]["id"] == tagged_id


async def test_scores_persist_across_requests(client: httpx.AsyncClient) -> None:
    """Merge scores and observe them in later requests."""
    agent_id = await create_agent(client)
    session_id = await create_session(client, agent_id)
    response = await client.post(
        f"/v1/sessions/{session_id}/scores", json={"scores": {"conciseness": 0.5}}
    )
    assert response.status_code == 200
    response = await client.post(
        f"/v1/sessions/{session_id}/scores",
        json={"scores": {"conciseness": 0.7, "tone": 1.0}},
    )
    assert response.status_code == 200

    response = await client.get(f"/v1/sessions/{session_id}")
    assert response.status_code == 200
    assert response.json()["scores"] == {"conciseness": 0.7, "tone": 1.0}


async def test_delete_cascades_nodes_and_tag_links(
    client: httpx.AsyncClient,
) -> None:
    """Delete a session with its nodes and tag links across requests."""
    agent_id = await create_agent(client)
    session_id = await create_session(client, agent_id)
    parent_id = str(uuid7())
    response = await client.post(
        f"/v1/sessions/{session_id}/nodes",
        json={
            "nodes": [
                {
                    "id": parent_id,
                    "sequence": 0,
                    "node_type": "span",
                    "name": "run",
                    "status": "completed",
                },
                {
                    "id": str(uuid7()),
                    "sequence": 1,
                    "parent_id": parent_id,
                    "node_type": "span",
                    "name": "step",
                    "status": "completed",
                },
            ]
        },
    )
    assert response.status_code == 200
    response = await client.post("/v1/tags", json={"name": "prod"})
    tag_id = response.json()["id"]
    response = await client.post(
        f"/v1/tags/{tag_id}/links",
        json={"resource_type": "session", "resource_id": session_id},
    )
    assert response.status_code == 201

    response = await client.delete(f"/v1/sessions/{session_id}")
    assert response.status_code == 204
    response = await client.get(f"/v1/sessions/{session_id}")
    assert response.status_code == 404
    # Detaching the tag link now returns 404, the delete removed it.
    response = await client.delete(f"/v1/tags/{tag_id}/links/session/{session_id}")
    assert response.status_code == 404


async def test_node_ingest_conflicts(client: httpx.AsyncClient) -> None:
    """Translate node constraints into HTTP 409 and 422."""
    agent_id = await create_agent(client)
    session_id = await create_session(client, agent_id)
    response = await client.post(
        f"/v1/sessions/{session_id}/nodes",
        json={
            "nodes": [
                {
                    "id": str(uuid7()),
                    "sequence": 0,
                    "node_type": "span",
                    "name": "run",
                    "status": "completed",
                }
            ]
        },
    )
    assert response.status_code == 200

    response = await client.post(
        f"/v1/sessions/{session_id}/nodes",
        json={
            "nodes": [
                {
                    "id": str(uuid7()),
                    "sequence": 0,
                    "node_type": "span",
                    "name": "other",
                    "status": "completed",
                }
            ]
        },
    )
    assert response.status_code == 409
    assert response.json() == {
        "detail": f"A node sequence is already registered in session {session_id}"
    }

    missing_id = uuid.uuid4()
    response = await client.post(
        f"/v1/sessions/{session_id}/nodes",
        json={
            "nodes": [
                {
                    "id": str(uuid7()),
                    "sequence": 1,
                    "parent_id": str(missing_id),
                    "node_type": "span",
                    "name": "child",
                    "status": "completed",
                }
            ]
        },
    )
    assert response.status_code == 422
    assert response.json() == {"detail": f"Parent node {missing_id} was not found"}
