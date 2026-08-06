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

import json
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import db_settings, lifespan_client


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app running its full lifespan."""
    async with lifespan_client(db_settings()) as client:
        yield client


@pytest.fixture
async def agent_id(client: httpx.AsyncClient) -> str:
    """Provide the id of an agent to attach sessions to."""
    created = (await client.post("/v1/agents", json={"name": "assistant"})).json()
    return created["id"]


def _session_body(agent_id: str, **overrides: object) -> dict[str, object]:
    body: dict[str, object] = {
        "agent_id": agent_id,
        "origin": "recorded",
        "inputs": {"prompt": "hi"},
        "outputs": None,
        "expected": None,
        "metadata": {},
    }
    body.update(overrides)
    return body


async def test_sessions_persist_across_requests(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Prove the per-request commit through separate requests."""
    response = await client.post("/v1/sessions", json=_session_body(agent_id))
    assert response.status_code == 201
    created = response.json()

    response = await client.get(f"/v1/sessions/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created

    response = await client.get("/v1/sessions")
    assert response.status_code == 200
    body = response.json()
    assert body["next_cursor"] is None
    assert body["items"][0] == created


async def test_sessions_number_sequentially_per_agent(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Number an agent's sessions sequentially, each agent counting alone."""
    first = (await client.post("/v1/sessions", json=_session_body(agent_id))).json()
    second = (await client.post("/v1/sessions", json=_session_body(agent_id))).json()
    other_agent = (await client.post("/v1/agents", json={"name": "other"})).json()
    other = (
        await client.post("/v1/sessions", json=_session_body(other_agent["id"]))
    ).json()
    assert first["number"] == 1
    assert second["number"] == 2
    assert other["number"] == 1


async def test_duplicate_external_id_conflict(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Translate the database constraint into HTTP 409."""
    body = _session_body(
        agent_id, origin="imported", imported_from="langsmith", external_id="run-1"
    )
    response = await client.post("/v1/sessions", json=body)
    assert response.status_code == 201
    response = await client.post("/v1/sessions", json=body)
    assert response.status_code == 409


async def test_update_persists_across_requests(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Persist a status transition and outputs update across requests."""
    created = (await client.post("/v1/sessions", json=_session_body(agent_id))).json()
    response = await client.patch(
        f"/v1/sessions/{created['id']}",
        json={"status": "completed", "outputs": {"answer": 42}},
    )
    assert response.status_code == 200

    response = await client.get(f"/v1/sessions/{created['id']}")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "completed"
    assert body["outputs"] == {"answer": 42}
    assert body["updated"] > created["updated"]


async def test_delete_persists_across_requests(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Persist a deletion across requests."""
    created = (await client.post("/v1/sessions", json=_session_body(agent_id))).json()
    response = await client.delete(f"/v1/sessions/{created['id']}")
    assert response.status_code == 204

    response = await client.get(f"/v1/sessions/{created['id']}")
    assert response.status_code == 404


async def test_ingest_and_list_nodes_persist_across_requests(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Persist ingested nodes and their session rollups across requests."""
    created = (await client.post("/v1/sessions", json=_session_body(agent_id))).json()

    response = await client.post(
        f"/v1/sessions/{created['id']}/nodes",
        json={
            "nodes": [
                {
                    "index": 0,
                    "node_type": "llm_call",
                    "name": "call",
                    "status": "completed",
                    "cost": "1.50",
                    "tokens": {"input_tokens": 10, "output_tokens": 5},
                    "inputs": {"q": "hi"},
                    "outputs": None,
                    "attributes": None,
                    "metadata": {},
                },
                {
                    "index": 1,
                    "parent_index": 0,
                    "node_type": "tool_call",
                    "name": "search",
                    "status": "completed",
                    "tool_name": "search",
                    "inputs": {"q": "hi"},
                    "outputs": None,
                    "attributes": None,
                    "metadata": {},
                },
            ]
        },
    )
    assert response.status_code == 200
    nodes = response.json()
    assert nodes[1]["parent_id"] == nodes[0]["id"]
    assert nodes[1]["cache_key"] is not None

    session = (await client.get(f"/v1/sessions/{created['id']}")).json()
    assert session["cost"] == "1.50"
    assert session["llm_call_count"] == 1
    assert session["tool_call_count"] == 1

    response = await client.get(f"/v1/sessions/{created['id']}/nodes")
    assert response.status_code == 200
    items = response.json()["items"]
    assert [item["index"] for item in items] == [0, 1]
    assert items[0]["inputs"] is None

    response = await client.get(
        f"/v1/sessions/{created['id']}/nodes", params={"include_payloads": "true"}
    )
    assert response.status_code == 200
    items = response.json()["items"]
    assert items[0]["inputs"] == {"q": "hi"}


async def test_ingest_into_terminal_recorded_session_rejected(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Reject node ingest into a terminal recorded session."""
    created = (await client.post("/v1/sessions", json=_session_body(agent_id))).json()
    await client.patch(f"/v1/sessions/{created['id']}", json={"status": "completed"})
    response = await client.post(
        f"/v1/sessions/{created['id']}/nodes",
        json={
            "nodes": [
                {
                    "index": 0,
                    "node_type": "span",
                    "name": "x",
                    "status": "completed",
                    "inputs": None,
                    "outputs": None,
                    "attributes": None,
                    "metadata": {},
                }
            ]
        },
    )
    assert response.status_code == 409


async def test_list_sessions_filters_by_status_filter(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Filter sessions by a status filter expression end to end."""
    completed = (await client.post("/v1/sessions", json=_session_body(agent_id))).json()
    await client.patch(f"/v1/sessions/{completed['id']}", json={"status": "completed"})
    await client.post("/v1/sessions", json=_session_body(agent_id))

    filter_expression = {"field": "status", "op": "eq", "value": "completed"}
    response = await client.get(
        "/v1/sessions", params={"filter": json.dumps(filter_expression)}
    )
    assert response.status_code == 200
    items = response.json()["items"]
    assert [item["id"] for item in items] == [completed["id"]]
