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
"""End-to-end investigation tests against PostgreSQL."""

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
    """Provide the id of an agent to investigate sessions of."""
    created = (await client.post("/v1/agents", json={"name": "assistant"})).json()
    return created["id"]


async def _create_session(client: httpx.AsyncClient, agent_id: str) -> str:
    """Store a session on the given agent and return its id."""
    response = await client.post(
        "/v1/sessions",
        json={
            "agent_id": agent_id,
            "origin": "recorded",
            "inputs": {"prompt": "hi"},
            "outputs": None,
            "metadata": {},
        },
    )
    return response.json()["id"]


async def _create_investigation(
    client: httpx.AsyncClient, agent_id: str, session_ids: list[str]
) -> dict[str, object]:
    """Create an investigation with two questions linking the given sessions.

    The first session carries a curated view.
    """
    response = await client.post(
        "/v1/investigations",
        json={
            "agent_id": agent_id,
            "name": "payment-failures",
            "description": "Investigate silent payment failures",
            "questions": [
                {"key": "root_cause", "question": "What caused the failure?"},
                {"key": "retry_ok", "question": "Was retrying the right call?"},
            ],
            "sessions": [
                {
                    "session_id": session_ids[0],
                    "view": {
                        "summary": "Retry loop without backoff",
                        "items": [
                            {
                                "label": "Retry loop",
                                "description": ("Retries three times without backoff."),
                            }
                        ],
                    },
                },
                *[{"session_id": session_id} for session_id in session_ids[1:]],
            ],
        },
    )
    assert response.status_code == 201
    return response.json()


async def test_investigation_persists_across_requests(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Prove the per-request commit through separate requests."""
    session_ids = [await _create_session(client, agent_id) for _ in range(2)]
    created = await _create_investigation(client, agent_id, session_ids)
    assert created["status"] == "pending"
    assert created["total_sessions"] == 2
    assert created["completed_sessions"] == 0

    response = await client.get(f"/v1/investigations/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created

    filter_expression = {"field": "agent_id", "op": "eq", "value": agent_id}
    response = await client.get(
        "/v1/investigations", params={"filter": json.dumps(filter_expression)}
    )
    assert response.status_code == 200
    body = response.json()
    assert body["next_cursor"] is None
    assert [item["id"] for item in body["items"]] == [created["id"]]


async def test_update_persists_across_requests(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Persist a name and description update across requests."""
    session_ids = [await _create_session(client, agent_id) for _ in range(2)]
    created = await _create_investigation(client, agent_id, session_ids)

    response = await client.patch(
        f"/v1/investigations/{created['id']}",
        json={"name": "renamed", "description": "updated rationale"},
    )
    assert response.status_code == 200

    response = await client.get(f"/v1/investigations/{created['id']}")
    assert response.status_code == 200
    body = response.json()
    assert body["name"] == "renamed"
    assert body["description"] == "updated rationale"
    assert body["updated"] > created["updated"]


async def test_list_sessions_ordered_by_position(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """List an investigation's sessions in position order, with the curated view."""
    session_ids = [await _create_session(client, agent_id) for _ in range(2)]
    created = await _create_investigation(client, agent_id, session_ids)

    response = await client.get(f"/v1/investigations/{created['id']}/sessions")
    assert response.status_code == 200
    body = response.json()
    items = body["items"]
    assert [item["session_id"] for item in items] == session_ids
    assert [item["position"] for item in items] == [0, 1]
    assert [item["verdict"] for item in items] == [None, None]
    assert items[0]["view"]["summary"] == "Retry loop without backoff"
    assert items[1]["view"] is None


async def test_manual_completion_after_verdicts(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Complete the investigation manually after every session got a verdict."""
    session_ids = [await _create_session(client, agent_id) for _ in range(2)]
    created = await _create_investigation(client, agent_id, session_ids)

    response = await client.patch(
        f"/v1/investigations/{created['id']}/sessions/{session_ids[0]}",
        json={"verdict": "acceptable"},
    )
    assert response.status_code == 200
    assert response.json()["verdict"] == "acceptable"

    response = await client.patch(
        f"/v1/investigations/{created['id']}/sessions/{session_ids[1]}",
        json={"verdict": "problematic"},
    )
    assert response.status_code == 200
    assert response.json()["verdict"] == "problematic"

    # Verdicts alone never complete the investigation.
    response = await client.get(f"/v1/investigations/{created['id']}")
    assert response.json()["status"] == "pending"
    assert response.json()["completed_sessions"] == 2

    response = await client.patch(
        f"/v1/investigations/{created['id']}", json={"status": "completed"}
    )
    assert response.status_code == 200

    response = await client.get(f"/v1/investigations/{created['id']}")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "completed"
    assert body["ended_at"] is not None
    assert body["completed_sessions"] == 2


async def test_create_investigation_missing_agent(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 when the agent does not exist."""
    response = await client.post(
        "/v1/investigations",
        json={
            "agent_id": "00000000-0000-0000-0000-000000000000",
            "name": "investigation",
            "questions": [{"key": "q", "question": "Why?"}],
            "sessions": [],
        },
    )
    assert response.status_code == 404
