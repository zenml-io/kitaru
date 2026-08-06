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
"""End-to-end annotation tests against PostgreSQL."""

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


async def _create_session_with_node(
    client: httpx.AsyncClient, agent_id: str
) -> tuple[str, str]:
    """Store a session with one ingested node and return their ids."""
    session = (
        await client.post(
            "/v1/sessions",
            json={
                "agent_id": agent_id,
                "origin": "recorded",
                "inputs": {"prompt": "hi"},
                "outputs": None,
                "metadata": {},
            },
        )
    ).json()
    nodes = (
        await client.post(
            f"/v1/sessions/{session['id']}/nodes",
            json={
                "nodes": [
                    {
                        "index": 0,
                        "node_type": "llm_call",
                        "name": "call",
                        "status": "completed",
                        "inputs": {"q": "hi"},
                        "outputs": None,
                        "attributes": None,
                        "metadata": {},
                    }
                ]
            },
        )
    ).json()
    return session["id"], nodes[0]["id"]


async def _create_investigation_with_link(
    client: httpx.AsyncClient, agent_id: str, session_id: str
) -> tuple[dict[str, object], dict[str, object]]:
    """Create a one-session investigation and return it with its session link."""
    investigation = (
        await client.post(
            "/v1/investigations",
            json={
                "agent_id": agent_id,
                "name": "payment-failures",
                "questions": [
                    {"key": "root_cause", "question": "What caused the failure?"},
                    {"key": "retry_ok", "question": "Was retrying the right call?"},
                ],
                "sessions": [{"session_id": session_id}],
            },
        )
    ).json()
    links = (
        await client.get(f"/v1/investigations/{investigation['id']}/sessions")
    ).json()["items"]
    return investigation, links[0]


async def test_manual_annotation_persists_across_requests(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Prove the per-request commit through separate requests."""
    session_id, node_id = await _create_session_with_node(client, agent_id)
    response = await client.post(
        "/v1/annotations",
        json={
            "session_id": session_id,
            "selector": {"node_id": node_id, "part": "input"},
            "value": "Looks fine",
        },
    )
    assert response.status_code == 201
    created = response.json()
    assert created["session_id"] == session_id
    assert created["investigation_session_id"] is None
    assert created["question_key"] is None

    response = await client.get(f"/v1/annotations/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created

    filter_expression = {"field": "session_id", "op": "eq", "value": session_id}
    response = await client.get(
        "/v1/annotations", params={"filter": json.dumps(filter_expression)}
    )
    assert response.status_code == 200
    body = response.json()
    assert body["next_cursor"] is None
    assert [item["id"] for item in body["items"]] == [created["id"]]


async def test_manual_annotation_invalid_node_rejected(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Reject a selector naming a node outside the annotated session."""
    session_id, _ = await _create_session_with_node(client, agent_id)
    response = await client.post(
        "/v1/annotations",
        json={
            "session_id": session_id,
            "selector": {"node_id": "00000000-0000-0000-0000-000000000000"},
            "value": "x",
        },
    )
    assert response.status_code == 422


async def test_update_and_delete_manual_annotation(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Persist a value update and a deletion across requests."""
    session_id, _ = await _create_session_with_node(client, agent_id)
    created = (
        await client.post(
            "/v1/annotations",
            json={
                "session_id": session_id,
                "value": "first note",
            },
        )
    ).json()

    response = await client.patch(
        f"/v1/annotations/{created['id']}",
        json={"value": {"confidence": 0.9, "flagged": True}},
    )
    assert response.status_code == 200

    response = await client.get(f"/v1/annotations/{created['id']}")
    assert response.status_code == 200
    body = response.json()
    assert body["value"] == {"confidence": 0.9, "flagged": True}
    assert body["updated"] > created["updated"]

    response = await client.delete(f"/v1/annotations/{created['id']}")
    assert response.status_code == 204

    response = await client.get(f"/v1/annotations/{created['id']}")
    assert response.status_code == 404


async def test_investigation_answer_starts_investigation_and_replaces_on_repeat(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Start the investigation on the first answer, and replace it on a repeat."""
    session_id, _ = await _create_session_with_node(client, agent_id)
    investigation, link = await _create_investigation_with_link(
        client, agent_id, session_id
    )
    assert investigation["status"] == "pending"

    response = await client.post(
        "/v1/annotations",
        json={
            "investigation_session_id": link["id"],
            "question_key": "root_cause",
            "value": "Retry loop swallowed the error",
        },
    )
    assert response.status_code == 201
    answer = response.json()
    assert answer["session_id"] == session_id
    assert answer["investigation_session_id"] == link["id"]
    assert answer["question_key"] == "root_cause"

    response = await client.get(f"/v1/investigations/{investigation['id']}")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "in_progress"
    assert body["started_at"] is not None

    # Answering the same question again replaces the row instead of adding one.
    response = await client.post(
        "/v1/annotations",
        json={
            "investigation_session_id": link["id"],
            "question_key": "root_cause",
            "value": "Actually a timeout, not a retry loop",
        },
    )
    assert response.status_code == 201
    replaced = response.json()
    assert replaced["id"] == answer["id"]
    assert replaced["value"] == "Actually a timeout, not a retry loop"

    filter_expression = {
        "field": "investigation_session_id",
        "op": "eq",
        "value": link["id"],
    }
    response = await client.get(
        "/v1/annotations", params={"filter": json.dumps(filter_expression)}
    )
    assert response.status_code == 200
    items = response.json()["items"]
    assert len(items) == 1
    assert items[0]["value"] == "Actually a timeout, not a retry loop"


async def test_investigation_answer_unknown_question_key_rejected(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Reject an answer whose question key is not one of the investigation's."""
    session_id, _ = await _create_session_with_node(client, agent_id)
    _investigation, link = await _create_investigation_with_link(
        client, agent_id, session_id
    )
    response = await client.post(
        "/v1/annotations",
        json={
            "investigation_session_id": link["id"],
            "question_key": "not_a_question",
            "value": "x",
        },
    )
    assert response.status_code == 422


async def test_list_annotations_filtered_by_investigation_id(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Filter annotations by investigation id through the session-link join."""
    session_id, _ = await _create_session_with_node(client, agent_id)
    investigation, link = await _create_investigation_with_link(
        client, agent_id, session_id
    )
    await client.post(
        "/v1/annotations",
        json={
            "session_id": session_id,
            "value": "manual note",
        },
    )
    answer = (
        await client.post(
            "/v1/annotations",
            json={
                "investigation_session_id": link["id"],
                "question_key": "root_cause",
                "value": "answer",
            },
        )
    ).json()

    filter_expression = {
        "field": "investigation_id",
        "op": "eq",
        "value": investigation["id"],
    }
    response = await client.get(
        "/v1/annotations", params={"filter": json.dumps(filter_expression)}
    )
    assert response.status_code == 200
    items = response.json()["items"]
    assert [item["id"] for item in items] == [answer["id"]]


async def test_delete_investigation_cascades_answers_but_keeps_manual(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Cascade an investigation's links and answers, leaving manual notes."""
    session_id, _ = await _create_session_with_node(client, agent_id)
    investigation, link = await _create_investigation_with_link(
        client, agent_id, session_id
    )
    manual = (
        await client.post(
            "/v1/annotations",
            json={
                "session_id": session_id,
                "value": "manual note",
            },
        )
    ).json()
    answer = (
        await client.post(
            "/v1/annotations",
            json={
                "investigation_session_id": link["id"],
                "question_key": "root_cause",
                "value": "answer",
            },
        )
    ).json()

    response = await client.delete(f"/v1/investigations/{investigation['id']}")
    assert response.status_code == 204

    response = await client.get(f"/v1/annotations/{answer['id']}")
    assert response.status_code == 404

    response = await client.get(f"/v1/annotations/{manual['id']}")
    assert response.status_code == 200

    filter_expression = {"field": "session_id", "op": "eq", "value": session_id}
    response = await client.get(
        "/v1/annotations", params={"filter": json.dumps(filter_expression)}
    )
    assert response.status_code == 200
    items = response.json()["items"]
    assert [item["id"] for item in items] == [manual["id"]]
