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
"""Tests for the replay routes."""

import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest
from test_experiments_api import (
    SCORING_POLICY,
    SCORING_POLICY_RESPONSE,
    create_agent,
    create_completed_session,
    create_runnable_version,
)

from conftest import experiment_app


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with fake-backed services."""
    transport = httpx.ASGITransport(app=experiment_app())
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def create_replay(
    client: httpx.AsyncClient, session_id: str, **overrides: object
) -> dict:
    """Store a standalone replay through the API.

    Args:
        client: HTTP client for the app.
        session_id: Id of the session to replay.
        **overrides: Create request body overrides.

    Returns:
        Created job body.
    """
    body: dict[str, object] = {
        "input_session_id": session_id,
        "scoring_policy": SCORING_POLICY,
        **overrides,
    }
    response = await client.post("/v1/replays", json=body)
    assert response.status_code == 201
    return response.json()


async def test_create_replay_defaults(client: httpx.AsyncClient) -> None:
    """Create a standalone replay with the history default tool policy."""
    agent_id = await create_agent(client)
    version_id = await create_runnable_version(client, agent_id)
    session_id = await create_completed_session(client, agent_id)
    response = await client.post(
        "/v1/replays",
        json={"input_session_id": session_id, "scoring_policy": SCORING_POLICY},
    )
    assert response.status_code == 201
    body = response.json()
    assert body["experiment_run_id"] is None
    assert body["input_session_id"] == session_id
    assert body["result_session_id"] is None
    assert body["agent_version_id"] == version_id
    assert body["status"] == "pending"
    assert body["attempt"] == 1
    assert body["worker_id"] is None
    assert body["passed"] is None
    assert body["override"] is None
    assert body["tool_policy"] == {
        "default": {
            "type": "history",
            "scope": "original_session",
            "on_miss": "fail",
        },
        "tools": {},
    }
    assert body["scoring_policy"] == SCORING_POLICY_RESPONSE
    assert body["created"] is not None
    assert body["updated"] is not None
    assert uuid.UUID(body["id"])


async def test_create_replay_repeats_freely(client: httpx.AsyncClient) -> None:
    """Replay the same session standalone any number of times."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    session_id = await create_completed_session(client, agent_id)
    first = await create_replay(client, session_id)
    second = await create_replay(client, session_id)
    assert first["id"] != second["id"]


async def test_create_replay_rejects_cohort_scope(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for a history policy scoped to a cohort."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    session_id = await create_completed_session(client, agent_id)
    response = await client.post(
        "/v1/replays",
        json={
            "input_session_id": session_id,
            "scoring_policy": SCORING_POLICY,
            "tool_policy": {
                "default": {"type": "history", "scope": "cohort"},
                "tools": {},
            },
        },
    )
    assert response.status_code == 422
    assert response.json() == {
        "detail": "Standalone replays cannot use history scope 'cohort'"
    }


async def test_create_replay_in_progress_session(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for an in-progress original session."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    response = await client.post(
        "/v1/sessions", json={"agent_id": agent_id, "origin": "recorded"}
    )
    assert response.status_code == 201
    session_id = response.json()["id"]
    response = await client.post(
        "/v1/replays",
        json={"input_session_id": session_id, "scoring_policy": SCORING_POLICY},
    )
    assert response.status_code == 422
    assert response.json() == {"detail": f"Session {session_id} is in progress"}


async def test_create_replay_unknown_session(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown session id."""
    missing_id = uuid.uuid4()
    response = await client.post(
        "/v1/replays",
        json={
            "input_session_id": str(missing_id),
            "scoring_policy": SCORING_POLICY,
        },
    )
    assert response.status_code == 404
    assert response.json() == {"detail": f"Session {missing_id} was not found"}


async def test_create_replay_no_runnable_version(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 when no runnable version resolves."""
    agent_id = await create_agent(client)
    session_id = await create_completed_session(client, agent_id)
    response = await client.post(
        "/v1/replays",
        json={"input_session_id": session_id, "scoring_policy": SCORING_POLICY},
    )
    assert response.status_code == 409
    assert response.json() == {"detail": f"Agent {agent_id} has no runnable version"}


async def test_create_replay_cross_agent_version(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for a version of another agent."""
    agent_id = await create_agent(client)
    other_id = await create_agent(client, name="triage-bot")
    other_version_id = await create_runnable_version(client, other_id)
    session_id = await create_completed_session(client, agent_id)
    response = await client.post(
        "/v1/replays",
        json={
            "input_session_id": session_id,
            "agent_version_id": other_version_id,
            "scoring_policy": SCORING_POLICY,
        },
    )
    assert response.status_code == 422
    assert response.json() == {
        "detail": f"Agent version {other_version_id} does not belong to "
        f"agent {agent_id}"
    }


async def test_create_replay_missing_scoring_policy(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 without a scoring policy."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    session_id = await create_completed_session(client, agent_id)
    response = await client.post("/v1/replays", json={"input_session_id": session_id})
    assert response.status_code == 422
