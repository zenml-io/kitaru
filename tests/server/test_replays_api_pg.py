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
"""End-to-end replay tests against PostgreSQL."""

from collections.abc import AsyncGenerator

import httpx
import pytest
from test_experiments_api_pg import SCORING_POLICY

from conftest import db_settings, lifespan_client


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app running its full lifespan."""
    # Under the none auth scheme every request runs as the account
    # bootstrapped at startup, which owns all created replays.
    async with lifespan_client(db_settings()) as client:
        yield client


async def seed_session(client: httpx.AsyncClient) -> str:
    """Store an agent, a runnable version, and a completed session.

    Args:
        client: HTTP client for the app.

    Returns:
        Id of the created session.
    """
    response = await client.post("/v1/agents", json={"name": "support-bot"})
    assert response.status_code == 201
    agent_id = response.json()["id"]
    response = await client.post(
        f"/v1/agents/{agent_id}/versions",
        json={
            "version": "v1",
            "run_spec": {"command": "python agent.py", "timeout_seconds": 600},
        },
    )
    assert response.status_code == 201
    response = await client.post(
        "/v1/sessions", json={"agent_id": agent_id, "origin": "recorded"}
    )
    assert response.status_code == 201
    session_id = response.json()["id"]
    response = await client.patch(
        f"/v1/sessions/{session_id}", json={"status": "completed"}
    )
    assert response.status_code == 200
    return session_id


async def test_standalone_replay_flow_persists_across_requests(
    client: httpx.AsyncClient,
) -> None:
    """Create standalone replays and read them back with filters."""
    session_id = await seed_session(client)
    body = {"original_session_id": session_id, "scoring_policy": SCORING_POLICY}
    response = await client.post("/v1/replays", json=body)
    assert response.status_code == 201
    first = response.json()
    assert first["experiment_run_id"] is None
    assert first["tool_policy"]["default"]["type"] == "history"

    # Nulls are distinct in the unique constraint, so the same session
    # replays standalone any number of times.
    response = await client.post("/v1/replays", json=body)
    assert response.status_code == 201

    response = await client.get(f"/v1/replays/{first['id']}")
    assert response.status_code == 200
    assert response.json() == first

    response = await client.get(
        "/v1/replays",
        params={"original_session_id": session_id, "standalone": "true"},
    )
    assert response.status_code == 200
    assert response.json()["total"] == 2

    # The replay blocks session deletion.
    response = await client.delete(f"/v1/sessions/{session_id}")
    assert response.status_code == 409
    assert response.json() == {
        "detail": f"Session {session_id} is referenced by replays"
    }


async def test_standalone_worker_lifecycle_end_to_end(
    client: httpx.AsyncClient,
) -> None:
    """Walk a standalone replay through claim, release, retry, and delete."""
    session_id = await seed_session(client)
    body = {"original_session_id": session_id, "scoring_policy": SCORING_POLICY}
    response = await client.post("/v1/replays", json=body)
    assert response.status_code == 201
    replay_id = response.json()["id"]

    response = await client.post(
        f"/v1/replays/{replay_id}/claim", json={"worker_id": "worker-1"}
    )
    assert response.status_code == 200
    claimed = response.json()
    assert claimed["status"] == "claimed"
    assert claimed["worker_id"] == "worker-1"

    response = await client.post(f"/v1/replays/{replay_id}/release")
    assert response.status_code == 200
    released = response.json()
    assert released["status"] == "pending"
    assert released["attempt"] == 2
    assert released["worker_id"] is None

    response = await client.post(
        f"/v1/replays/{replay_id}/claim", json={"worker_id": "worker-2"}
    )
    assert response.status_code == 200
    response = await client.patch(
        f"/v1/replays/{replay_id}",
        json={"status": "failed", "error": "agent exited with code 1"},
    )
    assert response.status_code == 200

    response = await client.post(f"/v1/replays/{replay_id}/heartbeat")
    assert response.status_code == 200
    assert response.json() == {"status": "failed", "canceled": True}

    response = await client.post(f"/v1/replays/{replay_id}/retry")
    assert response.status_code == 200
    retried = response.json()
    assert retried["status"] == "pending"
    assert retried["attempt"] == 3
    assert retried["error"] is None
    assert retried["result_session_id"] is None

    response = await client.delete(f"/v1/replays/{replay_id}")
    assert response.status_code == 204
    response = await client.get(f"/v1/replays/{replay_id}")
    assert response.status_code == 404
