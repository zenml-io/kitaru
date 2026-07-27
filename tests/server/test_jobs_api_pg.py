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
"""End-to-end job tests against PostgreSQL."""

from collections.abc import AsyncGenerator

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from test_experiments_api_pg import SCORING_POLICY

from conftest import db_settings, lifespan_client
from kitaru.server.database import DatabaseService


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app running its full lifespan."""
    # Under the none auth scheme every request runs as the account
    # bootstrapped at startup, which owns all created jobs.
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


async def test_standalone_job_flow_persists_across_requests(
    client: httpx.AsyncClient,
) -> None:
    """Create standalone jobs and read them back with filters."""
    session_id = await seed_session(client)
    body = {"input_session_id": session_id, "scoring_policy": SCORING_POLICY}
    response = await client.post("/v1/replays", json=body)
    assert response.status_code == 201
    first = response.json()
    assert first["experiment_run_id"] is None
    assert first["tool_policy"]["default"]["type"] == "history"

    # Nulls are distinct in the unique constraint, so the same session
    # jobs standalone any number of times.
    response = await client.post("/v1/replays", json=body)
    assert response.status_code == 201

    response = await client.get(f"/v1/jobs/{first['id']}")
    assert response.status_code == 200
    assert response.json() == first

    response = await client.get(
        "/v1/jobs",
        params={"input_session_id": session_id, "standalone": "true"},
    )
    assert response.status_code == 200
    assert response.json()["total"] == 2

    # The job blocks session deletion.
    response = await client.delete(f"/v1/sessions/{session_id}")
    assert response.status_code == 409
    assert response.json() == {"detail": f"Session {session_id} is referenced by jobs"}


async def test_standalone_worker_lifecycle_end_to_end(
    client: httpx.AsyncClient,
) -> None:
    """Walk a standalone job through claim, release, retry, and delete."""
    session_id = await seed_session(client)
    body = {"input_session_id": session_id, "scoring_policy": SCORING_POLICY}
    response = await client.post("/v1/replays", json=body)
    assert response.status_code == 201
    job_id = response.json()["id"]

    response = await client.post("/v1/workers", json={"name": "worker-1"})
    assert response.status_code == 200
    worker_id = response.json()["id"]
    response = await client.post(
        f"/v1/jobs/{job_id}/claim", json={"worker_id": worker_id}
    )
    assert response.status_code == 200
    claimed = response.json()
    assert claimed["status"] == "claimed"
    assert claimed["worker_id"] == worker_id

    response = await client.post(f"/v1/jobs/{job_id}/release")
    assert response.status_code == 200
    released = response.json()
    assert released["status"] == "pending"
    assert released["attempt"] == 2
    assert released["worker_id"] is None

    response = await client.post("/v1/workers", json={"name": "worker-2"})
    assert response.status_code == 200
    other_worker_id = response.json()["id"]
    response = await client.post(
        f"/v1/jobs/{job_id}/claim", json={"worker_id": other_worker_id}
    )
    assert response.status_code == 200

    response = await client.post(
        f"/v1/workers/{other_worker_id}/heartbeat", json={"job_ids": [job_id]}
    )
    assert response.status_code == 200
    assert response.json() == {"abandon": []}

    response = await client.patch(
        f"/v1/jobs/{job_id}",
        json={"status": "failed", "error": "agent exited with code 1"},
    )
    assert response.status_code == 200

    response = await client.post(
        f"/v1/workers/{other_worker_id}/heartbeat", json={"job_ids": [job_id]}
    )
    assert response.status_code == 200
    assert response.json() == {"abandon": [job_id]}

    response = await client.post(f"/v1/jobs/{job_id}/retry")
    assert response.status_code == 200
    retried = response.json()
    assert retried["status"] == "pending"
    assert retried["attempt"] == 3
    assert retried["error"] is None
    assert retried["result_session_id"] is None

    response = await client.delete(f"/v1/jobs/{job_id}")
    assert response.status_code == 204
    response = await client.get(f"/v1/jobs/{job_id}")
    assert response.status_code == 404


async def test_session_run_flow_end_to_end(client: httpx.AsyncClient) -> None:
    """Create, claim, link, and complete a session run against PostgreSQL."""
    response = await client.post("/v1/agents", json={"name": "runner-bot"})
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
        "/v1/session-runs",
        json={"agent_id": agent_id, "inputs": {"prompt": "hi"}, "name": "smoke"},
    )
    assert response.status_code == 201
    created = response.json()
    assert created["kind"] == "session_run"
    assert created["execution_target"] == "pool"
    job_id = created["id"]

    response = await client.post("/v1/workers", json={"name": "worker-1"})
    assert response.status_code == 200
    worker_id = response.json()["id"]
    last_seen = response.json()["last_seen_at"]
    response = await client.post(
        "/v1/jobs/claim", json={"worker_id": worker_id, "max_jobs": 5}
    )
    assert response.status_code == 200
    claimed = response.json()["jobs"]
    assert [entry["job"]["id"] for entry in claimed] == [job_id]
    assert claimed[0]["job"]["worker_id"] == worker_id
    assert claimed[0]["spec"]["job_id"] == job_id
    response = await client.get(f"/v1/workers/{worker_id}")
    assert response.status_code == 200
    assert response.json()["last_seen_at"] >= last_seen

    response = await client.get(f"/v1/jobs/{job_id}/spec")
    assert response.status_code == 200
    spec = response.json()
    assert spec["kind"] == "session_run"
    assert spec["inputs"] == {"prompt": "hi"}
    assert spec["name"] == "smoke"
    assert spec["scorer"] is None

    response = await client.patch(f"/v1/jobs/{job_id}", json={"status": "running"})
    assert response.status_code == 200
    response = await client.post(
        "/v1/sessions",
        json={"agent_id": agent_id, "origin": "recorded", "job_id": job_id},
    )
    assert response.status_code == 201
    assert response.json()["origin"] == "recorded"
    result_session_id = response.json()["id"]

    response = await client.patch(f"/v1/jobs/{job_id}", json={"status": "completed"})
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "completed"
    assert body["result_session_id"] == result_session_id
    assert body["passed"] is None


async def test_migrated_job_table_carries_the_hot_path_indexes(
    client: httpx.AsyncClient,
) -> None:
    """Observe the partial claim and stale sweep indexes after the migrations."""
    _ = client
    settings = db_settings()
    engine = create_async_engine(DatabaseService.generate_database_uri(settings))
    try:
        async with engine.connect() as connection:
            rows = await connection.execute(
                text(
                    "SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'job'"
                )
            )
            definitions = {name: definition for name, definition in rows.all()}
    finally:
        await engine.dispose()

    assert (
        "WHERE ((status)::text = 'pending'::text)" in definitions["ix_job_pending_id"]
    )
    stale = definitions["ix_job_active_heartbeat_at"]
    assert "COALESCE(heartbeat_at, claimed_at)" in stale
    assert "WHERE" in stale
