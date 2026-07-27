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
"""Tests for the session run routes."""

import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest
from test_experiments_api import create_agent, create_runnable_version
from test_jobs_api import register_worker

from conftest import experiment_app


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with fake-backed services."""
    transport = httpx.ASGITransport(app=experiment_app())
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def test_create_session_run(client: httpx.AsyncClient) -> None:
    """Create a session run resolving the latest runnable version."""
    agent_id = await create_agent(client)
    version_id = await create_runnable_version(client, agent_id)
    response = await client.post(
        "/v1/session-runs",
        json={"agent_id": agent_id, "inputs": {"prompt": "hi"}, "name": "smoke"},
    )
    assert response.status_code == 201
    body = response.json()
    assert body["kind"] == "session_run"
    assert body["agent_version_id"] == version_id
    assert body["inputs"] == {"prompt": "hi"}
    assert body["name"] == "smoke"
    assert body["status"] == "pending"
    assert body["execution_target"] == "pool"
    assert body["experiment_run_id"] is None
    assert body["input_session_id"] is None

    response = await client.get(f"/v1/jobs/{body['id']}")
    assert response.status_code == 200
    assert response.json() == body


async def test_create_session_run_explicit_version(client: httpx.AsyncClient) -> None:
    """Create a session run from an agent version id alone."""
    agent_id = await create_agent(client)
    version_id = await create_runnable_version(client, agent_id)
    response = await client.post(
        "/v1/session-runs", json={"agent_version_id": version_id}
    )
    assert response.status_code == 201
    body = response.json()
    assert body["agent_version_id"] == version_id
    assert body["inputs"] is None
    assert body["name"] is None


async def test_create_session_run_requires_agent_reference(
    client: httpx.AsyncClient,
) -> None:
    """Observe HTTP 422 when neither agent nor agent version is set."""
    response = await client.post("/v1/session-runs", json={})
    assert response.status_code == 422


async def test_create_session_run_unknown_version(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown agent version id."""
    missing_id = uuid.uuid4()
    response = await client.post(
        "/v1/session-runs", json={"agent_version_id": str(missing_id)}
    )
    assert response.status_code == 404
    assert response.json() == {"detail": f"Agent version {missing_id} was not found"}


async def test_create_session_run_on_demand_without_image(
    client: httpx.AsyncClient,
) -> None:
    """Observe HTTP 409 for an on demand target without a run image."""
    agent_id = await create_agent(client)
    version_id = await create_runnable_version(client, agent_id)
    response = await client.post(
        "/v1/session-runs",
        json={"agent_id": agent_id, "execution_target": "on_demand"},
    )
    assert response.status_code == 409
    assert response.json() == {"detail": f"Agent version {version_id} has no run image"}


async def test_session_run_lifecycle_through_job_routes(
    client: httpx.AsyncClient,
) -> None:
    """Walk a session run through claim, spec, linking, and completion."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    response = await client.post(
        "/v1/session-runs",
        json={"agent_id": agent_id, "inputs": {"prompt": "hi"}, "name": "smoke"},
    )
    assert response.status_code == 201
    job_id = response.json()["id"]

    worker_id = await register_worker(client)
    response = await client.post(
        "/v1/jobs/claim", json={"worker_id": worker_id, "max_jobs": 5}
    )
    assert response.status_code == 200
    claimed = response.json()["jobs"]
    assert [entry["job"]["id"] for entry in claimed] == [job_id]
    assert claimed[0]["job"]["worker_id"] == worker_id
    assert claimed[0]["spec"]["inputs"] == {"prompt": "hi"}

    response = await client.get(f"/v1/jobs/{job_id}/spec")
    assert response.status_code == 200
    spec = response.json()
    assert spec == {
        "job_id": job_id,
        "kind": "session_run",
        "inputs": {"prompt": "hi"},
        "override": None,
        "tool_policy": None,
        "scorer": None,
        "importer": None,
        "run": {
            "command": "python agent.py",
            "working_dir": None,
            "env": {},
            "timeout_seconds": 600,
        },
        "secret_env": {},
        "input_session_id": None,
        "name": "smoke",
    }

    response = await client.patch(f"/v1/jobs/{job_id}", json={"status": "running"})
    assert response.status_code == 200

    response = await client.post(
        "/v1/sessions",
        json={"agent_id": agent_id, "origin": "recorded", "job_id": job_id},
    )
    assert response.status_code == 201
    result_session = response.json()
    assert result_session["origin"] == "recorded"

    response = await client.patch(f"/v1/jobs/{job_id}", json={"status": "completed"})
    assert response.status_code == 409
    assert response.json() == {
        "detail": f"Result session {result_session['id']} of job {job_id} "
        "is not completed"
    }

    response = await client.patch(
        f"/v1/sessions/{result_session['id']}", json={"status": "completed"}
    )
    assert response.status_code == 200
    response = await client.patch(f"/v1/jobs/{job_id}", json={"status": "completed"})
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "completed"
    assert body["result_session_id"] == result_session["id"]
    assert body["result"] is None

    response = await client.post(
        f"/v1/jobs/{job_id}/tool-lookup",
        json={"tool_name": "get_weather", "inputs": None, "cache_key": "a" * 64},
    )
    assert response.status_code == 409
    assert response.json() == {"detail": f"Job {job_id} is not of kind 'replay'"}


async def test_claim_unknown_worker(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown worker id."""
    missing_id = uuid.uuid4()
    response = await client.post(
        "/v1/jobs/claim", json={"worker_id": str(missing_id), "max_jobs": 1}
    )
    assert response.status_code == 404
    assert response.json() == {"detail": f"Worker {missing_id} was not found"}
