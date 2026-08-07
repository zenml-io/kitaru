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
"""End-to-end job and task lifecycle tests against PostgreSQL."""

from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import db_settings, lifespan_client

RUNTIME = {"platform": "bare"}


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app running its full lifespan."""
    async with lifespan_client(db_settings()) as client:
        yield client


async def _create_runnable_agent_version(client: httpx.AsyncClient) -> tuple[str, str]:
    """Create an agent and a runnable version.

    Returns:
        Agent id and agent version id.
    """
    agent = (await client.post("/api/v1/agents", json={"name": "assistant"})).json()
    version = (
        await client.post(
            f"/api/v1/agents/{agent['id']}/versions",
            json={
                "run_spec": {
                    "type": "command",
                    "command": "run.sh",
                    "timeout_seconds": 60,
                }
            },
        )
    ).json()
    return agent["id"], version["id"]


async def test_session_run_lifecycle_completes_the_job(
    client: httpx.AsyncClient,
) -> None:
    """A claimed, completed agent task settles its job completed."""
    agent_id, version_id = await _create_runnable_agent_version(client)
    job = (
        await client.post(
            "/api/v1/session-runs",
            json={"agent_version_id": version_id, "inputs": {"q": "hi"}},
        )
    ).json()
    assert job["status"] == "pending"

    registration = (
        await client.post(
            "/api/v1/workers",
            json={"name": "worker-1", "scope": {}, "runtime": RUNTIME, "metadata": {}},
        )
    ).json()
    worker_headers = {"Authorization": f"Bearer {registration['token']}"}

    claimed = (
        await client.post(
            "/api/v1/tasks/claim", json={"max_tasks": 10}, headers=worker_headers
        )
    ).json()
    assert len(claimed["tasks"]) == 1
    entry = claimed["tasks"][0]
    task = entry["task"]
    assert task["job_id"] == job["id"]
    assert task["status"] == "claimed"
    task_headers = {"Authorization": f"Bearer {entry['token']}"}

    response = await client.get(f"/api/v1/jobs/{job['id']}")
    assert response.json()["status"] == "running"

    response = await client.patch(
        f"/api/v1/tasks/{task['id']}", json={"status": "running"}, headers=task_headers
    )
    assert response.status_code == 200

    session = (
        await client.post(
            "/api/v1/sessions",
            json={
                "agent_id": agent_id,
                "origin": "recorded",
                "inputs": None,
                "outputs": None,
            },
            headers=task_headers,
        )
    ).json()

    response = await client.patch(
        f"/api/v1/sessions/{session['id']}", json={"status": "completed", "outputs": {}}
    )
    assert response.status_code == 200

    response = await client.patch(
        f"/api/v1/tasks/{task['id']}", json={"status": "completed"}, headers=task_headers
    )
    assert response.status_code == 200

    response = await client.get(f"/api/v1/jobs/{job['id']}")
    body = response.json()
    assert body["status"] == "completed"
    assert body["ended_at"] is not None

    response = await client.get(f"/api/v1/tasks/{task['id']}")
    assert response.json()["result_session_id"] == session["id"]


async def test_cancel_job_persists_across_requests(client: httpx.AsyncClient) -> None:
    """Canceling a job with only pending tasks settles it canceled."""
    _, version_id = await _create_runnable_agent_version(client)
    job = (
        await client.post(
            "/api/v1/session-runs",
            json={"agent_version_id": version_id, "inputs": None},
        )
    ).json()

    response = await client.post(f"/api/v1/jobs/{job['id']}/cancel")
    assert response.status_code == 200
    assert response.json()["cancel_requested_at"] is not None

    response = await client.get(f"/api/v1/jobs/{job['id']}")
    assert response.json()["status"] == "canceled"

    tasks = (await client.get(f"/api/v1/jobs/{job['id']}/tasks")).json()["items"]
    assert tasks[0]["status"] == "canceled"


async def test_cancel_job_conflicts_once_settled(client: httpx.AsyncClient) -> None:
    """A second cancel of an already settled job observes HTTP 409."""
    _, version_id = await _create_runnable_agent_version(client)
    job = (
        await client.post(
            "/api/v1/session-runs",
            json={"agent_version_id": version_id, "inputs": None},
        )
    ).json()
    await client.post(f"/api/v1/jobs/{job['id']}/cancel")
    response = await client.post(f"/api/v1/jobs/{job['id']}/cancel")
    assert response.status_code == 409


async def test_delete_job_cascades_its_tasks(client: httpx.AsyncClient) -> None:
    """Deleting a job removes its tasks, no longer reachable by id."""
    _, version_id = await _create_runnable_agent_version(client)
    job = (
        await client.post(
            "/api/v1/session-runs",
            json={"agent_version_id": version_id, "inputs": None},
        )
    ).json()
    tasks = (await client.get(f"/api/v1/jobs/{job['id']}/tasks")).json()["items"]
    task_id = tasks[0]["id"]

    response = await client.delete(f"/api/v1/jobs/{job['id']}")
    assert response.status_code == 204

    assert (await client.get(f"/api/v1/jobs/{job['id']}")).status_code == 404
    assert (await client.get(f"/api/v1/tasks/{task_id}")).status_code == 404


async def test_heartbeat_reports_cancel_requested_tasks(
    client: httpx.AsyncClient,
) -> None:
    """A heartbeat reports a cancel-requested task in cancel_task_ids."""
    _, version_id = await _create_runnable_agent_version(client)
    job = (
        await client.post(
            "/api/v1/session-runs",
            json={"agent_version_id": version_id, "inputs": None},
        )
    ).json()
    registration = (
        await client.post(
            "/api/v1/workers",
            json={"name": "worker-1", "scope": {}, "runtime": RUNTIME, "metadata": {}},
        )
    ).json()
    worker = registration["worker"]
    worker_headers = {"Authorization": f"Bearer {registration['token']}"}
    claimed = (
        await client.post(
            "/api/v1/tasks/claim", json={"max_tasks": 10}, headers=worker_headers
        )
    ).json()
    task_id = claimed["tasks"][0]["task"]["id"]

    await client.post(f"/api/v1/jobs/{job['id']}/cancel")

    response = await client.post(
        f"/api/v1/workers/{worker['id']}/heartbeat",
        json={"task_ids": [task_id]},
        headers=worker_headers,
    )
    assert response.status_code == 200
    assert response.json()["cancel_task_ids"] == [task_id]
