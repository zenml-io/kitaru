"""End-to-end v2 task execution against migrated PostgreSQL."""

from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import db_settings, lifespan_client


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an unauthenticated trusted-team client on a fresh database."""
    async with lifespan_client(db_settings()) as client:
        yield client


async def test_agent_task_runs_to_job_completion(
    client: httpx.AsyncClient,
) -> None:
    """Persist and settle an agent job across separate HTTP requests."""
    response = await client.post(
        "/v1/agents",
        json={"name": "weather-agent", "description": "Test agent"},
    )
    assert response.status_code == 201, response.text
    agent = response.json()

    response = await client.post(
        f"/v1/agents/{agent['id']}/versions",
        json={
            "display_version": "test",
            "run_spec": {
                "command": "python agent.py",
                "timeout_seconds": 30,
            },
            "capabilities": {"tools": ["weather"]},
        },
    )
    assert response.status_code == 201, response.text
    version = response.json()

    response = await client.post(
        "/v1/session-runs",
        json={
            "agent_version_id": version["id"],
            "inputs": {"city": "Amsterdam"},
            "name": "weather-run",
        },
    )
    assert response.status_code == 201, response.text
    job = response.json()

    response = await client.post(
        "/v1/workers",
        json={
            "name": "test-worker",
            "scope": {"kinds": ["agent"]},
            "runtime": {"platform": "test"},
        },
    )
    assert response.status_code == 200, response.text
    worker = response.json()

    response = await client.post(
        "/v1/tasks/claim",
        json={"worker_id": worker["id"], "max_tasks": 1},
    )
    assert response.status_code == 200, response.text
    claimed = response.json()["tasks"]
    assert len(claimed) == 1
    task = claimed[0]["task"]
    assert claimed[0]["spec"]["details"]["inputs"] == {"city": "Amsterdam"}

    response = await client.patch(
        f"/v1/tasks/{task['id']}",
        json={"status": "running", "attempt": task["attempt"]},
    )
    assert response.status_code == 200, response.text

    response = await client.post(
        "/v1/sessions",
        json={
            "agent_id": agent["id"],
            "agent_version_id": version["id"],
            "task_id": task["id"],
            "origin": "recorded",
            "inputs": {"city": "Amsterdam"},
        },
    )
    assert response.status_code == 201, response.text
    session = response.json()

    response = await client.patch(
        f"/v1/sessions/{session['id']}",
        json={
            "status": "completed",
            "outputs": {"temperature": 18},
        },
    )
    assert response.status_code == 200, response.text

    response = await client.patch(
        f"/v1/tasks/{task['id']}",
        json={"status": "completed", "attempt": task["attempt"]},
    )
    assert response.status_code == 200, response.text
    assert response.json()["result_session_id"] == session["id"]

    response = await client.get(f"/v1/jobs/{job['id']}")
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "completed"
