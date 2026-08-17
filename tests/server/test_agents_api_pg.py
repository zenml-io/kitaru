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
"""End-to-end agent tests against PostgreSQL."""

from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import db_settings, lifespan_client


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app running its full lifespan."""
    # Under the none auth scheme every request runs as the account
    # bootstrapped at startup, which owns all created agents.
    async with lifespan_client(db_settings()) as client:
        yield client


async def test_agents_persist_across_requests(client: httpx.AsyncClient) -> None:
    """Prove the per-request commit through separate requests."""
    response = await client.post(
        "/api/v1/agents", json={"name": "assistant", "description": "Helps"}
    )
    assert response.status_code == 201
    created = response.json()

    response = await client.get(f"/api/v1/agents/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created

    response = await client.get("/api/v1/agents")
    assert response.status_code == 200
    body = response.json()
    assert body["next_cursor"] is None
    assert body["items"][0] == created


async def test_duplicate_name_conflict(client: httpx.AsyncClient) -> None:
    """Translate the database constraint into HTTP 409."""
    response = await client.post("/api/v1/agents", json={"name": "assistant"})
    assert response.status_code == 201
    response = await client.post("/api/v1/agents", json={"name": "assistant"})
    assert response.status_code == 409
    assert response.json() == {"detail": "Agent name 'assistant' is already registered"}


async def test_update_persists_across_requests(client: httpx.AsyncClient) -> None:
    """Persist an update across requests."""
    created = (await client.post("/api/v1/agents", json={"name": "assistant"})).json()
    response = await client.patch(
        f"/api/v1/agents/{created['id']}", json={"description": "Reviews"}
    )
    assert response.status_code == 200

    response = await client.get(f"/api/v1/agents/{created['id']}")
    assert response.status_code == 200
    body = response.json()
    assert body["description"] == "Reviews"
    assert body["updated"] > created["updated"]


async def test_delete_persists_across_requests(client: httpx.AsyncClient) -> None:
    """Persist a deletion across requests."""
    created = (await client.post("/api/v1/agents", json={"name": "assistant"})).json()
    response = await client.delete(f"/api/v1/agents/{created['id']}")
    assert response.status_code == 204

    response = await client.get(f"/api/v1/agents/{created['id']}")
    assert response.status_code == 404


async def test_create_version_persists_across_requests(
    client: httpx.AsyncClient,
) -> None:
    """Prove a created version is visible from a separate request."""
    agent = (await client.post("/api/v1/agents", json={"name": "assistant"})).json()
    response = await client.post(
        f"/api/v1/agents/{agent['id']}/versions",
        json={"display_version": "v1", "description": "First cut"},
    )
    assert response.status_code == 201
    created = response.json()
    assert created["version"] == 1

    response = await client.get(f"/api/v1/agent-versions/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created

    response = await client.get(f"/api/v1/agents/{agent['id']}")
    assert response.status_code == 200
    assert response.json()["latest_version"] == 1


async def test_version_numbering_sequence(client: httpx.AsyncClient) -> None:
    """Assign consecutive version numbers per agent across requests."""
    agent = (await client.post("/api/v1/agents", json={"name": "assistant"})).json()
    first = (
        await client.post(f"/api/v1/agents/{agent['id']}/versions", json={})
    ).json()
    second = (
        await client.post(f"/api/v1/agents/{agent['id']}/versions", json={})
    ).json()
    assert first["version"] == 1
    assert second["version"] == 2


async def test_create_version_with_secrets_round_trips(
    client: httpx.AsyncClient,
) -> None:
    """Round-trip a run spec whose secret ids reference real secrets."""
    agent = (await client.post("/api/v1/agents", json={"name": "assistant"})).json()
    secret_a = (
        await client.post(
            "/api/v1/secrets", json={"name": "secret-a", "values": {"k": "v"}}
        )
    ).json()
    secret_b = (
        await client.post(
            "/api/v1/secrets", json={"name": "secret-b", "values": {"k": "v"}}
        )
    ).json()

    response = await client.post(
        f"/api/v1/agents/{agent['id']}/versions",
        json={
            "run_spec": {
                "command": "run.sh",
                "secret_ids": [secret_a["id"], secret_b["id"]],
            }
        },
    )
    assert response.status_code == 201
    created = response.json()

    response = await client.get(f"/api/v1/agent-versions/{created['id']}")
    assert response.status_code == 200
    assert response.json()["run_spec"]["secret_ids"] == [
        secret_a["id"],
        secret_b["id"],
    ]


async def test_delete_agent_cascades_related_resources(
    client: httpx.AsyncClient,
) -> None:
    """Delete an agent together with everything that references it."""
    agent = (await client.post("/api/v1/agents", json={"name": "assistant"})).json()
    agent_id = agent["id"]
    version = (
        await client.post(
            f"/api/v1/agents/{agent_id}/versions",
            json={"run_spec": {"command": "run.sh", "timeout_seconds": 60}},
        )
    ).json()
    session = (
        await client.post(
            "/api/v1/sessions",
            json={
                "agent_id": agent_id,
                "agent_version_id": version["id"],
                "origin": "recorded",
                "inputs": {"q": "hi"},
                "outputs": None,
            },
        )
    ).json()
    cohort = (
        await client.post("/api/v1/cohorts", json={"name": "c1", "agent_id": agent_id})
    ).json()
    cohort_version = (
        await client.post(
            f"/api/v1/cohorts/{cohort['id']}/versions",
            json={"add_session_ids": [session["id"]]},
        )
    ).json()
    blob = (
        await client.post(
            "/api/v1/blobs",
            files={"file": ("score.py", b"def score(): pass", "text/plain")},
        )
    ).json()
    evaluator = (
        await client.post(
            "/api/v1/evaluators", json={"name": "accuracy", "metadata": {}}
        )
    ).json()
    await client.post(
        f"/api/v1/evaluators/{evaluator['id']}/versions",
        json={
            "source": {"type": "script", "blob_id": blob["id"], "entrypoint": "score"}
        },
    )
    experiment = (
        await client.post(
            "/api/v1/experiments",
            json={
                "name": "exp1",
                "agent_id": agent_id,
                "evaluators": [{"evaluator": "accuracy"}],
            },
        )
    ).json()
    experiment_run = (
        await client.post(
            f"/api/v1/experiments/{experiment['id']}/runs",
            json={
                "cohort_version_id": cohort_version["id"],
                "agent_version_id": version["id"],
            },
        )
    ).json()
    investigation = (
        await client.post(
            "/api/v1/investigations",
            json={
                "agent_id": agent_id,
                "name": "inv1",
                "sessions": [
                    {
                        "session_id": session["id"],
                        "questions": [{"key": "cause", "question": "Why?"}],
                    }
                ],
            },
        )
    ).json()
    replay = (
        await client.post(
            "/api/v1/replays",
            json={
                "baseline_session_id": session["id"],
                "evaluators": [{"evaluator": "accuracy"}],
            },
        )
    ).json()
    # Drive the ad hoc replay to its evaluator task so a task referencing
    # the agent only through its input session exists.
    registration = (
        await client.post(
            "/api/v1/workers",
            json={
                "name": "worker-1",
                "scope": {"claims": [{"kind": "agent"}, {"kind": "evaluator"}]},
                "runtime": {"platform": "bare"},
                "metadata": {},
            },
        )
    ).json()
    worker_headers = {"Authorization": f"Bearer {registration['token']}"}
    claimed = (
        await client.post(
            "/api/v1/tasks/claim", json={"max_tasks": 10}, headers=worker_headers
        )
    ).json()
    agent_entry = next(
        entry
        for entry in claimed["tasks"]
        if entry["task"]["kind"] == "agent"
        and entry["task"]["job_id"] == replay["job_id"]
    )
    agent_task_headers = {"Authorization": f"Bearer {agent_entry['token']}"}
    await client.patch(
        f"/api/v1/tasks/{agent_entry['task']['id']}",
        json={"status": "running"},
        headers=agent_task_headers,
    )
    result_session = (
        await client.post(
            "/api/v1/sessions",
            json={"origin": "replay", "inputs": None, "outputs": None},
            headers=agent_task_headers,
        )
    ).json()
    await client.patch(
        f"/api/v1/sessions/{result_session['id']}",
        json={"status": "completed", "outputs": {}},
    )
    await client.patch(
        f"/api/v1/tasks/{agent_entry['task']['id']}",
        json={"status": "completed"},
        headers=agent_task_headers,
    )
    claimed = (
        await client.post(
            "/api/v1/tasks/claim", json={"max_tasks": 10}, headers=worker_headers
        )
    ).json()
    assert any(entry["task"]["kind"] == "evaluator" for entry in claimed["tasks"])

    response = await client.delete(f"/api/v1/agents/{agent_id}")
    assert response.status_code == 204

    for path in (
        f"/api/v1/agents/{agent_id}",
        f"/api/v1/agent-versions/{version['id']}",
        f"/api/v1/sessions/{session['id']}",
        f"/api/v1/sessions/{result_session['id']}",
        f"/api/v1/cohorts/{cohort['id']}",
        f"/api/v1/cohort-versions/{cohort_version['id']}",
        f"/api/v1/experiments/{experiment['id']}",
        f"/api/v1/experiment-runs/{experiment_run['id']}",
        f"/api/v1/investigations/{investigation['id']}",
        f"/api/v1/replays/{replay['id']}",
    ):
        response = await client.get(path)
        assert response.status_code == 404, path
