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

import asyncio
import uuid
from collections.abc import AsyncGenerator
from typing import Any

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from conftest import db_settings, lifespan_client
from kitaru.server.api import agent_deletion
from kitaru.server.database.service import DatabaseService


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


async def test_delete_cascades_versions(client: httpx.AsyncClient) -> None:
    """Cascade an agent's versions when the agent is deleted."""
    agent = (await client.post("/api/v1/agents", json={"name": "assistant"})).json()
    version = (
        await client.post(f"/api/v1/agents/{agent['id']}/versions", json={})
    ).json()

    response = await client.delete(f"/api/v1/agents/{agent['id']}")
    assert response.status_code == 204

    response = await client.get(f"/api/v1/agent-versions/{version['id']}")
    assert response.status_code == 404


async def test_delete_missing_agent(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 when deleting an agent that does not exist."""
    response = await client.delete(f"/api/v1/agents/{uuid.uuid4()}")
    assert response.status_code == 404


async def _upload_blob(client: httpx.AsyncClient, name: str, content: bytes) -> str:
    """Store a blob, returning its id."""
    response = await client.post(
        "/api/v1/blobs", files={"file": (name, content, "text/plain")}
    )
    return response.json()["id"]


async def _setup_agent_subtree(client: httpx.AsyncClient) -> dict[str, Any]:
    """Build an agent with a version, a subtree, and an unrelated task.

    Covers every phase of agent deletion: an experiment with a run whose
    replay pins a cohort version and a session, a cohort, an investigation,
    five sessions (more than the batch size the test shrinks deletion to),
    a secret bound to a version, and tags linked to a session and a version.

    Returns:
        Ids needed to drive the deletion and assert its aftermath.
    """
    agent = (await client.post("/api/v1/agents", json={"name": "assistant"})).json()

    secret = (
        await client.post(
            "/api/v1/secrets", json={"name": "run-secret", "values": {"k": "v"}}
        )
    ).json()
    version = (
        await client.post(
            f"/api/v1/agents/{agent['id']}/versions",
            json={
                "run_spec": {
                    "command": "run.sh",
                    "timeout_seconds": 60,
                    "secret_ids": [secret["id"]],
                }
            },
        )
    ).json()
    other_version = (
        await client.post(f"/api/v1/agents/{agent['id']}/versions", json={})
    ).json()

    session_ids = []
    for _ in range(5):
        session = (
            await client.post(
                "/api/v1/sessions",
                json={
                    "agent_id": agent["id"],
                    "agent_version_id": version["id"],
                    "origin": "recorded",
                    "inputs": {"q": "hi"},
                    "outputs": None,
                },
            )
        ).json()
        session_ids.append(session["id"])

    await client.post(
        f"/api/v1/sessions/{session_ids[0]}/nodes",
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

    session_tag = (await client.post("/api/v1/tags", json={"name": "flagged"})).json()
    await client.post(
        f"/api/v1/tags/{session_tag['id']}/links",
        json={"resource_type": "session", "resource_id": session_ids[0]},
    )
    version_tag = (await client.post("/api/v1/tags", json={"name": "stable"})).json()
    await client.post(
        f"/api/v1/tags/{version_tag['id']}/links",
        json={"resource_type": "agent_version", "resource_id": version["id"]},
    )

    cohort = (
        await client.post(
            "/api/v1/cohorts", json={"name": "cohort-1", "agent_id": agent["id"]}
        )
    ).json()
    cohort_version = (
        await client.post(
            f"/api/v1/cohorts/{cohort['id']}/versions",
            json={"add_session_ids": [session_ids[0]]},
        )
    ).json()

    investigation = (
        await client.post(
            "/api/v1/investigations",
            json={
                "agent_id": agent["id"],
                "name": "payment-failures",
                "sessions": [
                    {
                        "session_id": session_ids[1],
                        "questions": [{"key": "cause", "question": "What happened?"}],
                    }
                ],
            },
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
                "agent_id": agent["id"],
                "evaluators": [{"evaluator": "accuracy"}],
            },
        )
    ).json()
    await client.post(
        f"/api/v1/experiments/{experiment['id']}/runs",
        json={
            "cohort_version_id": cohort_version["id"],
            "agent_version_id": version["id"],
        },
    )

    payload_blob_id = await _upload_blob(client, "payload.json", b"[]")
    importer_script_blob_id = await _upload_blob(
        client, "importer.py", b"def run(): pass"
    )
    importer = (
        await client.post("/api/v1/importers", json={"name": "importer-1"})
    ).json()
    await client.post(
        f"/api/v1/importers/{importer['id']}/versions",
        json={
            "source": {
                "type": "script",
                "blob_id": importer_script_blob_id,
                "entrypoint": "run",
            }
        },
    )
    job = (
        await client.post(
            "/api/v1/imports",
            json={
                "importer": "importer-1",
                "agent_id": agent["id"],
                "payload_blob_id": payload_blob_id,
            },
        )
    ).json()
    tasks = (await client.get(f"/api/v1/jobs/{job['id']}/tasks")).json()["items"]
    assert tasks[0]["agent_id"] == agent["id"]

    return {
        "agent_id": agent["id"],
        "version_ids": [version["id"], other_version["id"]],
        "session_ids": session_ids,
        "secret_id": secret["id"],
        "session_tag_id": session_tag["id"],
        "version_tag_id": version_tag["id"],
        "cohort_id": cohort["id"],
        "investigation_id": investigation["id"],
        "experiment_id": experiment["id"],
        "task_id": tasks[0]["id"],
    }


async def test_delete_agent_cascades_its_full_subtree(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Delete an agent whose subtree spans every deletion phase.

    Shrinking the batch size below the session count forces the session
    phase to loop over more than one page.
    """
    monkeypatch.setattr(agent_deletion, "DELETE_BATCH_SIZE", 2)
    settings = db_settings()
    async with lifespan_client(settings) as client:
        setup = await _setup_agent_subtree(client)

        response = await client.delete(f"/api/v1/agents/{setup['agent_id']}")
        assert response.status_code == 204

        assert (
            await client.get(f"/api/v1/agents/{setup['agent_id']}")
        ).status_code == 404
        for session_id in setup["session_ids"]:
            assert (
                await client.get(f"/api/v1/sessions/{session_id}")
            ).status_code == 404
        for version_id in setup["version_ids"]:
            assert (
                await client.get(f"/api/v1/agent-versions/{version_id}")
            ).status_code == 404
        assert (
            await client.get(f"/api/v1/cohorts/{setup['cohort_id']}")
        ).status_code == 404
        assert (
            await client.get(f"/api/v1/investigations/{setup['investigation_id']}")
        ).status_code == 404
        assert (
            await client.get(f"/api/v1/experiments/{setup['experiment_id']}")
        ).status_code == 404

        # Secrets are account-level. Only the version binding cascades.
        assert (
            await client.get(f"/api/v1/secrets/{setup['secret_id']}")
        ).status_code == 200

        tags = (await client.get("/api/v1/tags")).json()["items"]
        tag_ids = {tag["id"] for tag in tags}
        assert setup["session_tag_id"] in tag_ids
        assert setup["version_tag_id"] in tag_ids
        assert (
            await client.delete(
                f"/api/v1/tags/{setup['session_tag_id']}/links/session/"
                f"{setup['session_ids'][0]}"
            )
        ).status_code == 404
        assert (
            await client.delete(
                f"/api/v1/tags/{setup['version_tag_id']}/links/agent_version/"
                f"{setup['version_ids'][0]}"
            )
        ).status_code == 404

        engine = create_async_engine(DatabaseService.generate_database_uri(settings))
        try:
            async with engine.connect() as connection:
                row = (
                    await connection.execute(
                        text("SELECT id FROM task WHERE id = :id"),
                        {"id": setup["task_id"]},
                    )
                ).one_or_none()
                assert row is None
        finally:
            await engine.dispose()


async def test_concurrent_deletes_of_one_agent_both_succeed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Race two deletes of the same agent, each skipping rows the other took."""
    monkeypatch.setattr(agent_deletion, "DELETE_BATCH_SIZE", 2)
    settings = db_settings(DB_POOL_SIZE=2, DB_MAX_OVERFLOW=0)
    async with lifespan_client(settings) as client:
        setup = await _setup_agent_subtree(client)

        first, second = await asyncio.gather(
            client.delete(f"/api/v1/agents/{setup['agent_id']}"),
            client.delete(f"/api/v1/agents/{setup['agent_id']}"),
        )
        assert first.status_code == 204, first.text
        assert second.status_code == 204, second.text
        assert (
            await client.get(f"/api/v1/agents/{setup['agent_id']}")
        ).status_code == 404
        for session_id in setup["session_ids"]:
            assert (
                await client.get(f"/api/v1/sessions/{session_id}")
            ).status_code == 404


async def test_delete_agent_removes_standalone_replays(
    client: httpx.AsyncClient,
) -> None:
    """Delete an agent whose session is the baseline of a standalone replay."""
    setup = await _setup_agent_subtree(client)
    response = await client.post(
        "/api/v1/replays",
        json={
            "baseline_session_id": setup["session_ids"][2],
            "evaluators": [{"evaluator": "accuracy"}],
        },
    )
    assert response.status_code == 201, response.text
    job_id = response.json()["job_id"]

    response = await client.delete(f"/api/v1/agents/{setup['agent_id']}")
    assert response.status_code == 204, response.text
    assert (await client.get(f"/api/v1/agents/{setup['agent_id']}")).status_code == 404
    assert (await client.get(f"/api/v1/jobs/{job_id}")).status_code == 404
    assert (await client.get("/api/v1/replays")).json()["items"] == []
