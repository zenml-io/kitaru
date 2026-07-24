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
"""End-to-end trace import jobs against PostgreSQL."""

import asyncio
import json
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import db_settings, lifespan_client


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide the full app with a fast import worker poll."""
    async with lifespan_client(db_settings(IMPORT_WORKER_POLL_SECONDS=0.01)) as client:
        yield client


async def create_version(client: httpx.AsyncClient) -> str:
    """Create one agent version through the public API."""
    agent_response = await client.post("/v1/agents", json={"name": "support-bot"})
    assert agent_response.status_code == 201
    version_response = await client.post(
        f"/v1/agents/{agent_response.json()['id']}/versions",
        json={"version": "v1"},
    )
    assert version_response.status_code == 201
    return version_response.json()["id"]


def export(output: str) -> bytes:
    """Build one observation-first Langfuse export."""
    return (
        json.dumps(
            {
                "id": "root",
                "traceId": "trace-1",
                "sessionId": "conversation-1",
                "projectId": "project-1",
                "type": "SPAN",
                "name": "agent",
                "input": {"message": "hello"},
                "output": {"answer": output},
                "startTime": "2026-07-24T10:00:00Z",
                "endTime": "2026-07-24T10:00:01Z",
            }
        )
        + "\n"
    ).encode()


async def upload(client: httpx.AsyncClient, version_id: str, content: bytes) -> dict:
    """Create an import job and wait for its terminal status."""
    response = await client.post(
        "/v1/import-jobs",
        data={
            "importer_id": "langfuse",
            "agent_version_id": version_id,
        },
        files={"file": ("traces.jsonl", content, "application/x-ndjson")},
    )
    assert response.status_code == 201
    job_id = response.json()["id"]
    for _ in range(200):
        response = await client.get(f"/v1/import-jobs/{job_id}")
        assert response.status_code == 200
        job = response.json()
        if job["status"] not in {"pending", "running"}:
            return job
        await asyncio.sleep(0.01)
    pytest.fail(f"Import job {job_id} did not finish")


async def test_worker_imports_deduplicates_and_revises(
    client: httpx.AsyncClient,
) -> None:
    """Run the persisted job from upload through immutable session revisions."""
    version_id = await create_version(client)

    first_job = await upload(client, version_id, export("hi"))
    duplicate_job = await upload(client, version_id, export("hi"))
    changed_job = await upload(client, version_id, export("hello"))

    assert first_job["status"] == "completed"
    assert first_job["imported_count"] == 1
    assert first_job["deduplicated_count"] == 0
    assert first_job["failed_count"] == 0
    assert duplicate_job["status"] == "completed"
    assert duplicate_job["imported_count"] == 0
    assert duplicate_job["deduplicated_count"] == 1
    assert duplicate_job["session_ids"] == first_job["session_ids"]
    assert changed_job["status"] == "completed"
    assert changed_job["imported_count"] == 1

    response = await client.get(
        "/v1/sessions",
        params={
            "provider": "langfuse",
            "external_id": "conversation-1",
        },
    )
    assert response.status_code == 200
    sessions = sorted(
        response.json()["items"], key=lambda session: session["source_revision"]
    )
    assert [session["source_revision"] for session in sessions] == [1, 2]
    assert sessions[0]["source_instance"] == "project-1"
    assert sessions[0]["agent_version_id"] == version_id
    assert sessions[0]["inputs"] == {
        "schema_version": 1,
        "turns": [
            {
                "source_trace_id": "trace-1",
                "inputs": {"message": "hello"},
            }
        ],
    }
    assert sessions[0]["replay_readiness"]["level"] == "ready"
    assert sessions[1]["supersedes_session_id"] == sessions[0]["id"]

    response = await client.get(
        f"/v1/sessions/{sessions[1]['id']}/nodes",
        params={"include_payloads": True},
    )
    assert response.status_code == 200
    nodes = response.json()
    assert nodes[0]["external_id"] == "trace-1:root"
    assert nodes[0]["outputs"] == {"answer": "hello"}


async def test_worker_keeps_valid_sessions_when_one_group_fails(
    client: httpx.AsyncClient,
) -> None:
    """Complete with errors after committing each valid source session."""
    version_id = await create_version(client)
    valid = json.loads(export("hi"))
    invalid = {
        **valid,
        "id": "other-root",
        "traceId": "other-trace",
        "sessionId": "invalid-conversation",
    }
    invalid.pop("projectId")
    content = (json.dumps(valid) + "\n" + json.dumps(invalid) + "\n").encode()

    job = await upload(client, version_id, content)

    assert job["status"] == "completed_with_errors"
    assert job["source_session_count"] == 2
    assert job["imported_count"] == 1
    assert job["failed_count"] == 1
    assert job["errors"][0]["source_id"] == "invalid-conversation"
    response = await client.get(
        "/v1/sessions",
        params={"provider": "langfuse"},
    )
    assert response.status_code == 200
    assert response.json()["total"] == 1


async def test_worker_fails_malformed_file(client: httpx.AsyncClient) -> None:
    """Fail the whole job when JSONL cannot be parsed."""
    version_id = await create_version(client)

    job = await upload(client, version_id, b"{not-json}\n")

    assert job["status"] == "failed"
    assert job["imported_count"] == 0
    assert job["error"] == "Line 1 is not valid JSON"
