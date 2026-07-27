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
"""Tests for the import routes."""

import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import experiment_app

PAYLOAD = b'{"external_id": "abc"}\n'
CODE = b"def parse(payload):\n    return []\n"


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with fake-backed services."""
    transport = httpx.ASGITransport(app=experiment_app())
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def upload_blob(
    client: httpx.AsyncClient, name: str, content: bytes, media_type: str
) -> str:
    """Upload a blob through the API.

    Args:
        client: HTTP client for the app.
        name: File name of the upload.
        content: File content.
        media_type: Media type of the content.

    Returns:
        Id of the created blob.
    """
    response = await client.post(
        "/v1/blobs", files={"file": (name, content, media_type)}
    )
    assert response.status_code == 201
    blob_id: str = response.json()["id"]
    return blob_id


async def create_agent(client: httpx.AsyncClient, name: str = "support-bot") -> str:
    """Create an agent through the API.

    Args:
        client: HTTP client for the app.
        name: Agent name.

    Returns:
        Id of the created agent.
    """
    response = await client.post("/v1/agents", json={"name": name})
    assert response.status_code == 201
    agent_id: str = response.json()["id"]
    return agent_id


async def register_importer(
    client: httpx.AsyncClient,
    name: str = "langfuse",
    versions: int = 1,
    provider: str = "langfuse",
) -> str:
    """Register an importer with code versions through the API.

    Args:
        client: HTTP client for the app.
        name: Importer name.
        versions: Number of versions to register.
        provider: Provider the importer reads from.

    Returns:
        Id of the created importer.
    """
    response = await client.post(
        "/v1/importers", json={"name": name, "provider": provider}
    )
    assert response.status_code == 201
    importer_id: str = response.json()["id"]
    for index in range(versions):
        blob_id = await upload_blob(
            client, f"{name}{index}.py", CODE + bytes(index), "text/x-python"
        )
        response = await client.post(
            f"/v1/importers/{importer_id}/versions",
            json={"blob_id": blob_id, "entrypoint": "parse"},
        )
        assert response.status_code == 201
    return importer_id


async def test_create_import(client: httpx.AsyncClient) -> None:
    """Create an import and observe HTTP 201."""
    importer_id = await register_importer(client, versions=2)
    agent_id = await create_agent(client)
    payload_blob_id = await upload_blob(
        client, "payload.jsonl", PAYLOAD, "application/jsonl"
    )
    latest = (await client.get(f"/v1/importers/{importer_id}/versions/2")).json()
    response = await client.post(
        "/v1/imports",
        json={
            "importer": "langfuse",
            "agent_id": agent_id,
            "payload_blob_id": payload_blob_id,
            "params": {"project": "demo"},
        },
    )
    assert response.status_code == 201
    body = response.json()
    assert body["kind"] == "import"
    assert body["status"] == "pending"
    assert body["execution_target"] == "pool"
    assert body["agent_version_id"] is None
    assert body["agent_id"] == agent_id
    assert body["input_session_id"] is None
    assert body["plugin_version_id"] == latest["id"]
    assert body["payload_blob_id"] == payload_blob_id
    assert body["inputs"] == {"project": "demo"}
    assert body["stats"] is None
    assert uuid.UUID(body["id"])


async def test_create_import_pins_explicit_version(client: httpx.AsyncClient) -> None:
    """Pin the requested importer version."""
    importer_id = await register_importer(client, versions=2)
    agent_id = await create_agent(client)
    payload_blob_id = await upload_blob(
        client, "payload.jsonl", PAYLOAD, "application/jsonl"
    )
    first = (await client.get(f"/v1/importers/{importer_id}/versions/1")).json()
    response = await client.post(
        "/v1/imports",
        json={
            "importer": "langfuse",
            "agent_id": agent_id,
            "version": 1,
            "payload_blob_id": payload_blob_id,
        },
    )
    assert response.status_code == 201
    assert response.json()["plugin_version_id"] == first["id"]


async def test_create_import_unknown_importer(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unregistered importer name."""
    agent_id = await create_agent(client)
    payload_blob_id = await upload_blob(
        client, "payload.jsonl", PAYLOAD, "application/jsonl"
    )
    response = await client.post(
        "/v1/imports",
        json={
            "importer": "langfuse",
            "agent_id": agent_id,
            "payload_blob_id": payload_blob_id,
        },
    )
    assert response.status_code == 404
    assert response.json() == {
        "detail": "Plugin 'langfuse' of kind 'importer' was not found"
    }


async def test_create_import_unknown_version(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a version the importer does not have."""
    importer_id = await register_importer(client)
    agent_id = await create_agent(client)
    payload_blob_id = await upload_blob(
        client, "payload.jsonl", PAYLOAD, "application/jsonl"
    )
    response = await client.post(
        "/v1/imports",
        json={
            "importer": "langfuse",
            "agent_id": agent_id,
            "version": 4,
            "payload_blob_id": payload_blob_id,
        },
    )
    assert response.status_code == 404
    assert response.json() == {"detail": f"Plugin {importer_id} has no version 4"}


async def test_create_import_unknown_payload(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a payload blob that does not exist."""
    await register_importer(client)
    agent_id = await create_agent(client)
    missing = uuid.uuid4()
    response = await client.post(
        "/v1/imports",
        json={
            "importer": "langfuse",
            "agent_id": agent_id,
            "payload_blob_id": str(missing),
        },
    )
    assert response.status_code == 404
    assert response.json() == {"detail": f"Blob {missing} was not found"}


async def test_create_import_rejects_invalid_body(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for a missing payload blob id and an unknown field."""
    agent_id = await create_agent(client)
    response = await client.post(
        "/v1/imports", json={"importer": "langfuse", "agent_id": agent_id}
    )
    assert response.status_code == 422
    response = await client.post(
        "/v1/imports",
        json={
            "importer": "langfuse",
            "agent_id": agent_id,
            "payload_blob_id": str(uuid.uuid4()),
            "unknown": 1,
        },
    )
    assert response.status_code == 422


async def test_create_import_importer_without_provider(
    client: httpx.AsyncClient,
) -> None:
    """Observe HTTP 422 for an importer that carries no provider."""
    await client.post("/v1/importers", json={"name": "bare"})
    blob_id = await upload_blob(client, "bare.py", CODE, "text/x-python")
    importer_id = (await client.get("/v1/importers?name=bare")).json()["items"][0]["id"]
    await client.post(
        f"/v1/importers/{importer_id}/versions",
        json={"blob_id": blob_id, "entrypoint": "parse"},
    )
    agent_id = await create_agent(client)
    payload_blob_id = await upload_blob(
        client, "payload.jsonl", PAYLOAD, "application/jsonl"
    )
    response = await client.post(
        "/v1/imports",
        json={
            "importer": "bare",
            "agent_id": agent_id,
            "payload_blob_id": payload_blob_id,
        },
    )
    assert response.status_code == 422
    assert response.json() == {"detail": "Importer 'bare' carries no provider"}


async def test_create_import_unknown_agent(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an agent that does not exist."""
    await register_importer(client)
    payload_blob_id = await upload_blob(
        client, "payload.jsonl", PAYLOAD, "application/jsonl"
    )
    missing = uuid.uuid4()
    response = await client.post(
        "/v1/imports",
        json={
            "importer": "langfuse",
            "agent_id": str(missing),
            "payload_blob_id": payload_blob_id,
        },
    )
    assert response.status_code == 404
    assert response.json() == {"detail": f"Agent {missing} was not found"}


async def test_import_lifecycle_through_job_routes(client: httpx.AsyncClient) -> None:
    """Claim, run, report stats, and complete an import job."""
    await register_importer(client)
    agent_id = await create_agent(client)
    payload_blob_id = await upload_blob(
        client, "payload.jsonl", PAYLOAD, "application/jsonl"
    )
    job_id = (
        await client.post(
            "/v1/imports",
            json={
                "importer": "langfuse",
                "agent_id": agent_id,
                "payload_blob_id": payload_blob_id,
            },
        )
    ).json()["id"]
    worker_id = (await client.post("/v1/workers", json={"name": "runner-1"})).json()[
        "id"
    ]

    response = await client.post(
        "/v1/jobs/claim", json={"worker_id": worker_id, "max_jobs": 5}
    )
    assert response.status_code == 200
    claimed = response.json()["jobs"]
    assert [entry["job"]["id"] for entry in claimed] == [job_id]
    assert claimed[0]["spec"]["importer"]["plugin"]["entrypoint"] == "parse"

    response = await client.patch(f"/v1/jobs/{job_id}", json={"status": "running"})
    assert response.status_code == 200

    response = await client.get(f"/v1/jobs/{job_id}/spec")
    assert response.status_code == 200
    spec = response.json()
    assert spec["kind"] == "import"
    assert spec["run"] is None
    assert spec["scorer"] is None
    assert spec["importer"]["plugin"]["entrypoint"] == "parse"
    assert spec["importer"]["payload"]["blob_id"] == payload_blob_id
    assert spec["importer"]["provider"] == "langfuse"
    assert spec["importer"]["agent_id"] == agent_id
    assert spec["importer"]["params"] == {}

    response = await client.patch(f"/v1/jobs/{job_id}", json={"status": "completed"})
    assert response.status_code == 409
    assert response.json() == {"detail": f"Job {job_id} has no stats"}

    stats = {
        "created": 2,
        "skipped": 1,
        "failed": 1,
        "failures": [{"line": 7, "external_id": "ext-7", "error": "bad line"}],
    }
    response = await client.patch(f"/v1/jobs/{job_id}", json={"stats": stats})
    assert response.status_code == 200
    assert response.json()["stats"] == stats
    assert response.json()["status"] == "running"

    response = await client.patch(f"/v1/jobs/{job_id}", json={"status": "completed"})
    assert response.status_code == 200
    assert response.json()["status"] == "completed"
    assert response.json()["stats"] == stats


async def test_list_jobs_filters_imports(client: httpx.AsyncClient) -> None:
    """Filter the job list down to import jobs."""
    await register_importer(client)
    agent_id = await create_agent(client)
    payload_blob_id = await upload_blob(
        client, "payload.jsonl", PAYLOAD, "application/jsonl"
    )
    job_id = (
        await client.post(
            "/v1/imports",
            json={
                "importer": "langfuse",
                "agent_id": agent_id,
                "payload_blob_id": payload_blob_id,
            },
        )
    ).json()["id"]
    response = await client.get("/v1/jobs", params={"kind": "import"})
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["items"][0]["id"] == job_id
