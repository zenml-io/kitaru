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
"""End-to-end import tests against PostgreSQL."""

from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import db_settings, lifespan_client

CODE = b"def parse(payload):\n    return []\n"
PAYLOAD = b'{"external_id": "abc"}\n'


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app running its full lifespan."""
    # Under the none auth scheme every request runs as the account
    # bootstrapped at startup, which owns all created jobs.
    async with lifespan_client(db_settings()) as client:
        yield client


async def seed_importer(client: httpx.AsyncClient) -> None:
    """Store an importer with one code version.

    Args:
        client: HTTP client for the app.
    """
    blob_id = (
        await client.post(
            "/v1/blobs", files={"file": ("importer.py", CODE, "text/x-python")}
        )
    ).json()["id"]
    importer_id = (
        await client.post(
            "/v1/importers", json={"name": "langfuse", "provider": "langfuse"}
        )
    ).json()["id"]
    response = await client.post(
        f"/v1/importers/{importer_id}/versions",
        json={"blob_id": blob_id, "entrypoint": "parse"},
    )
    assert response.status_code == 201


async def test_import_flow_persists_across_requests(client: httpx.AsyncClient) -> None:
    """Create an import, claim it, report stats, and complete it."""
    await seed_importer(client)
    agent_id = (await client.post("/v1/agents", json={"name": "support-bot"})).json()[
        "id"
    ]
    payload_blob_id = (
        await client.post(
            "/v1/blobs",
            files={"file": ("payload.jsonl", PAYLOAD, "application/jsonl")},
        )
    ).json()["id"]

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
    created = response.json()
    assert created["kind"] == "import"
    assert created["payload_blob_id"] == payload_blob_id
    assert created["agent_id"] == agent_id
    assert created["inputs"] == {"project": "demo"}

    response = await client.get(f"/v1/jobs/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created

    worker_id = (await client.post("/v1/workers", json={"name": "runner-1"})).json()[
        "id"
    ]
    response = await client.post(
        "/v1/jobs/claim", json={"worker_id": worker_id, "max_jobs": 5}
    )
    assert response.status_code == 200
    assert [entry["job"]["id"] for entry in response.json()["jobs"]] == [created["id"]]

    response = await client.get(f"/v1/jobs/{created['id']}/spec")
    assert response.status_code == 200
    spec = response.json()
    assert spec["importer"]["plugin"]["entrypoint"] == "parse"
    assert spec["importer"]["payload"]["blob_id"] == payload_blob_id
    assert spec["importer"]["provider"] == "langfuse"
    assert spec["importer"]["agent_id"] == agent_id
    assert spec["importer"]["params"] == {"project": "demo"}

    response = await client.patch(
        f"/v1/jobs/{created['id']}", json={"status": "running"}
    )
    assert response.status_code == 200
    stats = {"created": 1, "skipped": 0, "failed": 0, "failures": []}
    response = await client.patch(f"/v1/jobs/{created['id']}", json={"stats": stats})
    assert response.status_code == 200
    response = await client.patch(
        f"/v1/jobs/{created['id']}", json={"status": "completed"}
    )
    assert response.status_code == 200

    response = await client.get(f"/v1/jobs/{created['id']}")
    assert response.json()["status"] == "completed"
    assert response.json()["stats"] == stats


async def test_import_rejects_unknown_payload(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a payload blob that does not exist."""
    await seed_importer(client)
    agent_id = (await client.post("/v1/agents", json={"name": "support-bot"})).json()[
        "id"
    ]
    response = await client.post(
        "/v1/imports",
        json={
            "importer": "langfuse",
            "agent_id": agent_id,
            "payload_blob_id": "00000000-0000-0000-0000-000000000000",
        },
    )
    assert response.status_code == 404
