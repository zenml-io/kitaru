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
"""End-to-end scorer tests against PostgreSQL."""

from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import db_settings, lifespan_client

CONTENT = b"def score(session):\n    return 1.0\n"


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app running its full lifespan."""
    # Under the none auth scheme every request runs as the account
    # bootstrapped at startup, which owns all created scorers.
    async with lifespan_client(db_settings()) as client:
        yield client


async def upload_blob(client: httpx.AsyncClient, content: bytes = CONTENT) -> str:
    """Upload a code blob through the API.

    Args:
        client: HTTP client routed to the app.
        content: Content to upload.

    Returns:
        Id of the stored blob.
    """
    response = await client.post(
        "/v1/blobs", files={"file": ("scorer.py", content, "text/x-python")}
    )
    assert response.status_code in {200, 201}
    blob_id: str = response.json()["id"]
    return blob_id


async def test_scorers_persist_across_requests(client: httpx.AsyncClient) -> None:
    """Prove the per-request commit through separate requests."""
    response = await client.post("/v1/scorers", json={"name": "relevance"})
    assert response.status_code == 201
    created = response.json()

    response = await client.get(f"/v1/scorers/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created

    response = await client.get("/v1/scorers")
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["items"][0] == created


async def test_duplicate_name_conflict(client: httpx.AsyncClient) -> None:
    """Translate the database constraint into HTTP 409."""
    response = await client.post("/v1/scorers", json={"name": "relevance"})
    assert response.status_code == 201
    response = await client.post("/v1/scorers", json={"name": "relevance"})
    assert response.status_code == 409
    assert response.json() == {
        "detail": "Plugin name 'relevance' is already registered"
    }


async def test_versions_persist_across_requests(client: httpx.AsyncClient) -> None:
    """Allocate version numbers across separate requests."""
    blob_id = await upload_blob(client)
    created = (await client.post("/v1/scorers", json={"name": "relevance"})).json()

    first = await client.post(
        f"/v1/scorers/{created['id']}/versions",
        json={"blob_id": blob_id, "entrypoint": "score"},
    )
    assert first.status_code == 201
    assert first.json()["version"] == 1

    second = await client.post(
        f"/v1/scorers/{created['id']}/versions",
        json={"blob_id": blob_id, "entrypoint": "score"},
    )
    assert second.status_code == 201
    assert second.json()["version"] == 2

    response = await client.get(f"/v1/scorers/{created['id']}")
    assert response.json()["latest_version"] == 2

    response = await client.get(f"/v1/scorers/{created['id']}/versions/2")
    assert response.status_code == 200
    assert response.json() == second.json()


async def test_delete_cascades_versions(client: httpx.AsyncClient) -> None:
    """Delete the versions of a deleted scorer."""
    blob_id = await upload_blob(client)
    created = (await client.post("/v1/scorers", json={"name": "relevance"})).json()
    await client.post(
        f"/v1/scorers/{created['id']}/versions",
        json={"blob_id": blob_id, "entrypoint": "score"},
    )

    response = await client.delete(f"/v1/scorers/{created['id']}")
    assert response.status_code == 204

    response = await client.get(f"/v1/scorers/{created['id']}/versions")
    assert response.status_code == 404
