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
"""End-to-end blob tests against PostgreSQL."""

import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import db_settings, lifespan_client


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app running its full lifespan."""
    async with lifespan_client(db_settings()) as client:
        yield client


async def test_blobs_persist_across_requests(client: httpx.AsyncClient) -> None:
    """Prove the per-request commit through separate requests."""
    response = await client.post(
        "/api/v1/blobs", files={"file": ("a.txt", b"hello", "text/plain")}
    )
    assert response.status_code == 201
    created = response.json()

    response = await client.get(f"/api/v1/blobs/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created

    response = await client.get(f"/api/v1/blobs/{created['id']}/content")
    assert response.status_code == 200
    assert response.content == b"hello"


async def test_dedup_conflict_returns_stored_row(client: httpx.AsyncClient) -> None:
    """Return the stored blob with HTTP 200 on a dedup hit."""
    first = await client.post(
        "/api/v1/blobs", files={"file": ("a.txt", b"same", "text/plain")}
    )
    assert first.status_code == 201
    second = await client.post(
        "/api/v1/blobs", files={"file": ("b.txt", b"same", "text/plain")}
    )
    assert second.status_code == 200
    assert second.json()["id"] == first.json()["id"]


async def test_delete_persists_across_requests(client: httpx.AsyncClient) -> None:
    """Persist a deletion across requests."""
    created = (
        await client.post(
            "/api/v1/blobs", files={"file": ("a.txt", b"hello", "text/plain")}
        )
    ).json()
    response = await client.delete(f"/api/v1/blobs/{created['id']}")
    assert response.status_code == 204

    response = await client.get(f"/api/v1/blobs/{created['id']}")
    assert response.status_code == 404


async def test_delete_in_use_conflict(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 when a script version references the blob."""
    blob = (
        await client.post(
            "/api/v1/blobs", files={"file": ("a.txt", b"hello", "text/plain")}
        )
    ).json()
    response = await client.post(
        "/api/v1/evaluators", json={"name": f"scorer-{uuid.uuid4().hex[:8]}"}
    )
    assert response.status_code == 201
    evaluator_id = response.json()["id"]
    response = await client.post(
        f"/api/v1/evaluators/{evaluator_id}/versions",
        json={
            "source": {"type": "script", "blob_id": blob["id"], "entrypoint": "score"}
        },
    )
    assert response.status_code == 201

    response = await client.delete(f"/api/v1/blobs/{blob['id']}")
    assert response.status_code == 409
