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
"""Tests for the blob routes."""

import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import FakeBlobRepository, create_blob
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    authorize_with_task,
    authorize_with_worker_or_task,
    get_blob_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.blob_service import BlobService
from kitaru.server.domain.account import Account

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


@pytest.fixture
def repository() -> FakeBlobRepository:
    """Provide the fake blob repository backing the app."""
    return FakeBlobRepository()


@pytest.fixture
async def client(
    repository: FakeBlobRepository,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with a blob service capped at 16 bytes."""
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )
    service = BlobService(repository=repository, max_size_bytes=16)
    app.dependency_overrides[get_blob_service] = lambda: service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    app.dependency_overrides[authorize_with_task] = lambda: AuthContext(account=ACCOUNT)
    app.dependency_overrides[authorize_with_worker_or_task] = lambda: AuthContext(
        account=ACCOUNT
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def test_upload_blob(client: httpx.AsyncClient) -> None:
    """Upload a new blob and observe HTTP 201."""
    response = await client.post(
        "/v1/blobs", files={"file": ("script.py", b"print(1)", "text/x-python")}
    )
    assert response.status_code == 201
    body = response.json()
    assert body["media_type"] == "text/x-python"
    assert body["size"] == len(b"print(1)")
    assert body["created"] is not None
    assert uuid.UUID(body["id"])


async def test_upload_blob_default_media_type(client: httpx.AsyncClient) -> None:
    """Default the media type when the upload carries none."""
    response = await client.post("/v1/blobs", files={"file": ("blob", b"data", "")})
    assert response.status_code == 201
    assert response.json()["media_type"] == "application/octet-stream"


async def test_upload_blob_dedup(client: httpx.AsyncClient) -> None:
    """Observe HTTP 200 with the same id on a dedup hit."""
    first = await client.post(
        "/v1/blobs", files={"file": ("a.txt", b"same", "text/plain")}
    )
    assert first.status_code == 201
    second = await client.post(
        "/v1/blobs", files={"file": ("b.txt", b"same", "text/plain")}
    )
    assert second.status_code == 200
    assert second.json()["id"] == first.json()["id"]
    assert second.json()["sha256"] == first.json()["sha256"]


async def test_upload_blob_too_large(client: httpx.AsyncClient) -> None:
    """Observe HTTP 413 for an upload exceeding the size cap."""
    response = await client.post(
        "/v1/blobs", files={"file": ("big.bin", b"x" * 17, "application/octet-stream")}
    )
    assert response.status_code == 413
    assert response.json() == {"detail": "Blob exceeds 16 bytes"}


async def test_get_blob(client: httpx.AsyncClient) -> None:
    """Get a blob's metadata by id."""
    created = (
        await client.post(
            "/v1/blobs", files={"file": ("a.txt", b"hello", "text/plain")}
        )
    ).json()
    response = await client.get(f"/v1/blobs/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created


async def test_get_blob_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown blob id."""
    missing_id = uuid.uuid4()
    response = await client.get(f"/v1/blobs/{missing_id}")
    assert response.status_code == 404
    assert response.json() == {"detail": f"Blob {missing_id} was not found"}


async def test_download_blob(client: httpx.AsyncClient) -> None:
    """Download a blob's content with attachment headers."""
    created = (
        await client.post(
            "/v1/blobs", files={"file": ("a.txt", b"hello", "text/plain")}
        )
    ).json()
    response = await client.get(f"/v1/blobs/{created['id']}/content")
    assert response.status_code == 200
    assert response.content == b"hello"
    assert response.headers["content-type"] == "text/plain; charset=utf-8" or (
        response.headers["content-type"] == "text/plain"
    )
    assert response.headers["content-disposition"] == "attachment"
    assert response.headers["x-content-type-options"] == "nosniff"


async def test_download_blob_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown blob id."""
    missing_id = uuid.uuid4()
    response = await client.get(f"/v1/blobs/{missing_id}/content")
    assert response.status_code == 404


async def test_delete_blob(client: httpx.AsyncClient) -> None:
    """Delete a blob and observe HTTP 204."""
    created = (
        await client.post(
            "/v1/blobs", files={"file": ("a.txt", b"hello", "text/plain")}
        )
    ).json()
    response = await client.delete(f"/v1/blobs/{created['id']}")
    assert response.status_code == 204
    response = await client.get(f"/v1/blobs/{created['id']}")
    assert response.status_code == 404


async def test_delete_blob_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown blob id."""
    response = await client.delete(f"/v1/blobs/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_delete_blob_in_use(
    client: httpx.AsyncClient, repository: FakeBlobRepository
) -> None:
    """Observe HTTP 409 when the blob is referenced by a plugin version."""
    blob = await create_blob(repository, ACCOUNT.id)
    repository.mark_referenced(blob.id)
    response = await client.delete(f"/v1/blobs/{blob.id}")
    assert response.status_code == 409
    assert response.json() == {
        "detail": f"Blob {blob.id} is in use by a plugin version"
    }
