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

import hashlib
import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import FakeBlobRepository
from kitaru.server.adapters.db.blob_storage import DatabaseBlobStorage
from kitaru.server.adapters.rest.dependencies import authorize, get_blob_service
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.blob_service import BlobService
from kitaru.server.domain.account import Account

ACCOUNT = Account(id=uuid.uuid4(), name="ann")

CONTENT = b"def score(session):\n    return 1.0\n"


@pytest.fixture
def repository() -> FakeBlobRepository:
    """Provide the fake blob repository backing the app."""
    return FakeBlobRepository()


@pytest.fixture
async def client(
    repository: FakeBlobRepository,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with a fake-backed blob service."""
    app = create_app(
        APISettings(DB_HOST="localhost", SECRET_ENCRYPTION_KEY="test-encryption-key")
    )
    service = BlobService(
        repository=repository, storage=DatabaseBlobStorage(), max_size_bytes=1024
    )
    app.dependency_overrides[get_blob_service] = lambda: service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def test_upload_blob(client: httpx.AsyncClient) -> None:
    """Upload a blob and observe HTTP 201."""
    response = await client.post(
        "/v1/blobs", files={"file": ("scorer.py", CONTENT, "text/x-python")}
    )
    assert response.status_code == 201
    body = response.json()
    assert body["sha256"] == hashlib.sha256(CONTENT).hexdigest()
    assert body["size"] == len(CONTENT)
    assert body["media_type"] == "text/x-python"
    assert body["created"] is not None
    assert uuid.UUID(body["id"])


async def test_upload_blob_response_has_no_content(client: httpx.AsyncClient) -> None:
    """Never expose the stored content in the response."""
    response = await client.post(
        "/v1/blobs", files={"file": ("scorer.py", CONTENT, "text/x-python")}
    )
    assert response.status_code == 201
    assert set(response.json()) == {
        "id",
        "sha256",
        "size",
        "media_type",
        "created",
    }


async def test_upload_blob_deduplicates(client: httpx.AsyncClient) -> None:
    """Observe HTTP 200 with the stored blob on a repeated upload."""
    first = await client.post(
        "/v1/blobs", files={"file": ("scorer.py", CONTENT, "text/x-python")}
    )
    assert first.status_code == 201
    second = await client.post(
        "/v1/blobs", files={"file": ("other.py", CONTENT, "text/plain")}
    )
    assert second.status_code == 200
    assert second.json() == first.json()


async def test_upload_blob_too_large(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for content over the size limit."""
    response = await client.post(
        "/v1/blobs", files={"file": ("big.bin", b"x" * 1025, "text/plain")}
    )
    assert response.status_code == 422
    assert response.json() == {"detail": "Blob exceeds 1024 bytes"}


async def test_upload_blob_missing_file(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 without an uploaded file."""
    response = await client.post("/v1/blobs")
    assert response.status_code == 422


async def test_download_blob(client: httpx.AsyncClient) -> None:
    """Download the content under the stored media type."""
    created = (
        await client.post(
            "/v1/blobs", files={"file": ("scorer.py", CONTENT, "text/x-python")}
        )
    ).json()
    response = await client.get(f"/v1/blobs/{created['id']}/content")
    assert response.status_code == 200
    assert response.content == CONTENT
    assert response.headers["content-type"].startswith("text/x-python")


async def test_download_blob_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown blob id."""
    missing_id = uuid.uuid4()
    response = await client.get(f"/v1/blobs/{missing_id}/content")
    assert response.status_code == 404
    assert response.json() == {"detail": f"Blob {missing_id} was not found"}
