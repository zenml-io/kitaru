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
"""Tests for the importer routes."""

import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import FakeBlobRepository, FakePluginRepository
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_importer_service,
    get_scorer_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.plugin_service import PluginService
from kitaru.server.domain.account import Account
from kitaru.server.domain.blob import Blob
from kitaru.server.domain.plugin import PluginKind

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


@pytest.fixture
def blob_repository() -> FakeBlobRepository:
    """Provide the fake blob repository backing the app."""
    return FakeBlobRepository()


@pytest.fixture
async def blob_id(blob_repository: FakeBlobRepository) -> uuid.UUID:
    """Provide the id of a stored code blob."""
    blob = await blob_repository.create(
        Blob(
            owner_id=ACCOUNT.id,
            sha256="a" * 64,
            size=3,
            media_type="text/x-python",
            data=b"abc",
        )
    )
    return blob.id


@pytest.fixture
async def client(
    blob_repository: FakeBlobRepository,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with a fake-backed plugin service."""
    app = create_app(
        APISettings(DB_HOST="localhost", SECRET_ENCRYPTION_KEY="test-encryption-key")
    )
    repository = FakePluginRepository(blob_repository)
    scorer_service = PluginService(
        repository=repository,
        blob_repository=blob_repository,
        kind=PluginKind.SCORER,
    )
    importer_service = PluginService(
        repository=repository,
        blob_repository=blob_repository,
        kind=PluginKind.IMPORTER,
    )
    app.dependency_overrides[get_scorer_service] = lambda: scorer_service
    app.dependency_overrides[get_importer_service] = lambda: importer_service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def test_create_importer(client: httpx.AsyncClient) -> None:
    """Create an importer and observe HTTP 201."""
    response = await client.post(
        "/v1/importers",
        json={
            "name": "langfuse",
            "provider": "langfuse",
            "metadata": {"region": "eu"},
        },
    )
    assert response.status_code == 201
    body = response.json()
    assert body["name"] == "langfuse"
    assert body["owner_id"] == str(ACCOUNT.id)
    assert body["provider"] == "langfuse"
    assert body["metadata"] == {"region": "eu"}
    assert body["latest_version"] == 0


async def test_create_importer_duplicate_name(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 for a duplicate importer name."""
    await client.post("/v1/importers", json={"name": "langfuse"})
    response = await client.post("/v1/importers", json={"name": "langfuse"})
    assert response.status_code == 409
    assert response.json() == {"detail": "Plugin name 'langfuse' is already registered"}


async def test_create_importer_invalid_provider(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for an overlong provider."""
    response = await client.post(
        "/v1/importers", json={"name": "langfuse", "provider": "x" * 65}
    )
    assert response.status_code == 422


async def test_list_importers(client: httpx.AsyncClient) -> None:
    """List importers with filters and pagination."""
    await client.post("/v1/importers", json={"name": "one", "provider": "langfuse"})
    await client.post("/v1/importers", json={"name": "two", "provider": "braintrust"})
    await client.post("/v1/scorers", json={"name": "relevance"})

    response = await client.get("/v1/importers")
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 2
    assert [item["name"] for item in body["items"]] == ["one", "two"]

    response = await client.get("/v1/importers", params={"provider": "braintrust"})
    body = response.json()
    assert body["total"] == 1
    assert body["items"][0]["name"] == "two"

    response = await client.get("/v1/importers", params={"name": "one"})
    assert response.json()["total"] == 1

    response = await client.get("/v1/importers", params={"page": 2, "page_size": 1})
    body = response.json()
    assert body["total"] == 2
    assert [item["name"] for item in body["items"]] == ["two"]


async def test_get_importer(client: httpx.AsyncClient) -> None:
    """Get an importer by id."""
    created = (await client.post("/v1/importers", json={"name": "langfuse"})).json()
    response = await client.get(f"/v1/importers/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created


async def test_get_scorer_under_importers(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a scorer id under the importer routes."""
    created = (await client.post("/v1/scorers", json={"name": "relevance"})).json()
    response = await client.get(f"/v1/importers/{created['id']}")
    assert response.status_code == 404


async def test_delete_importer(client: httpx.AsyncClient) -> None:
    """Delete an importer and observe HTTP 204."""
    created = (await client.post("/v1/importers", json={"name": "langfuse"})).json()
    response = await client.delete(f"/v1/importers/{created['id']}")
    assert response.status_code == 204
    assert (await client.get(f"/v1/importers/{created['id']}")).status_code == 404


async def test_create_importer_version(
    client: httpx.AsyncClient, blob_id: uuid.UUID
) -> None:
    """Create importer versions and observe HTTP 201 with rising numbers."""
    created = (await client.post("/v1/importers", json={"name": "langfuse"})).json()
    response = await client.post(
        f"/v1/importers/{created['id']}/versions",
        json={"blob_id": str(blob_id), "entrypoint": "parse"},
    )
    assert response.status_code == 201
    body = response.json()
    assert body["importer_id"] == created["id"]
    assert body["version"] == 1
    assert body["format"] == "inline"
    assert body["entrypoint"] == "parse"

    response = await client.post(
        f"/v1/importers/{created['id']}/versions",
        json={"blob_id": str(blob_id), "entrypoint": "parse"},
    )
    assert response.json()["version"] == 2


async def test_list_importer_versions(
    client: httpx.AsyncClient, blob_id: uuid.UUID
) -> None:
    """List the versions of an importer."""
    created = (await client.post("/v1/importers", json={"name": "langfuse"})).json()
    for _ in range(2):
        await client.post(
            f"/v1/importers/{created['id']}/versions",
            json={"blob_id": str(blob_id), "entrypoint": "parse"},
        )

    response = await client.get(f"/v1/importers/{created['id']}/versions")
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 2
    assert [item["version"] for item in body["items"]] == [1, 2]


async def test_get_importer_version(
    client: httpx.AsyncClient, blob_id: uuid.UUID
) -> None:
    """Get an importer version by version number."""
    created = (await client.post("/v1/importers", json={"name": "langfuse"})).json()
    version = (
        await client.post(
            f"/v1/importers/{created['id']}/versions",
            json={"blob_id": str(blob_id), "entrypoint": "parse"},
        )
    ).json()
    response = await client.get(f"/v1/importers/{created['id']}/versions/1")
    assert response.status_code == 200
    assert response.json() == version


async def test_get_importer_version_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a version the importer does not have."""
    created = (await client.post("/v1/importers", json={"name": "langfuse"})).json()
    response = await client.get(f"/v1/importers/{created['id']}/versions/1")
    assert response.status_code == 404
    assert response.json() == {"detail": f"Plugin {created['id']} has no version 1"}
