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
"""Tests for the scorer routes."""

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


async def test_create_scorer(client: httpx.AsyncClient) -> None:
    """Create a scorer and observe HTTP 201."""
    response = await client.post("/v1/scorers", json={"name": "relevance"})
    assert response.status_code == 201
    body = response.json()
    assert body["name"] == "relevance"
    assert body["owner_id"] == str(ACCOUNT.id)
    assert body["latest_version"] == 0
    assert uuid.UUID(body["id"])


async def test_create_scorer_response_fields(client: httpx.AsyncClient) -> None:
    """Never expose the kind, provider, or metadata in the response."""
    response = await client.post("/v1/scorers", json={"name": "relevance"})
    assert response.status_code == 201
    assert set(response.json()) == {
        "id",
        "owner_id",
        "name",
        "latest_version",
        "created",
        "updated",
    }


async def test_create_scorer_duplicate_name(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 for a duplicate scorer name."""
    assert (await client.post("/v1/scorers", json={"name": "relevance"})).status_code
    response = await client.post("/v1/scorers", json={"name": "relevance"})
    assert response.status_code == 409
    assert response.json() == {
        "detail": "Plugin name 'relevance' is already registered"
    }


async def test_create_scorer_invalid_name(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for an invalid scorer name."""
    response = await client.post("/v1/scorers", json={"name": "in valid"})
    assert response.status_code == 422


async def test_create_scorer_rejects_provider(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for a provider on a scorer."""
    response = await client.post(
        "/v1/scorers", json={"name": "relevance", "provider": "langfuse"}
    )
    assert response.status_code == 422


async def test_list_scorers(client: httpx.AsyncClient) -> None:
    """List scorers with filters and pagination."""
    for name in ["alpha", "beta", "gamma"]:
        await client.post("/v1/scorers", json={"name": name})

    response = await client.get("/v1/scorers")
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 3
    assert [item["name"] for item in body["items"]] == ["alpha", "beta", "gamma"]

    response = await client.get("/v1/scorers", params={"name": "beta"})
    assert response.json()["total"] == 1

    response = await client.get("/v1/scorers", params={"page": 2, "page_size": 2})
    body = response.json()
    assert body["total"] == 3
    assert [item["name"] for item in body["items"]] == ["gamma"]


async def test_list_scorers_excludes_importers(client: httpx.AsyncClient) -> None:
    """Never list importers under the scorer routes."""
    await client.post("/v1/importers", json={"name": "langfuse"})
    response = await client.get("/v1/scorers")
    assert response.json()["total"] == 0


async def test_get_scorer(client: httpx.AsyncClient) -> None:
    """Get a scorer by id."""
    created = (await client.post("/v1/scorers", json={"name": "relevance"})).json()
    response = await client.get(f"/v1/scorers/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created


async def test_get_scorer_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown scorer id."""
    missing_id = uuid.uuid4()
    response = await client.get(f"/v1/scorers/{missing_id}")
    assert response.status_code == 404
    assert response.json() == {"detail": f"Plugin {missing_id} was not found"}


async def test_get_importer_under_scorers(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an importer id under the scorer routes."""
    created = (await client.post("/v1/importers", json={"name": "langfuse"})).json()
    response = await client.get(f"/v1/scorers/{created['id']}")
    assert response.status_code == 404


async def test_delete_scorer(client: httpx.AsyncClient) -> None:
    """Delete a scorer and observe HTTP 204."""
    created = (await client.post("/v1/scorers", json={"name": "relevance"})).json()
    response = await client.delete(f"/v1/scorers/{created['id']}")
    assert response.status_code == 204
    assert (await client.get(f"/v1/scorers/{created['id']}")).status_code == 404


async def test_create_scorer_version(
    client: httpx.AsyncClient, blob_id: uuid.UUID
) -> None:
    """Create scorer versions and observe HTTP 201 with rising numbers."""
    created = (await client.post("/v1/scorers", json={"name": "relevance"})).json()
    response = await client.post(
        f"/v1/scorers/{created['id']}/versions",
        json={"blob_id": str(blob_id), "entrypoint": "score"},
    )
    assert response.status_code == 201
    body = response.json()
    assert body["scorer_id"] == created["id"]
    assert body["version"] == 1
    assert body["format"] == "inline"
    assert body["blob_id"] == str(blob_id)
    assert body["entrypoint"] == "score"

    response = await client.post(
        f"/v1/scorers/{created['id']}/versions",
        json={"blob_id": str(blob_id), "entrypoint": "score"},
    )
    assert response.json()["version"] == 2
    assert (await client.get(f"/v1/scorers/{created['id']}")).json()[
        "latest_version"
    ] == 2


async def test_create_scorer_version_unknown_blob(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown code blob."""
    created = (await client.post("/v1/scorers", json={"name": "relevance"})).json()
    missing_id = uuid.uuid4()
    response = await client.post(
        f"/v1/scorers/{created['id']}/versions",
        json={"blob_id": str(missing_id), "entrypoint": "score"},
    )
    assert response.status_code == 404
    assert response.json() == {"detail": f"Blob {missing_id} was not found"}


async def test_create_scorer_version_unknown_format(
    client: httpx.AsyncClient, blob_id: uuid.UUID
) -> None:
    """Observe HTTP 422 for a code format the server does not accept."""
    created = (await client.post("/v1/scorers", json={"name": "relevance"})).json()
    response = await client.post(
        f"/v1/scorers/{created['id']}/versions",
        json={"format": "archive", "blob_id": str(blob_id), "entrypoint": "score"},
    )
    assert response.status_code == 422


async def test_list_scorer_versions(
    client: httpx.AsyncClient, blob_id: uuid.UUID
) -> None:
    """List the versions of a scorer."""
    created = (await client.post("/v1/scorers", json={"name": "relevance"})).json()
    for _ in range(3):
        await client.post(
            f"/v1/scorers/{created['id']}/versions",
            json={"blob_id": str(blob_id), "entrypoint": "score"},
        )

    response = await client.get(f"/v1/scorers/{created['id']}/versions")
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 3
    assert [item["version"] for item in body["items"]] == [1, 2, 3]


async def test_get_scorer_version(
    client: httpx.AsyncClient, blob_id: uuid.UUID
) -> None:
    """Get a scorer version by version number."""
    created = (await client.post("/v1/scorers", json={"name": "relevance"})).json()
    version = (
        await client.post(
            f"/v1/scorers/{created['id']}/versions",
            json={"blob_id": str(blob_id), "entrypoint": "score"},
        )
    ).json()
    response = await client.get(f"/v1/scorers/{created['id']}/versions/1")
    assert response.status_code == 200
    assert response.json() == version


async def test_get_scorer_version_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a version the scorer does not have."""
    created = (await client.post("/v1/scorers", json={"name": "relevance"})).json()
    response = await client.get(f"/v1/scorers/{created['id']}/versions/1")
    assert response.status_code == 404
    assert response.json() == {"detail": f"Plugin {created['id']} has no version 1"}
