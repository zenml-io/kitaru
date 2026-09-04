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

import json
import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import (
    FakeBlobRepository,
    FakePluginRepository,
    db_settings,
    lifespan_client,
    override_idempotency,
)
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_importer_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.plugin_service import PluginService
from kitaru.server.domain.account import Account
from kitaru.server.domain.plugin import PluginKind

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


@pytest.fixture
def blob_repository() -> FakeBlobRepository:
    """Provide a fake blob repository."""
    return FakeBlobRepository()


@pytest.fixture
def repository(blob_repository: FakeBlobRepository) -> FakePluginRepository:
    """Provide the fake plugin repository backing the app."""
    return FakePluginRepository(blob_repository=blob_repository)


@pytest.fixture
async def client(
    repository: FakePluginRepository,
    blob_repository: FakeBlobRepository,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with a fake-backed importer service."""
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )
    service = PluginService(
        kind=PluginKind.IMPORTER,
        repository=repository,
        blob_repository=blob_repository,
    )
    app.dependency_overrides[get_importer_service] = lambda: service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    override_idempotency(app, ACCOUNT)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def test_create_importer(client: httpx.AsyncClient) -> None:
    """Create an importer with a provider and observe HTTP 201."""
    response = await client.post(
        "/api/v1/importers", json={"name": "langfuse-import", "provider": "langfuse"}
    )
    assert response.status_code == 201
    body = response.json()
    assert body["name"] == "langfuse-import"
    assert body["provider"] == "langfuse"
    assert body["latest_version"] == 0
    assert "agent_id" not in body


async def test_create_importer_duplicate_name(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 for a duplicate importer name."""
    response = await client.post("/api/v1/importers", json={"name": "langfuse-import"})
    assert response.status_code == 201
    response = await client.post("/api/v1/importers", json={"name": "langfuse-import"})
    assert response.status_code == 409
    assert response.json() == {
        "detail": "Importer name 'langfuse-import' is already registered"
    }


async def test_create_importer_reserved_name(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for a name using the reserved default-plugin prefix."""
    response = await client.post(
        "/api/v1/importers", json={"name": "kitaru/langfuse-import"}
    )
    assert response.status_code == 422


async def test_list_importers_filter_by_provider(client: httpx.AsyncClient) -> None:
    """List importers filtered by provider."""
    await client.post(
        "/api/v1/importers", json={"name": "langfuse-import", "provider": "langfuse"}
    )
    await client.post(
        "/api/v1/importers",
        json={"name": "braintrust-import", "provider": "braintrust"},
    )

    filter_expression = {"field": "provider", "op": "eq", "value": "langfuse"}
    response = await client.get(
        "/api/v1/importers", params={"filter": json.dumps(filter_expression)}
    )
    assert response.status_code == 200
    body = response.json()
    assert [item["name"] for item in body["items"]] == ["langfuse-import"]


async def test_get_importer(client: httpx.AsyncClient) -> None:
    """Get an importer by id."""
    created = (
        await client.post("/api/v1/importers", json={"name": "langfuse-import"})
    ).json()
    response = await client.get(f"/api/v1/importers/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created


async def test_get_importer_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown importer id."""
    missing_id = uuid.uuid4()
    response = await client.get(f"/api/v1/importers/{missing_id}")
    assert response.status_code == 404
    assert response.json() == {"detail": f"Plugin {missing_id} was not found"}


async def test_update_importer(client: httpx.AsyncClient) -> None:
    """Update an importer's description and metadata."""
    created = (
        await client.post("/api/v1/importers", json={"name": "langfuse-import"})
    ).json()
    response = await client.patch(
        f"/api/v1/importers/{created['id']}",
        json={"description": "Imports from Langfuse", "metadata": {"a": 1}},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["description"] == "Imports from Langfuse"
    assert body["metadata"] == {"a": 1}
    assert body["provider"] is None


async def test_update_importer_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown importer id."""
    response = await client.patch(
        f"/api/v1/importers/{uuid.uuid4()}", json={"description": "x"}
    )
    assert response.status_code == 404


async def test_delete_importer(client: httpx.AsyncClient) -> None:
    """Delete an importer and observe HTTP 204."""
    created = (
        await client.post("/api/v1/importers", json={"name": "langfuse-import"})
    ).json()
    response = await client.delete(f"/api/v1/importers/{created['id']}")
    assert response.status_code == 204
    response = await client.get(f"/api/v1/importers/{created['id']}")
    assert response.status_code == 404


async def test_delete_importer_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown importer id."""
    response = await client.delete(f"/api/v1/importers/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_delete_importer_in_use() -> None:
    """Observe HTTP 409 for an importer whose version an import references."""
    async with lifespan_client(db_settings()) as client:
        agent = (await client.post("/api/v1/agents", json={"name": "assistant"})).json()
        script = await client.post(
            "/api/v1/blobs",
            files={"file": ("run.py", b"def run(): pass", "text/plain")},
        )
        payload = await client.post(
            "/api/v1/blobs", files={"file": ("payload.json", b"[]", "text/plain")}
        )
        importer = (
            await client.post("/api/v1/importers", json={"name": "langfuse-import"})
        ).json()
        response = await client.post(
            f"/api/v1/importers/{importer['id']}/versions",
            json={
                "source": {
                    "type": "script",
                    "blob_id": script.json()["id"],
                    "entrypoint": "run",
                }
            },
        )
        assert response.status_code == 201, response.text
        response = await client.post(
            "/api/v1/imports",
            json={
                "importer": "langfuse-import",
                "agent_id": agent["id"],
                "payload_blob_id": payload.json()["id"],
            },
        )
        assert response.status_code == 201, response.text

        response = await client.delete(f"/api/v1/importers/{importer['id']}")
        assert response.status_code == 409
        assert response.json() == {
            "detail": f"Plugin {importer['id']} is in use by an import"
        }
        response = await client.get(f"/api/v1/importers/{importer['id']}")
        assert response.status_code == 200


async def test_create_importer_version(client: httpx.AsyncClient) -> None:
    """Create an importer version and observe HTTP 201."""
    created = (
        await client.post("/api/v1/importers", json={"name": "langfuse-import"})
    ).json()
    response = await client.post(
        f"/api/v1/importers/{created['id']}/versions",
        json={
            "source": {
                "type": "package",
                "requirement": "kitaru-importer==1.0.0",
                "entrypoint": "pkg:run",
            },
            "display_version": "v1",
        },
    )
    assert response.status_code == 201
    body = response.json()
    assert body["importer_id"] == created["id"]
    assert body["version"] == 1
    assert body["display_version"] == "v1"


async def test_create_importer_version_missing_blob(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 when a script source names an unknown blob."""
    created = (
        await client.post("/api/v1/importers", json={"name": "langfuse-import"})
    ).json()
    missing_blob_id = uuid.uuid4()
    response = await client.post(
        f"/api/v1/importers/{created['id']}/versions",
        json={
            "source": {
                "type": "script",
                "blob_id": str(missing_blob_id),
                "entrypoint": "run",
            }
        },
    )
    assert response.status_code == 404
    assert response.json() == {"detail": f"Blob {missing_blob_id} was not found"}


async def test_list_importer_versions(client: httpx.AsyncClient) -> None:
    """List an importer's versions."""
    created = (
        await client.post("/api/v1/importers", json={"name": "langfuse-import"})
    ).json()
    body = {
        "source": {
            "type": "package",
            "requirement": "kitaru-importer==1.0.0",
            "entrypoint": "pkg:run",
        }
    }
    await client.post(f"/api/v1/importers/{created['id']}/versions", json=body)
    await client.post(f"/api/v1/importers/{created['id']}/versions", json=body)

    response = await client.get(f"/api/v1/importers/{created['id']}/versions")
    assert response.status_code == 200
    assert sorted(item["version"] for item in response.json()["items"]) == [1, 2]


async def test_get_importer_version(client: httpx.AsyncClient) -> None:
    """Get an importer version by version number."""
    created = (
        await client.post("/api/v1/importers", json={"name": "langfuse-import"})
    ).json()
    version = (
        await client.post(
            f"/api/v1/importers/{created['id']}/versions",
            json={
                "source": {
                    "type": "package",
                    "requirement": "kitaru-importer==1.0.0",
                    "entrypoint": "pkg:run",
                }
            },
        )
    ).json()
    response = await client.get(
        f"/api/v1/importers/{created['id']}/versions/{version['version']}"
    )
    assert response.status_code == 200
    assert response.json() == version


async def test_get_importer_version_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown version number."""
    created = (
        await client.post("/api/v1/importers", json={"name": "langfuse-import"})
    ).json()
    response = await client.get(f"/api/v1/importers/{created['id']}/versions/1")
    assert response.status_code == 404


async def test_update_importer_version(client: httpx.AsyncClient) -> None:
    """Update an importer version's display version."""
    created = (
        await client.post("/api/v1/importers", json={"name": "langfuse-import"})
    ).json()
    version = (
        await client.post(
            f"/api/v1/importers/{created['id']}/versions",
            json={
                "source": {
                    "type": "package",
                    "requirement": "kitaru-importer==1.0.0",
                    "entrypoint": "pkg:run",
                },
                "display_version": "v1",
            },
        )
    ).json()
    response = await client.patch(
        f"/api/v1/importers/{created['id']}/versions/{version['version']}",
        json={"display_version": "v1.0.1"},
    )
    assert response.status_code == 200
    assert response.json()["display_version"] == "v1.0.1"


async def test_update_importer_version_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown version number."""
    created = (
        await client.post("/api/v1/importers", json={"name": "langfuse-import"})
    ).json()
    response = await client.patch(
        f"/api/v1/importers/{created['id']}/versions/1", json={"display_version": "v1"}
    )
    assert response.status_code == 404
