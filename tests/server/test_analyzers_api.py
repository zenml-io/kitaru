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
"""Tests for the analyzer routes."""

import json
import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import (
    FakeAgentRepository,
    FakeBlobRepository,
    FakePluginRepository,
    create_blob,
    override_idempotency,
)
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_analyzer_service,
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
def agent_repository() -> FakeAgentRepository:
    """Provide the fake agent repository backing the app."""
    return FakeAgentRepository()


@pytest.fixture
def repository(
    blob_repository: FakeBlobRepository, agent_repository: FakeAgentRepository
) -> FakePluginRepository:
    """Provide the fake plugin repository backing the app."""
    return FakePluginRepository(
        blob_repository=blob_repository, agent_repository=agent_repository
    )


@pytest.fixture
async def client(
    repository: FakePluginRepository,
    blob_repository: FakeBlobRepository,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with a fake-backed analyzer service."""
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )
    service = PluginService(
        kind=PluginKind.ANALYZER,
        repository=repository,
        blob_repository=blob_repository,
    )
    app.dependency_overrides[get_analyzer_service] = lambda: service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    override_idempotency(app, ACCOUNT)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def test_create_analyzer(client: httpx.AsyncClient) -> None:
    """Create an analyzer and observe HTTP 201."""
    response = await client.post(
        "/api/v1/analyzers",
        json={
            "name": "trends",
            "description": "Surfaces usage trends",
            "metadata": {"a": 1},
        },
    )
    assert response.status_code == 201
    body = response.json()
    assert body["name"] == "trends"
    assert body["owner_id"] == str(ACCOUNT.id)
    assert body["description"] == "Surfaces usage trends"
    assert body["metadata"] == {"a": 1}
    assert body["latest_version"] == 0
    assert "provider" not in body
    assert "agent_id" not in body


async def test_create_analyzer_duplicate_name(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 for a duplicate analyzer name."""
    response = await client.post("/api/v1/analyzers", json={"name": "trends"})
    assert response.status_code == 201
    response = await client.post("/api/v1/analyzers", json={"name": "trends"})
    assert response.status_code == 409
    assert response.json() == {"detail": "Analyzer name 'trends' is already registered"}


async def test_create_analyzer_reserved_name(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for a name using the reserved default-plugin prefix."""
    response = await client.post("/api/v1/analyzers", json={"name": "kitaru/trends"})
    assert response.status_code == 422


async def test_create_analyzer_rejects_provider(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 when the request carries a provider field."""
    response = await client.post(
        "/api/v1/analyzers", json={"name": "trends", "provider": "langfuse"}
    )
    assert response.status_code == 422


async def test_create_analyzer_rejects_agent_id(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 when the request carries an agent_id field."""
    response = await client.post(
        "/api/v1/analyzers", json={"name": "trends", "agent_id": str(uuid.uuid4())}
    )
    assert response.status_code == 422


async def test_list_analyzers(client: httpx.AsyncClient) -> None:
    """List analyzers newest-first with a name filter."""
    for name in ["trends", "clusters"]:
        response = await client.post("/api/v1/analyzers", json={"name": name})
        assert response.status_code == 201

    response = await client.get("/api/v1/analyzers")
    assert response.status_code == 200
    body = response.json()
    assert body["next_cursor"] is None
    assert [item["name"] for item in body["items"]] == ["clusters", "trends"]

    filter_expression = {"field": "name", "op": "eq", "value": "trends"}
    response = await client.get(
        "/api/v1/analyzers", params={"filter": json.dumps(filter_expression)}
    )
    assert response.status_code == 200
    assert response.json()["items"][0]["name"] == "trends"


async def test_get_analyzer(client: httpx.AsyncClient) -> None:
    """Get an analyzer by id."""
    created = (await client.post("/api/v1/analyzers", json={"name": "trends"})).json()
    response = await client.get(f"/api/v1/analyzers/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created


async def test_get_analyzer_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown analyzer id."""
    missing_id = uuid.uuid4()
    response = await client.get(f"/api/v1/analyzers/{missing_id}")
    assert response.status_code == 404
    assert response.json() == {"detail": f"Plugin {missing_id} was not found"}


async def test_update_analyzer(client: httpx.AsyncClient) -> None:
    """Update an analyzer's description and metadata."""
    created = (await client.post("/api/v1/analyzers", json={"name": "trends"})).json()
    response = await client.patch(
        f"/api/v1/analyzers/{created['id']}",
        json={"description": "Surfaces usage trends", "metadata": {"a": 1}},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["description"] == "Surfaces usage trends"
    assert body["metadata"] == {"a": 1}


async def test_update_analyzer_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown analyzer id."""
    response = await client.patch(
        f"/api/v1/analyzers/{uuid.uuid4()}", json={"description": "x"}
    )
    assert response.status_code == 404


async def test_delete_analyzer(client: httpx.AsyncClient) -> None:
    """Delete an analyzer and observe HTTP 204."""
    created = (await client.post("/api/v1/analyzers", json={"name": "trends"})).json()
    response = await client.delete(f"/api/v1/analyzers/{created['id']}")
    assert response.status_code == 204
    response = await client.get(f"/api/v1/analyzers/{created['id']}")
    assert response.status_code == 404


async def test_delete_analyzer_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown analyzer id."""
    response = await client.delete(f"/api/v1/analyzers/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_create_analyzer_version(client: httpx.AsyncClient) -> None:
    """Create an analyzer version and observe HTTP 201."""
    created = (await client.post("/api/v1/analyzers", json={"name": "trends"})).json()
    response = await client.post(
        f"/api/v1/analyzers/{created['id']}/versions",
        json={
            "source": {
                "type": "package",
                "requirement": "kitaru-trends==1.0.0",
                "entrypoint": "pkg:analyze",
            },
            "display_version": "v1",
        },
    )
    assert response.status_code == 201
    body = response.json()
    assert body["analyzer_id"] == created["id"]
    assert body["version"] == 1
    assert body["display_version"] == "v1"
    assert body["source"]["type"] == "package"


async def test_create_analyzer_version_numbers_sequentially(
    client: httpx.AsyncClient,
) -> None:
    """Assign sequential version numbers."""
    created = (await client.post("/api/v1/analyzers", json={"name": "trends"})).json()
    body = {
        "source": {
            "type": "package",
            "requirement": "kitaru-trends==1.0.0",
            "entrypoint": "pkg:analyze",
        }
    }
    first = await client.post(f"/api/v1/analyzers/{created['id']}/versions", json=body)
    second = await client.post(f"/api/v1/analyzers/{created['id']}/versions", json=body)
    assert first.json()["version"] == 1
    assert second.json()["version"] == 2


async def test_create_analyzer_version_script_source(
    client: httpx.AsyncClient, blob_repository: FakeBlobRepository
) -> None:
    """Create a script-sourced analyzer version referencing a stored blob."""
    blob = await create_blob(blob_repository, ACCOUNT.id)
    created = (await client.post("/api/v1/analyzers", json={"name": "trends"})).json()
    response = await client.post(
        f"/api/v1/analyzers/{created['id']}/versions",
        json={
            "source": {
                "type": "script",
                "blob_id": str(blob.id),
                "entrypoint": "analyze",
            }
        },
    )
    assert response.status_code == 201
    assert response.json()["source"] == {
        "type": "script",
        "blob_id": str(blob.id),
        "entrypoint": "analyze",
    }


async def test_create_analyzer_version_missing_blob(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 when a script source names an unknown blob."""
    created = (await client.post("/api/v1/analyzers", json={"name": "trends"})).json()
    missing_blob_id = uuid.uuid4()
    response = await client.post(
        f"/api/v1/analyzers/{created['id']}/versions",
        json={
            "source": {
                "type": "script",
                "blob_id": str(missing_blob_id),
                "entrypoint": "analyze",
            }
        },
    )
    assert response.status_code == 404
    assert response.json() == {"detail": f"Blob {missing_blob_id} was not found"}


async def test_list_analyzer_versions(client: httpx.AsyncClient) -> None:
    """List an analyzer's versions."""
    created = (await client.post("/api/v1/analyzers", json={"name": "trends"})).json()
    body = {
        "source": {
            "type": "package",
            "requirement": "kitaru-trends==1.0.0",
            "entrypoint": "pkg:analyze",
        }
    }
    await client.post(f"/api/v1/analyzers/{created['id']}/versions", json=body)
    await client.post(f"/api/v1/analyzers/{created['id']}/versions", json=body)

    response = await client.get(f"/api/v1/analyzers/{created['id']}/versions")
    assert response.status_code == 200
    body_ = response.json()
    assert body_["next_cursor"] is None
    assert sorted(item["version"] for item in body_["items"]) == [1, 2]


async def test_get_analyzer_version(client: httpx.AsyncClient) -> None:
    """Get an analyzer version by version number."""
    created = (await client.post("/api/v1/analyzers", json={"name": "trends"})).json()
    version = (
        await client.post(
            f"/api/v1/analyzers/{created['id']}/versions",
            json={
                "source": {
                    "type": "package",
                    "requirement": "kitaru-trends==1.0.0",
                    "entrypoint": "pkg:analyze",
                }
            },
        )
    ).json()
    response = await client.get(
        f"/api/v1/analyzers/{created['id']}/versions/{version['version']}"
    )
    assert response.status_code == 200
    assert response.json() == version


async def test_get_analyzer_version_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown version number."""
    created = (await client.post("/api/v1/analyzers", json={"name": "trends"})).json()
    response = await client.get(f"/api/v1/analyzers/{created['id']}/versions/1")
    assert response.status_code == 404


async def test_update_analyzer_version(client: httpx.AsyncClient) -> None:
    """Update an analyzer version's display version."""
    created = (await client.post("/api/v1/analyzers", json={"name": "trends"})).json()
    version = (
        await client.post(
            f"/api/v1/analyzers/{created['id']}/versions",
            json={
                "source": {
                    "type": "package",
                    "requirement": "kitaru-trends==1.0.0",
                    "entrypoint": "pkg:analyze",
                },
                "display_version": "v1",
            },
        )
    ).json()
    response = await client.patch(
        f"/api/v1/analyzers/{created['id']}/versions/{version['version']}",
        json={"display_version": "v1.0.1"},
    )
    assert response.status_code == 200
    assert response.json()["display_version"] == "v1.0.1"


async def test_update_analyzer_version_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown version number."""
    created = (await client.post("/api/v1/analyzers", json={"name": "trends"})).json()
    response = await client.patch(
        f"/api/v1/analyzers/{created['id']}/versions/1", json={"display_version": "v1"}
    )
    assert response.status_code == 404
