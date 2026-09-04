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
"""Tests for the evaluator routes."""

import json
import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import (
    FakeAgentRepository,
    FakeBlobRepository,
    FakePluginRepository,
    create_agent,
    create_blob,
    override_idempotency,
)
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_evaluator_service,
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
    """Provide an HTTP client for the app with a fake-backed evaluator service."""
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )
    service = PluginService(
        kind=PluginKind.EVALUATOR,
        repository=repository,
        blob_repository=blob_repository,
    )
    app.dependency_overrides[get_evaluator_service] = lambda: service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    override_idempotency(app, ACCOUNT)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def test_create_evaluator(client: httpx.AsyncClient) -> None:
    """Create an evaluator and observe HTTP 201."""
    response = await client.post(
        "/api/v1/evaluators",
        json={
            "name": "accuracy",
            "description": "Scores accuracy",
            "metadata": {"a": 1},
        },
    )
    assert response.status_code == 201
    body = response.json()
    assert body["name"] == "accuracy"
    assert body["owner_id"] == str(ACCOUNT.id)
    assert body["description"] == "Scores accuracy"
    assert body["metadata"] == {"a": 1}
    assert body["latest_version"] == 0
    assert "provider" not in body


async def test_create_evaluator_duplicate_name(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 for a duplicate evaluator name."""
    response = await client.post("/api/v1/evaluators", json={"name": "accuracy"})
    assert response.status_code == 201
    response = await client.post("/api/v1/evaluators", json={"name": "accuracy"})
    assert response.status_code == 409
    assert response.json() == {
        "detail": "Evaluator name 'accuracy' is already registered"
    }


async def test_create_evaluator_reserved_name(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for a name using the reserved default-plugin prefix."""
    response = await client.post("/api/v1/evaluators", json={"name": "kitaru/accuracy"})
    assert response.status_code == 422


async def test_create_evaluator_rejects_provider(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 when the request carries a provider field."""
    response = await client.post(
        "/api/v1/evaluators", json={"name": "accuracy", "provider": "langfuse"}
    )
    assert response.status_code == 422


async def test_list_evaluators(client: httpx.AsyncClient) -> None:
    """List evaluators newest-first with a name filter."""
    for name in ["accuracy", "relevance"]:
        response = await client.post("/api/v1/evaluators", json={"name": name})
        assert response.status_code == 201

    response = await client.get("/api/v1/evaluators")
    assert response.status_code == 200
    body = response.json()
    assert body["next_cursor"] is None
    assert [item["name"] for item in body["items"]] == ["relevance", "accuracy"]

    filter_expression = {"field": "name", "op": "eq", "value": "accuracy"}
    response = await client.get(
        "/api/v1/evaluators", params={"filter": json.dumps(filter_expression)}
    )
    assert response.status_code == 200
    assert response.json()["items"][0]["name"] == "accuracy"


async def test_get_evaluator(client: httpx.AsyncClient) -> None:
    """Get an evaluator by id."""
    created = (
        await client.post("/api/v1/evaluators", json={"name": "accuracy"})
    ).json()
    response = await client.get(f"/api/v1/evaluators/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created


async def test_get_evaluator_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown evaluator id."""
    missing_id = uuid.uuid4()
    response = await client.get(f"/api/v1/evaluators/{missing_id}")
    assert response.status_code == 404
    assert response.json() == {"detail": f"Plugin {missing_id} was not found"}


async def test_update_evaluator(client: httpx.AsyncClient) -> None:
    """Update an evaluator's description and metadata."""
    created = (
        await client.post("/api/v1/evaluators", json={"name": "accuracy"})
    ).json()
    response = await client.patch(
        f"/api/v1/evaluators/{created['id']}",
        json={"description": "Scores accuracy", "metadata": {"a": 1}},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["description"] == "Scores accuracy"
    assert body["metadata"] == {"a": 1}


async def test_update_evaluator_explicit_null_clears_description(
    client: httpx.AsyncClient,
) -> None:
    """Clear the description when the update sets it to null explicitly."""
    created = (
        await client.post(
            "/api/v1/evaluators", json={"name": "accuracy", "description": "old"}
        )
    ).json()
    response = await client.patch(
        f"/api/v1/evaluators/{created['id']}", json={"description": None}
    )
    assert response.status_code == 200
    assert response.json()["description"] is None


async def test_update_evaluator_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown evaluator id."""
    response = await client.patch(
        f"/api/v1/evaluators/{uuid.uuid4()}", json={"description": "x"}
    )
    assert response.status_code == 404


async def test_delete_evaluator(client: httpx.AsyncClient) -> None:
    """Delete an evaluator and observe HTTP 204."""
    created = (
        await client.post("/api/v1/evaluators", json={"name": "accuracy"})
    ).json()
    response = await client.delete(f"/api/v1/evaluators/{created['id']}")
    assert response.status_code == 204
    response = await client.get(f"/api/v1/evaluators/{created['id']}")
    assert response.status_code == 404


async def test_delete_evaluator_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown evaluator id."""
    response = await client.delete(f"/api/v1/evaluators/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_create_evaluator_version(client: httpx.AsyncClient) -> None:
    """Create an evaluator version and observe HTTP 201."""
    created = (
        await client.post("/api/v1/evaluators", json={"name": "accuracy"})
    ).json()
    response = await client.post(
        f"/api/v1/evaluators/{created['id']}/versions",
        json={
            "source": {
                "type": "package",
                "requirement": "kitaru-scorer==1.0.0",
                "entrypoint": "pkg:score",
            },
            "display_version": "v1",
        },
    )
    assert response.status_code == 201
    body = response.json()
    assert body["evaluator_id"] == created["id"]
    assert body["version"] == 1
    assert body["display_version"] == "v1"
    assert body["source"]["type"] == "package"


async def test_create_evaluator_version_numbers_sequentially(
    client: httpx.AsyncClient,
) -> None:
    """Assign sequential version numbers."""
    created = (
        await client.post("/api/v1/evaluators", json={"name": "accuracy"})
    ).json()
    body = {
        "source": {
            "type": "package",
            "requirement": "kitaru-scorer==1.0.0",
            "entrypoint": "pkg:score",
        }
    }
    first = await client.post(f"/api/v1/evaluators/{created['id']}/versions", json=body)
    second = await client.post(
        f"/api/v1/evaluators/{created['id']}/versions", json=body
    )
    assert first.json()["version"] == 1
    assert second.json()["version"] == 2


async def test_create_evaluator_version_script_source(
    client: httpx.AsyncClient, blob_repository: FakeBlobRepository
) -> None:
    """Create a script-sourced evaluator version referencing a stored blob."""
    blob = await create_blob(blob_repository, ACCOUNT.id)
    created = (
        await client.post("/api/v1/evaluators", json={"name": "accuracy"})
    ).json()
    response = await client.post(
        f"/api/v1/evaluators/{created['id']}/versions",
        json={
            "source": {"type": "script", "blob_id": str(blob.id), "entrypoint": "score"}
        },
    )
    assert response.status_code == 201
    assert response.json()["source"] == {
        "type": "script",
        "blob_id": str(blob.id),
        "entrypoint": "score",
        "fetch_entrypoint": None,
    }


async def test_create_evaluator_version_missing_blob(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 when a script source names an unknown blob."""
    created = (
        await client.post("/api/v1/evaluators", json={"name": "accuracy"})
    ).json()
    missing_blob_id = uuid.uuid4()
    response = await client.post(
        f"/api/v1/evaluators/{created['id']}/versions",
        json={
            "source": {
                "type": "script",
                "blob_id": str(missing_blob_id),
                "entrypoint": "score",
            }
        },
    )
    assert response.status_code == 404
    assert response.json() == {"detail": f"Blob {missing_blob_id} was not found"}


@pytest.mark.parametrize(
    "requirement",
    [
        "kitaru-scorer>=1.0.0",
        "kitaru-scorer==1.0.*",
        "kitaru-scorer===1.0.0",
        "kitaru-scorer @ https://example.com/x.whl",
        "kitaru-scorer==1.0.0; python_version>='3.8'",
    ],
)
async def test_create_evaluator_version_invalid_requirement(
    client: httpx.AsyncClient, requirement: str
) -> None:
    """Observe HTTP 422 for a requirement that is not an exact pin."""
    created = (
        await client.post("/api/v1/evaluators", json={"name": "accuracy"})
    ).json()
    response = await client.post(
        f"/api/v1/evaluators/{created['id']}/versions",
        json={
            "source": {
                "type": "package",
                "requirement": requirement,
                "entrypoint": "pkg:score",
            }
        },
    )
    assert response.status_code == 422


async def test_create_evaluator_version_requirement_with_extras(
    client: httpx.AsyncClient,
) -> None:
    """Accept a requirement carrying extras alongside its exact pin."""
    created = (
        await client.post("/api/v1/evaluators", json={"name": "accuracy"})
    ).json()
    response = await client.post(
        f"/api/v1/evaluators/{created['id']}/versions",
        json={
            "source": {
                "type": "package",
                "requirement": "kitaru-scorer[extra]==1.0.0",
                "entrypoint": "pkg:score",
            }
        },
    )
    assert response.status_code == 201


async def test_create_evaluator_version_invalid_package_entrypoint(
    client: httpx.AsyncClient,
) -> None:
    """Observe HTTP 422 for a malformed package entrypoint."""
    created = (
        await client.post("/api/v1/evaluators", json={"name": "accuracy"})
    ).json()
    response = await client.post(
        f"/api/v1/evaluators/{created['id']}/versions",
        json={
            "source": {
                "type": "package",
                "requirement": "kitaru-scorer==1.0.0",
                "entrypoint": "not-a-module-attr",
            }
        },
    )
    assert response.status_code == 422


async def test_list_evaluator_versions(client: httpx.AsyncClient) -> None:
    """List an evaluator's versions."""
    created = (
        await client.post("/api/v1/evaluators", json={"name": "accuracy"})
    ).json()
    body = {
        "source": {
            "type": "package",
            "requirement": "kitaru-scorer==1.0.0",
            "entrypoint": "pkg:score",
        }
    }
    await client.post(f"/api/v1/evaluators/{created['id']}/versions", json=body)
    await client.post(f"/api/v1/evaluators/{created['id']}/versions", json=body)

    response = await client.get(f"/api/v1/evaluators/{created['id']}/versions")
    assert response.status_code == 200
    body = response.json()
    assert body["next_cursor"] is None
    assert sorted(item["version"] for item in body["items"]) == [1, 2]


async def test_get_evaluator_version(client: httpx.AsyncClient) -> None:
    """Get an evaluator version by version number."""
    created = (
        await client.post("/api/v1/evaluators", json={"name": "accuracy"})
    ).json()
    version = (
        await client.post(
            f"/api/v1/evaluators/{created['id']}/versions",
            json={
                "source": {
                    "type": "package",
                    "requirement": "kitaru-scorer==1.0.0",
                    "entrypoint": "pkg:score",
                }
            },
        )
    ).json()
    response = await client.get(
        f"/api/v1/evaluators/{created['id']}/versions/{version['version']}"
    )
    assert response.status_code == 200
    assert response.json() == version


async def test_get_evaluator_version_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown version number."""
    created = (
        await client.post("/api/v1/evaluators", json={"name": "accuracy"})
    ).json()
    response = await client.get(f"/api/v1/evaluators/{created['id']}/versions/1")
    assert response.status_code == 404


async def test_update_evaluator_version(client: httpx.AsyncClient) -> None:
    """Update an evaluator version's display version."""
    created = (
        await client.post("/api/v1/evaluators", json={"name": "accuracy"})
    ).json()
    version = (
        await client.post(
            f"/api/v1/evaluators/{created['id']}/versions",
            json={
                "source": {
                    "type": "package",
                    "requirement": "kitaru-scorer==1.0.0",
                    "entrypoint": "pkg:score",
                },
                "display_version": "v1",
            },
        )
    ).json()
    response = await client.patch(
        f"/api/v1/evaluators/{created['id']}/versions/{version['version']}",
        json={"display_version": "v1.0.1"},
    )
    assert response.status_code == 200
    assert response.json()["display_version"] == "v1.0.1"


async def test_update_evaluator_version_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown version number."""
    created = (
        await client.post("/api/v1/evaluators", json={"name": "accuracy"})
    ).json()
    response = await client.patch(
        f"/api/v1/evaluators/{created['id']}/versions/1", json={"display_version": "v1"}
    )
    assert response.status_code == 404


async def test_create_evaluator_with_logo_url(client: httpx.AsyncClient) -> None:
    """Round-trip the logo URL through create and get."""
    response = await client.post(
        "/api/v1/evaluators",
        json={"name": "accuracy", "logo_url": "https://example.com/accuracy.svg"},
    )
    assert response.status_code == 201
    assert response.json()["logo_url"] == "https://example.com/accuracy.svg"

    evaluator_id = response.json()["id"]
    fetched = await client.get(f"/api/v1/evaluators/{evaluator_id}")
    assert fetched.json()["logo_url"] == "https://example.com/accuracy.svg"


async def test_create_evaluator_without_logo_url(client: httpx.AsyncClient) -> None:
    """Return a null logo URL when none was given."""
    response = await client.post("/api/v1/evaluators", json={"name": "accuracy"})
    assert response.status_code == 201
    assert response.json()["logo_url"] is None


async def test_update_evaluator_logo_url(client: httpx.AsyncClient) -> None:
    """Update the logo URL through the evaluator update endpoint."""
    created = await client.post(
        "/api/v1/evaluators",
        json={"name": "accuracy", "logo_url": "https://example.com/old.svg"},
    )
    evaluator_id = created.json()["id"]

    updated = await client.patch(
        f"/api/v1/evaluators/{evaluator_id}",
        json={"logo_url": "https://example.com/new.svg"},
    )
    assert updated.status_code == 200
    assert updated.json()["logo_url"] == "https://example.com/new.svg"


async def test_create_evaluator_scoped_to_agent(
    client: httpx.AsyncClient, agent_repository: FakeAgentRepository
) -> None:
    """Create an evaluator scoped to an agent and observe agent_id in the response."""
    agent = await create_agent(agent_repository, ACCOUNT.id)
    response = await client.post(
        "/api/v1/evaluators", json={"name": "accuracy", "agent_id": str(agent.id)}
    )
    assert response.status_code == 201
    assert response.json()["agent_id"] == str(agent.id)


async def test_create_evaluator_without_agent_id(client: httpx.AsyncClient) -> None:
    """Return a null agent id for a global evaluator."""
    response = await client.post("/api/v1/evaluators", json={"name": "accuracy"})
    assert response.status_code == 201
    assert response.json()["agent_id"] is None


async def test_create_evaluator_unknown_agent_id(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an evaluator scoped to an unknown agent."""
    missing_agent_id = uuid.uuid4()
    response = await client.post(
        "/api/v1/evaluators",
        json={"name": "accuracy", "agent_id": str(missing_agent_id)},
    )
    assert response.status_code == 404
    assert response.json() == {"detail": f"Agent {missing_agent_id} was not found"}


async def test_list_evaluators_filtered_by_agent(
    client: httpx.AsyncClient, agent_repository: FakeAgentRepository
) -> None:
    """List global evaluators together with one agent's scoped evaluators."""
    agent = await create_agent(agent_repository, ACCOUNT.id)
    other_agent = await create_agent(agent_repository, ACCOUNT.id, name="other")
    await client.post("/api/v1/evaluators", json={"name": "global"})
    await client.post(
        "/api/v1/evaluators", json={"name": "scoped", "agent_id": str(agent.id)}
    )
    await client.post(
        "/api/v1/evaluators",
        json={"name": "other-scoped", "agent_id": str(other_agent.id)},
    )

    filter_expression = {
        "or": [
            {"field": "agent_id", "op": "is_null"},
            {"field": "agent_id", "op": "eq", "value": str(agent.id)},
        ]
    }
    response = await client.get(
        "/api/v1/evaluators", params={"filter": json.dumps(filter_expression)}
    )
    assert response.status_code == 200
    names = {item["name"] for item in response.json()["items"]}
    assert names == {"global", "scoped"}
