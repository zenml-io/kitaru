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
"""Tests for the experiment routes."""

import json
import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import (
    FakeExperimentRepository,
    FakePluginRepository,
    ReplayServices,
    build_replay_services,
    create_plugin,
)
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_experiment_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.domain.account import Account
from kitaru.server.domain.plugin import PackagePluginSource, PluginKind

ACCOUNT = Account(id=uuid.uuid4(), name="ann")

SOURCE = PackagePluginSource(requirement="kitaru-scorer==1.0.0", entrypoint="pkg:score")


@pytest.fixture
def services() -> ReplayServices:
    """Provide fake-backed experiment, replay, and run services."""
    return build_replay_services()


@pytest.fixture
def plugin_repository(services: ReplayServices) -> FakePluginRepository:
    """Provide the fake plugin repository backing the app."""
    return services.plugins


@pytest.fixture
def experiment_repository(services: ReplayServices) -> FakeExperimentRepository:
    """Provide the fake experiment repository backing the app."""
    return services.experiments


@pytest.fixture
async def client(services: ReplayServices) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with fake-backed experiment services."""
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )
    app.dependency_overrides[get_experiment_service] = lambda: (
        services.experiment_service
    )
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.fixture(autouse=True)
async def _registered_evaluator(plugin_repository: FakePluginRepository) -> None:
    """Register an evaluator plugin with one version, available to every test."""
    plugin = await create_plugin(
        plugin_repository, ACCOUNT.id, kind=PluginKind.EVALUATOR, name="accuracy"
    )
    await plugin_repository.create_version(plugin.id, SOURCE, display_version="v1")


async def test_create_experiment(client: httpx.AsyncClient) -> None:
    """Create an experiment and observe HTTP 201."""
    response = await client.post(
        "/v1/experiments",
        json={
            "name": "exp1",
            "description": "First experiment",
            "evaluators": [{"evaluator": "accuracy"}],
        },
    )
    assert response.status_code == 201
    body = response.json()
    assert body["name"] == "exp1"
    assert body["description"] == "First experiment"
    assert body["owner_id"] == str(ACCOUNT.id)
    assert body["override"] is None
    assert body["tool_policy"] == {"default": {"type": "passthrough"}, "tools": {}}
    assert body["evaluators"] == [{"evaluator": "accuracy", "version": 1, "params": {}}]
    assert uuid.UUID(body["id"])


async def test_create_experiment_unknown_evaluator(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 when an evaluator config names an unknown evaluator."""
    response = await client.post(
        "/v1/experiments",
        json={"name": "exp1", "evaluators": [{"evaluator": "missing"}]},
    )
    assert response.status_code == 404


async def test_create_experiment_duplicate_name(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 for a duplicate experiment name."""
    body = {"name": "exp1", "evaluators": [{"evaluator": "accuracy"}]}
    response = await client.post("/v1/experiments", json=body)
    assert response.status_code == 201
    response = await client.post("/v1/experiments", json=body)
    assert response.status_code == 409
    assert response.json() == {"detail": "Experiment name 'exp1' is already registered"}


async def test_create_experiment_requires_at_least_one_evaluator(
    client: httpx.AsyncClient,
) -> None:
    """Observe HTTP 422 when no evaluator is given."""
    response = await client.post(
        "/v1/experiments", json={"name": "exp1", "evaluators": []}
    )
    assert response.status_code == 422


async def test_get_experiment(client: httpx.AsyncClient) -> None:
    """Get an experiment by id."""
    created = (
        await client.post(
            "/v1/experiments",
            json={"name": "exp1", "evaluators": [{"evaluator": "accuracy"}]},
        )
    ).json()
    response = await client.get(f"/v1/experiments/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created


async def test_get_experiment_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a missing experiment."""
    response = await client.get(f"/v1/experiments/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_list_experiments(client: httpx.AsyncClient) -> None:
    """List experiments newest-first with a name filter."""
    for name in ["assistant-eval", "reviewer-eval"]:
        await client.post(
            "/v1/experiments",
            json={"name": name, "evaluators": [{"evaluator": "accuracy"}]},
        )

    response = await client.get("/v1/experiments")
    assert response.status_code == 200
    body = response.json()
    assert body["next_cursor"] is None
    assert [item["name"] for item in body["items"]] == [
        "reviewer-eval",
        "assistant-eval",
    ]

    filter_expression = {"field": "name", "op": "eq", "value": "assistant-eval"}
    response = await client.get(
        "/v1/experiments", params={"filter": json.dumps(filter_expression)}
    )
    assert response.status_code == 200
    assert response.json()["items"][0]["name"] == "assistant-eval"


async def test_update_experiment_name(client: httpx.AsyncClient) -> None:
    """Update an experiment's name."""
    created = (
        await client.post(
            "/v1/experiments",
            json={"name": "exp1", "evaluators": [{"evaluator": "accuracy"}]},
        )
    ).json()
    response = await client.patch(
        f"/v1/experiments/{created['id']}", json={"name": "renamed"}
    )
    assert response.status_code == 200
    assert response.json()["name"] == "renamed"


async def test_update_experiment_cannot_clear_name(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 when clearing the experiment name."""
    created = (
        await client.post(
            "/v1/experiments",
            json={"name": "exp1", "evaluators": [{"evaluator": "accuracy"}]},
        )
    ).json()
    response = await client.patch(
        f"/v1/experiments/{created['id']}", json={"name": None}
    )
    assert response.status_code == 422


async def test_update_experiment_override_explicit_null_clears(
    client: httpx.AsyncClient,
) -> None:
    """Clear the override with an explicit null."""
    created = (
        await client.post(
            "/v1/experiments",
            json={
                "name": "exp1",
                "override": {"prompt": "hi"},
                "evaluators": [{"evaluator": "accuracy"}],
            },
        )
    ).json()
    assert created["override"] == {
        "model": None,
        "system_prompt": None,
        "prompt": "hi",
        "model_params": None,
    }
    response = await client.patch(
        f"/v1/experiments/{created['id']}", json={"override": None}
    )
    assert response.status_code == 200
    assert response.json()["override"] is None


async def test_update_experiment_new_evaluators_replaces_config(
    client: httpx.AsyncClient, plugin_repository: FakePluginRepository
) -> None:
    """Build a new replay config when the evaluators change."""
    relevance = await create_plugin(
        plugin_repository, ACCOUNT.id, kind=PluginKind.EVALUATOR, name="relevance"
    )
    await plugin_repository.create_version(relevance.id, SOURCE, display_version="v1")

    created = (
        await client.post(
            "/v1/experiments",
            json={"name": "exp1", "evaluators": [{"evaluator": "accuracy"}]},
        )
    ).json()
    response = await client.patch(
        f"/v1/experiments/{created['id']}",
        json={"evaluators": [{"evaluator": "relevance"}]},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["evaluators"] == [
        {"evaluator": "relevance", "version": 1, "params": {}}
    ]


async def test_update_experiment_cannot_clear_evaluators(
    client: httpx.AsyncClient,
) -> None:
    """Observe HTTP 422 when clearing every evaluator."""
    created = (
        await client.post(
            "/v1/experiments",
            json={"name": "exp1", "evaluators": [{"evaluator": "accuracy"}]},
        )
    ).json()
    response = await client.patch(
        f"/v1/experiments/{created['id']}", json={"evaluators": []}
    )
    assert response.status_code == 422


async def test_update_experiment_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a missing experiment."""
    response = await client.patch(
        f"/v1/experiments/{uuid.uuid4()}", json={"description": "x"}
    )
    assert response.status_code == 404


async def test_delete_experiment(client: httpx.AsyncClient) -> None:
    """Delete an experiment."""
    created = (
        await client.post(
            "/v1/experiments",
            json={"name": "exp1", "evaluators": [{"evaluator": "accuracy"}]},
        )
    ).json()
    response = await client.delete(f"/v1/experiments/{created['id']}")
    assert response.status_code == 204
    response = await client.get(f"/v1/experiments/{created['id']}")
    assert response.status_code == 404


async def test_delete_experiment_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a missing experiment."""
    response = await client.delete(f"/v1/experiments/{uuid.uuid4()}")
    assert response.status_code == 404
