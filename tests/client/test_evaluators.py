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
"""Round-trip tests for the evaluators SDK resource."""

import uuid
from collections.abc import AsyncGenerator

import pytest

from conftest import FakeBlobRepository, FakePluginRepository, asgi_api_client
from kitaru.api_models.v1.evaluator import (
    EvaluatorCreateRequest,
    EvaluatorUpdateRequest,
    EvaluatorVersionCreateRequest,
    EvaluatorVersionUpdateRequest,
)
from kitaru.api_models.v1.plugin import PackagePluginSource
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError, NotFoundError
from kitaru.server.adapters.rest.dependencies import authorize, get_evaluator_service
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.plugin_service import PluginService
from kitaru.server.domain.account import Account
from kitaru.server.domain.plugin import PluginKind

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


@pytest.fixture
async def api_client() -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide an API client routed to the app with a fake-backed service."""
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )
    service = PluginService(
        kind=PluginKind.EVALUATOR,
        repository=FakePluginRepository(),
        blob_repository=FakeBlobRepository(),
    )
    app.dependency_overrides[get_evaluator_service] = lambda: service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    async with asgi_api_client(app) as client:
        yield client


async def test_create(api_client: KitaruAPIClient) -> None:
    """Create an evaluator through the SDK."""
    evaluator = await api_client.evaluators.create(
        EvaluatorCreateRequest(name="accuracy", metadata={"a": 1})
    )
    assert evaluator.name == "accuracy"
    assert evaluator.metadata == {"a": 1}


async def test_create_duplicate_name(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 409 as a typed error."""
    await api_client.evaluators.create(EvaluatorCreateRequest(name="accuracy"))
    with pytest.raises(APIError) as exc_info:
        await api_client.evaluators.create(EvaluatorCreateRequest(name="accuracy"))
    assert exc_info.value.status_code == 409


async def test_get(api_client: KitaruAPIClient) -> None:
    """Get an evaluator by id through the SDK."""
    created = await api_client.evaluators.create(
        EvaluatorCreateRequest(name="accuracy")
    )
    loaded = await api_client.evaluators.get(created.id)
    assert loaded == created


async def test_get_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.evaluators.get(uuid.uuid4())


async def test_list_and_iter(api_client: KitaruAPIClient) -> None:
    """List and iterate evaluators through the SDK."""
    for name in ["accuracy", "relevance"]:
        await api_client.evaluators.create(EvaluatorCreateRequest(name=name))

    page = await api_client.evaluators.list()
    assert [item.name for item in page.items] == ["relevance", "accuracy"]

    collected = [item.name async for item in api_client.evaluators.iter()]
    assert collected == ["relevance", "accuracy"]


async def test_update(api_client: KitaruAPIClient) -> None:
    """Update an evaluator through the SDK."""
    created = await api_client.evaluators.create(
        EvaluatorCreateRequest(name="accuracy")
    )
    updated = await api_client.evaluators.update(
        created.id, EvaluatorUpdateRequest(description="Scores accuracy")
    )
    assert updated.description == "Scores accuracy"


async def test_delete(api_client: KitaruAPIClient) -> None:
    """Delete an evaluator through the SDK."""
    created = await api_client.evaluators.create(
        EvaluatorCreateRequest(name="accuracy")
    )
    await api_client.evaluators.delete(created.id)
    with pytest.raises(NotFoundError):
        await api_client.evaluators.get(created.id)


async def test_create_and_get_version(api_client: KitaruAPIClient) -> None:
    """Create and get an evaluator version through the SDK."""
    created = await api_client.evaluators.create(
        EvaluatorCreateRequest(name="accuracy")
    )
    version = await api_client.evaluators.create_version(
        created.id,
        EvaluatorVersionCreateRequest(
            source=PackagePluginSource(
                requirement="kitaru-scorer==1.0.0", entrypoint="pkg:score"
            ),
            display_version="v1",
        ),
    )
    assert version.version == 1
    assert version.evaluator_id == created.id

    loaded = await api_client.evaluators.get_version(created.id, version.version)
    assert loaded == version


async def test_list_and_iter_versions(api_client: KitaruAPIClient) -> None:
    """List and iterate an evaluator's versions through the SDK."""
    created = await api_client.evaluators.create(
        EvaluatorCreateRequest(name="accuracy")
    )
    request = EvaluatorVersionCreateRequest(
        source=PackagePluginSource(
            requirement="kitaru-scorer==1.0.0", entrypoint="pkg:score"
        )
    )
    await api_client.evaluators.create_version(created.id, request)
    await api_client.evaluators.create_version(created.id, request)

    page = await api_client.evaluators.list_versions(created.id)
    assert sorted(item.version for item in page.items) == [1, 2]

    collected = [
        item.version async for item in api_client.evaluators.iter_versions(created.id)
    ]
    assert sorted(collected) == [1, 2]


async def test_update_version(api_client: KitaruAPIClient) -> None:
    """Update an evaluator version's display version through the SDK."""
    created = await api_client.evaluators.create(
        EvaluatorCreateRequest(name="accuracy")
    )
    version = await api_client.evaluators.create_version(
        created.id,
        EvaluatorVersionCreateRequest(
            source=PackagePluginSource(
                requirement="kitaru-scorer==1.0.0", entrypoint="pkg:score"
            ),
            display_version="v1",
        ),
    )
    updated = await api_client.evaluators.update_version(
        created.id,
        version.version,
        EvaluatorVersionUpdateRequest(display_version="v1.0.1"),
    )
    assert updated.display_version == "v1.0.1"
