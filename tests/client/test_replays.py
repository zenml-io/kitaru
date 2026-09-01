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
"""Round-trip tests for the replays SDK resource."""

import uuid
from collections.abc import AsyncGenerator

import pytest

from conftest import (
    ReplayServices,
    asgi_api_client,
    build_replay_services,
    create_agent,
    create_agent_version,
    create_blob,
    create_plugin,
    create_session,
    override_idempotency,
)
from kitaru.api_models.v1.replay import (
    ReplayCreateRequest,
    ReplayListParams,
    ReplayResponse,
    ToolLookupRequest,
)
from kitaru.api_models.v1.replay_config import EvaluatorConfig
from kitaru.api_models.v1.session import SessionOrigin, SessionStatus
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError, NotFoundError
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    authorize_with_task,
    get_replay_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent_version import RunSpec
from kitaru.server.domain.plugin import PluginKind, ScriptPluginSource

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


@pytest.fixture
def services() -> ReplayServices:
    """Provide fake-backed replay, experiment, and run services."""
    return build_replay_services()


@pytest.fixture
async def api_client(services: ReplayServices) -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide an API client routed to the app with fake-backed services."""
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )
    app.dependency_overrides[get_replay_service] = lambda: services.replay_service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    app.dependency_overrides[authorize_with_task] = lambda: AuthContext(account=ACCOUNT)
    override_idempotency(app, ACCOUNT)
    async with asgi_api_client(app) as client:
        yield client


@pytest.fixture
async def baseline_session_id(services: ReplayServices) -> uuid.UUID:
    """Provide a recorded session with a runnable agent version to replay."""
    agent = await create_agent(services.agents, ACCOUNT.id)
    version = await create_agent_version(
        services.agent_versions,
        agent_id=agent.id,
        owner_id=ACCOUNT.id,
        run_spec=RunSpec(command="run.sh"),
    )
    plugin = await create_plugin(
        services.plugins, ACCOUNT.id, kind=PluginKind.EVALUATOR, name="accuracy"
    )
    blob = await create_blob(services.blobs, ACCOUNT.id, content=b"score")
    await services.plugins.create_version(
        plugin.id,
        ScriptPluginSource(blob_id=blob.id, entrypoint="score"),
        display_version=None,
    )
    session = await create_session(
        services.sessions,
        ACCOUNT.id,
        agent_id=agent.id,
        agent_version_id=version.id,
        origin=SessionOrigin.RECORDED,
        status=SessionStatus.COMPLETED,
        inputs={"q": "hi"},
    )
    return session.id


@pytest.fixture
def evaluator_config() -> EvaluatorConfig:
    """Provide a config naming the fixture's registered evaluator."""
    return EvaluatorConfig(evaluator="accuracy")


async def test_create(
    api_client: KitaruAPIClient,
    baseline_session_id: uuid.UUID,
    evaluator_config: EvaluatorConfig,
) -> None:
    """Create a replay through the SDK."""
    replay = await api_client.replays.create(
        ReplayCreateRequest(
            baseline_session_id=baseline_session_id,
            evaluators=[evaluator_config],
        )
    )
    assert isinstance(replay, ReplayResponse)
    assert replay.baseline_session_id == baseline_session_id
    assert replay.status.value == "pending"


async def test_create_unknown_baseline_session(
    api_client: KitaruAPIClient, evaluator_config: EvaluatorConfig
) -> None:
    """Surface HTTP 404 as a typed error for a missing baseline session."""
    with pytest.raises(NotFoundError):
        await api_client.replays.create(
            ReplayCreateRequest(
                baseline_session_id=uuid.uuid4(),
                evaluators=[evaluator_config],
            )
        )


async def test_get(
    api_client: KitaruAPIClient,
    baseline_session_id: uuid.UUID,
    evaluator_config: EvaluatorConfig,
) -> None:
    """Get a replay by id through the SDK."""
    created = await api_client.replays.create(
        ReplayCreateRequest(
            baseline_session_id=baseline_session_id,
            evaluators=[evaluator_config],
        )
    )
    loaded = await api_client.replays.get(created.id)
    assert loaded == created


async def test_get_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.replays.get(uuid.uuid4())


async def test_list_and_iter(
    api_client: KitaruAPIClient,
    baseline_session_id: uuid.UUID,
    evaluator_config: EvaluatorConfig,
) -> None:
    """List and iterate replays through the SDK."""
    await api_client.replays.create(
        ReplayCreateRequest(
            baseline_session_id=baseline_session_id,
            evaluators=[evaluator_config],
        )
    )
    page = await api_client.replays.list(ReplayListParams())
    assert len(page.items) == 1

    collected = [item async for item in api_client.replays.iter()]
    assert len(collected) == 1


async def test_delete(
    api_client: KitaruAPIClient,
    baseline_session_id: uuid.UUID,
    evaluator_config: EvaluatorConfig,
) -> None:
    """Delete a replay through the SDK."""
    created = await api_client.replays.create(
        ReplayCreateRequest(
            baseline_session_id=baseline_session_id,
            evaluators=[evaluator_config],
        )
    )
    await api_client.replays.delete(created.id)
    with pytest.raises(NotFoundError):
        await api_client.replays.get(created.id)


async def test_tool_lookup_not_configured_for_history(
    api_client: KitaruAPIClient,
    baseline_session_id: uuid.UUID,
    evaluator_config: EvaluatorConfig,
) -> None:
    """Surface HTTP 422 as a typed error when a tool has no history config."""
    created = await api_client.replays.create(
        ReplayCreateRequest(
            baseline_session_id=baseline_session_id,
            evaluators=[evaluator_config],
        )
    )
    with pytest.raises(APIError) as excinfo:
        await api_client.replays.tool_lookup(
            created.id, ToolLookupRequest(tool_name="search", cache_key="a" * 64)
        )
    assert excinfo.value.status_code == 422
