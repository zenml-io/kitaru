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
"""Round-trip tests for the experiments SDK resource."""

import uuid
from collections.abc import AsyncGenerator

import pytest

from conftest import (
    FakePluginRepository,
    ReplayServices,
    asgi_api_client,
    build_replay_services,
    create_agent,
    create_plugin,
    override_idempotency,
)
from kitaru.api_models.v1.experiment import (
    ExperimentCreateRequest,
    ExperimentListParams,
    ExperimentResponse,
    ExperimentUpdateRequest,
)
from kitaru.api_models.v1.filter import FilterCondition, FilterOp
from kitaru.api_models.v1.replay_config import EvaluatorConfig, ReplayOverride
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError, NotFoundError
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
    """Provide a fake plugin repository."""
    return services.plugins


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
    app.dependency_overrides[get_experiment_service] = lambda: (
        services.experiment_service
    )
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    override_idempotency(app, ACCOUNT)
    async with asgi_api_client(app) as client:
        yield client


@pytest.fixture
async def agent_id(services: ReplayServices) -> uuid.UUID:
    """Provide an agent for experiments to belong to."""
    agent = await create_agent(services.agents, ACCOUNT.id)
    return agent.id


@pytest.fixture
async def evaluator_config(plugin_repository: FakePluginRepository) -> EvaluatorConfig:
    """Register an evaluator plugin and return a config naming it."""
    plugin = await create_plugin(
        plugin_repository, ACCOUNT.id, kind=PluginKind.EVALUATOR, name="accuracy"
    )
    await plugin_repository.create_version(plugin.id, SOURCE, display_version="v1")
    return EvaluatorConfig(evaluator="accuracy")


async def test_create(
    api_client: KitaruAPIClient, evaluator_config: EvaluatorConfig, agent_id: uuid.UUID
) -> None:
    """Create an experiment through the SDK."""
    experiment = await api_client.experiments.create(
        ExperimentCreateRequest(
            name="exp1",
            agent_id=agent_id,
            description="First run",
            evaluators=[evaluator_config],
        )
    )
    assert isinstance(experiment, ExperimentResponse)
    assert experiment.name == "exp1"
    assert experiment.owner_id == ACCOUNT.id
    assert experiment.description == "First run"
    assert experiment.evaluators[0].evaluator == "accuracy"
    assert experiment.evaluators[0].version == 1


async def test_create_duplicate_name(
    api_client: KitaruAPIClient, evaluator_config: EvaluatorConfig, agent_id: uuid.UUID
) -> None:
    """Surface HTTP 409 as a typed error."""
    request = ExperimentCreateRequest(
        name="exp1", agent_id=agent_id, evaluators=[evaluator_config]
    )
    await api_client.experiments.create(request)
    with pytest.raises(APIError) as exc_info:
        await api_client.experiments.create(request)
    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == "Experiment name 'exp1' is already registered"


async def test_get(
    api_client: KitaruAPIClient, evaluator_config: EvaluatorConfig, agent_id: uuid.UUID
) -> None:
    """Get an experiment by id through the SDK."""
    created = await api_client.experiments.create(
        ExperimentCreateRequest(
            name="exp1", agent_id=agent_id, evaluators=[evaluator_config]
        )
    )
    loaded = await api_client.experiments.get(created.id)
    assert loaded == created


async def test_get_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.experiments.get(uuid.uuid4())


async def test_list(
    api_client: KitaruAPIClient, evaluator_config: EvaluatorConfig, agent_id: uuid.UUID
) -> None:
    """List experiments newest-first with filters through the SDK."""
    for name in ["assistant-eval", "reviewer-eval"]:
        await api_client.experiments.create(
            ExperimentCreateRequest(
                name=name, agent_id=agent_id, evaluators=[evaluator_config]
            )
        )

    page = await api_client.experiments.list()
    assert page.next_cursor is None
    assert [item.name for item in page.items] == ["reviewer-eval", "assistant-eval"]

    page = await api_client.experiments.list(
        ExperimentListParams(
            filter=FilterCondition(field="name", op=FilterOp.EQ, value="assistant-eval")
        )
    )
    assert page.next_cursor is None
    assert page.items[0].name == "assistant-eval"


async def test_iter(
    api_client: KitaruAPIClient, evaluator_config: EvaluatorConfig, agent_id: uuid.UUID
) -> None:
    """Iterate every experiment across pages through the SDK."""
    for name in ["assistant-eval", "reviewer-eval", "triager-eval"]:
        await api_client.experiments.create(
            ExperimentCreateRequest(
                name=name, agent_id=agent_id, evaluators=[evaluator_config]
            )
        )

    collected = [
        item.name
        async for item in api_client.experiments.iter(ExperimentListParams(size=2))
    ]

    assert collected == ["triager-eval", "reviewer-eval", "assistant-eval"]


async def test_update(
    api_client: KitaruAPIClient, evaluator_config: EvaluatorConfig, agent_id: uuid.UUID
) -> None:
    """Update an experiment through the SDK."""
    created = await api_client.experiments.create(
        ExperimentCreateRequest(
            name="exp1", agent_id=agent_id, evaluators=[evaluator_config]
        )
    )
    updated = await api_client.experiments.update(
        created.id, ExperimentUpdateRequest(description="Reviews")
    )
    assert updated.description == "Reviews"


async def test_update_clears_override(
    api_client: KitaruAPIClient, evaluator_config: EvaluatorConfig, agent_id: uuid.UUID
) -> None:
    """Clear an experiment's override with an explicit null."""
    created = await api_client.experiments.create(
        ExperimentCreateRequest(
            name="exp1",
            agent_id=agent_id,
            override=ReplayOverride(prompt="hi"),
            evaluators=[evaluator_config],
        )
    )
    assert created.override is not None
    updated = await api_client.experiments.update(
        created.id, ExperimentUpdateRequest(override=None)
    )
    assert updated.override is None


async def test_delete(
    api_client: KitaruAPIClient, evaluator_config: EvaluatorConfig, agent_id: uuid.UUID
) -> None:
    """Delete an experiment through the SDK."""
    created = await api_client.experiments.create(
        ExperimentCreateRequest(
            name="exp1", agent_id=agent_id, evaluators=[evaluator_config]
        )
    )
    await api_client.experiments.delete(created.id)
    with pytest.raises(NotFoundError):
        await api_client.experiments.get(created.id)
