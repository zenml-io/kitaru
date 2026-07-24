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

from conftest import asgi_api_client, experiment_app
from kitaru.api_models.v1.agent_versions import (
    AgentVersionCreateRequest,
    ExecutionTarget,
    RunSpec,
)
from kitaru.api_models.v1.agents import AgentCreateRequest
from kitaru.api_models.v1.cohorts import CohortCreateRequest
from kitaru.api_models.v1.experiment_runs import ExperimentRunCreateRequest
from kitaru.api_models.v1.experiments import (
    ExperimentCreateRequest,
    ExperimentResponse,
    ExperimentUpdateRequest,
)
from kitaru.api_models.v1.replays import (
    PassthroughPolicy,
    ReplayOverride,
    ScorerConfig,
    ScoringPolicy,
    ToolPolicyConfig,
)
from kitaru.api_models.v1.sessions import (
    SessionCreateRequest,
    SessionOrigin,
    SessionStatus,
    SessionUpdateRequest,
)
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError, NotFoundError

SCORING_POLICY = ScoringPolicy(
    scorers=[ScorerConfig(name="conciseness", source="my_pkg.scorers:conciseness")],
    pass_threshold=0.5,
)


@pytest.fixture
async def api_client() -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide an API client routed to the app with fake-backed services."""
    async with asgi_api_client(experiment_app()) as client:
        yield client


async def create_cohort(api_client: KitaruAPIClient) -> tuple[uuid.UUID, uuid.UUID]:
    """Store an agent, a runnable version, and a cohort through the SDK.

    Args:
        api_client: API client routed to the app.

    Returns:
        Ids of the created cohort and agent version.
    """
    agent = await api_client.agents.create(AgentCreateRequest(name="support-bot"))
    version = await api_client.agent_versions.create(
        agent.id,
        AgentVersionCreateRequest(
            version="v1",
            run_spec=RunSpec(
                command="python agent.py",
                timeout_seconds=600,
                image="ghcr.io/acme/agent:v1",
            ),
        ),
    )
    session = await api_client.sessions.create(
        SessionCreateRequest(agent_id=agent.id, origin=SessionOrigin.RECORDED)
    )
    await api_client.sessions.update(
        session.id, SessionUpdateRequest(status=SessionStatus.COMPLETED)
    )
    cohort = await api_client.cohorts.create(
        CohortCreateRequest(
            name="baseline", agent_id=agent.id, session_ids=[session.id]
        )
    )
    return cohort.id, version.id


async def create_experiment(
    api_client: KitaruAPIClient, cohort_id: uuid.UUID, name: str = "swap-model"
) -> ExperimentResponse:
    """Store an experiment through the SDK.

    Args:
        api_client: API client routed to the app.
        cohort_id: Id of the cohort.
        name: Experiment name.

    Returns:
        Created experiment.
    """
    return await api_client.experiments.create(
        ExperimentCreateRequest(
            name=name, cohort_id=cohort_id, scoring_policy=SCORING_POLICY
        )
    )


async def test_create_get_update_delete_round_trip(
    api_client: KitaruAPIClient,
) -> None:
    """Round-trip an experiment through create, get, update, and delete."""
    cohort_id, _ = await create_cohort(api_client)
    created = await api_client.experiments.create(
        ExperimentCreateRequest(
            name="swap-model",
            description="Swap the model",
            cohort_id=cohort_id,
            override=ReplayOverride(model="claude-sonnet-5"),
            scoring_policy=SCORING_POLICY,
        )
    )
    assert created.name == "swap-model"
    assert created.cohort_id == cohort_id
    assert created.override is not None
    assert created.override.model == "claude-sonnet-5"
    assert created.tool_policy == ToolPolicyConfig(default=PassthroughPolicy())
    assert created.scoring_policy == SCORING_POLICY

    loaded = await api_client.experiments.get(created.id)
    assert loaded == created

    updated = await api_client.experiments.update(
        created.id, ExperimentUpdateRequest(name="renamed")
    )
    assert updated.name == "renamed"

    await api_client.experiments.delete(created.id)
    with pytest.raises(NotFoundError):
        await api_client.experiments.get(created.id)


async def test_list_experiments(api_client: KitaruAPIClient) -> None:
    """List experiments with a name filter."""
    cohort_id, _ = await create_cohort(api_client)
    await create_experiment(api_client, cohort_id, name="one")
    await create_experiment(api_client, cohort_id, name="two")

    page = await api_client.experiments.list()
    assert page.total == 2
    assert [item.name for item in page.items] == ["one", "two"]

    page = await api_client.experiments.list(name="two")
    assert page.total == 1


async def test_duplicate_name_raises(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 409 as an APIError."""
    cohort_id, _ = await create_cohort(api_client)
    await create_experiment(api_client, cohort_id)
    with pytest.raises(APIError) as exc_info:
        await create_experiment(api_client, cohort_id)
    assert exc_info.value.status_code == 409


async def test_create_and_list_runs(api_client: KitaruAPIClient) -> None:
    """Start runs and list them through the SDK."""
    cohort_id, version_id = await create_cohort(api_client)
    created = await create_experiment(api_client, cohort_id)
    run = await api_client.experiments.create_run(
        created.id, ExperimentRunCreateRequest(score_baselines=True)
    )
    assert run.experiment_id == created.id
    assert run.number == 1
    assert run.agent_version_id == version_id
    assert run.score_baselines is True
    assert run.execution_target is ExecutionTarget.POOL
    assert run.executor_handle is None
    assert run.progress.pending == 1
    assert run.progress.total == 1

    on_demand = await api_client.experiments.create_run(
        created.id,
        ExperimentRunCreateRequest(execution_target=ExecutionTarget.ON_DEMAND),
    )
    assert on_demand.execution_target is ExecutionTarget.ON_DEMAND

    page = await api_client.experiments.list_runs(created.id)
    assert page.total == 2
    assert page.items[0].id == run.id
