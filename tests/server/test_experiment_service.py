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
"""Tests for experiment use cases."""

import uuid
from typing import Any

import pytest

from conftest import (
    FakeExperimentRepository,
    FakePluginRepository,
    ReplayServices,
    build_replay_services,
    create_agent,
    create_experiment_run,
    create_plugin,
    create_replay,
)
from kitaru.analytics.events import AnalyticsEvent
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.job import JobKind, JobStatus
from kitaru.api_models.v1.replay_config import HistoryScope, ToolPolicyOnMiss
from kitaru.api_models.v1.tag import TagResourceType
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.experiment import (
    ExperimentCreate,
    ExperimentFilter,
    ExperimentUpdate,
)
from kitaru.server.application.models.replay_config import EvaluatorConfigInput
from kitaru.server.application.services.experiment_service import ExperimentService
from kitaru.server.application.services.server_analytics import ServerAnalytics
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import AgentNotFound
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.experiment import (
    DuplicateExperimentName,
    ExperimentFrozen,
    ExperimentNotFound,
)
from kitaru.server.domain.experiment_run import ExperimentRunNotFound
from kitaru.server.domain.job import Job
from kitaru.server.domain.plugin import PackagePluginSource, PluginKind, PluginNotFound
from kitaru.server.domain.replay_config import (
    HistoryConfig,
    PassthroughConfig,
    ReplayConfigNotFound,
    ReplayOverride,
    ToolPolicy,
)
from kitaru.server.domain.tag import Tag, TagLink
from kitaru.server.filtering import FilterCondition

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))

SOURCE = PackagePluginSource(requirement="kitaru-scorer==1.0.0", entrypoint="pkg:score")


class _RecordingAnalytics(ServerAnalytics):
    """Analytics tracker recording track calls instead of buffering them."""

    def __init__(self) -> None:
        """Initialize the tracker."""
        self.tracked: list[tuple[uuid.UUID, AnalyticsEvent | str, dict[str, Any]]] = []

    def track(
        self,
        user_id: uuid.UUID,
        event: AnalyticsEvent | str,
        properties: dict[str, Any] | None = None,
    ) -> None:
        """Record a track call instead of buffering it.

        Args:
            user_id: User id.
            event: Event name.
            properties: Event properties.
        """
        self.tracked.append((user_id, event, properties or {}))


@pytest.fixture
def services() -> ReplayServices:
    """Provide fake-backed experiment, replay, and run services."""
    return build_replay_services()


@pytest.fixture
def plugin_repository(services: ReplayServices) -> FakePluginRepository:
    """Provide a fake plugin repository."""
    return services.plugins


@pytest.fixture
def repository(services: ReplayServices) -> FakeExperimentRepository:
    """Provide a fake experiment repository."""
    return services.experiments


@pytest.fixture
def service(services: ReplayServices) -> ExperimentService:
    """Provide an experiment service backed by the fake repositories."""
    return services.experiment_service


@pytest.fixture
async def agent_id(services: ReplayServices) -> uuid.UUID:
    """Provide an agent for experiments to belong to."""
    agent = await create_agent(services.agents, ACTOR.account.id)
    return agent.id


async def _register_evaluator(
    plugin_repository: FakePluginRepository,
    name: str = "accuracy",
    agent_id: uuid.UUID | None = None,
) -> uuid.UUID:
    plugin = await create_plugin(
        plugin_repository,
        ACTOR.account.id,
        kind=PluginKind.EVALUATOR,
        name=name,
        agent_id=agent_id,
    )
    version = await plugin_repository.create_version(
        plugin.id, SOURCE, display_version="v1"
    )
    return version.id


async def test_create_experiment_resolves_evaluators(
    service: ExperimentService,
    plugin_repository: FakePluginRepository,
    agent_id: uuid.UUID,
) -> None:
    """Resolve evaluators and inline the replay config in the response."""
    version_id = await _register_evaluator(plugin_repository)
    command = ExperimentCreate(
        name="exp1",
        agent_id=agent_id,
        evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
    )
    experiment, config = await service.create_experiment(command, actor=ACTOR)
    assert experiment.name == "exp1"
    assert experiment.owner_id == ACTOR.account.id
    assert experiment.replay_config_id == config.id
    assert config.owner_id == ACTOR.account.id
    assert config.override is None
    assert config.tool_policy == ToolPolicy(default=PassthroughConfig())
    assert len(config.evaluators) == 1
    assert config.evaluators[0].evaluator == "accuracy"
    assert config.evaluators[0].version == 1
    assert config.evaluators[0].evaluator_version_id == version_id


async def test_create_experiment_with_override_and_tool_policy(
    service: ExperimentService,
    plugin_repository: FakePluginRepository,
    agent_id: uuid.UUID,
) -> None:
    """Store an explicit override and tool policy."""
    await _register_evaluator(plugin_repository)
    override = ReplayOverride(prompt="new prompt")
    tool_policy = ToolPolicy(default=PassthroughConfig())
    command = ExperimentCreate(
        name="exp1",
        agent_id=agent_id,
        override=override,
        tool_policy=tool_policy,
        evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
    )
    _, config = await service.create_experiment(command, actor=ACTOR)
    assert config.override == override
    assert config.tool_policy == tool_policy


async def test_create_experiment_duplicate_name(
    service: ExperimentService,
    plugin_repository: FakePluginRepository,
    agent_id: uuid.UUID,
) -> None:
    """Reject a second experiment with the same name."""
    await _register_evaluator(plugin_repository)
    command = ExperimentCreate(
        name="exp1",
        agent_id=agent_id,
        evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
    )
    await service.create_experiment(command, actor=ACTOR)
    with pytest.raises(DuplicateExperimentName):
        await service.create_experiment(command, actor=ACTOR)


async def test_create_experiment_unknown_evaluator(
    service: ExperimentService, agent_id: uuid.UUID
) -> None:
    """Raise when an evaluator config names an unknown evaluator."""
    command = ExperimentCreate(
        name="exp1",
        agent_id=agent_id,
        evaluators=[EvaluatorConfigInput(evaluator="missing")],
    )
    with pytest.raises(PluginNotFound):
        await service.create_experiment(command, actor=ACTOR)


async def test_create_experiment_unknown_agent(
    service: ExperimentService, plugin_repository: FakePluginRepository
) -> None:
    """Raise when the command names an unknown agent."""
    await _register_evaluator(plugin_repository)
    command = ExperimentCreate(
        name="exp1",
        agent_id=uuid.uuid4(),
        evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
    )
    with pytest.raises(AgentNotFound):
        await service.create_experiment(command, actor=ACTOR)


async def test_create_experiment_deleted_agent(
    service: ExperimentService,
    services: ReplayServices,
    plugin_repository: FakePluginRepository,
    agent_id: uuid.UUID,
) -> None:
    """Raise when the command names a deleted agent."""
    await _register_evaluator(plugin_repository)
    await services.agents.mark_deleted(agent_id)
    command = ExperimentCreate(
        name="exp1",
        agent_id=agent_id,
        evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
    )
    with pytest.raises(AgentNotFound):
        await service.create_experiment(command, actor=ACTOR)


async def test_create_experiment_duplicate_evaluator_version(
    service: ExperimentService,
    plugin_repository: FakePluginRepository,
    agent_id: uuid.UUID,
) -> None:
    """Reject two evaluator configs resolving to the same version."""
    await _register_evaluator(plugin_repository)
    command = ExperimentCreate(
        name="exp1",
        agent_id=agent_id,
        evaluators=[
            EvaluatorConfigInput(evaluator="accuracy", version=1),
            EvaluatorConfigInput(evaluator="accuracy"),
        ],
    )
    with pytest.raises(ValidationError, match="appears more than once"):
        await service.create_experiment(command, actor=ACTOR)


async def test_create_experiment_evaluator_scoped_to_other_agent(
    service: ExperimentService,
    plugin_repository: FakePluginRepository,
    agent_id: uuid.UUID,
) -> None:
    """Reject an evaluator scoped to an agent other than the experiment's."""
    await _register_evaluator(plugin_repository, agent_id=uuid.uuid4())
    command = ExperimentCreate(
        name="exp1",
        agent_id=agent_id,
        evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
    )
    with pytest.raises(ValidationError, match="scoped to a different agent"):
        await service.create_experiment(command, actor=ACTOR)


async def test_create_experiment_evaluator_scoped_to_same_agent(
    service: ExperimentService,
    plugin_repository: FakePluginRepository,
    agent_id: uuid.UUID,
) -> None:
    """Resolve an evaluator scoped to the experiment's own agent."""
    await _register_evaluator(plugin_repository, agent_id=agent_id)
    command = ExperimentCreate(
        name="exp1",
        agent_id=agent_id,
        evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
    )
    experiment, _ = await service.create_experiment(command, actor=ACTOR)
    assert experiment.name == "exp1"


async def test_get_experiment(
    service: ExperimentService,
    plugin_repository: FakePluginRepository,
    agent_id: uuid.UUID,
) -> None:
    """Load a stored experiment and its replay config by id."""
    await _register_evaluator(plugin_repository)
    command = ExperimentCreate(
        name="exp1",
        agent_id=agent_id,
        evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
    )
    created, created_config = await service.create_experiment(command, actor=ACTOR)
    loaded, loaded_config = await service.get_experiment(created.id, actor=ACTOR)
    assert loaded == created
    assert loaded_config == created_config


async def test_get_experiment_not_found(service: ExperimentService) -> None:
    """Raise for an unknown experiment id."""
    with pytest.raises(ExperimentNotFound):
        await service.get_experiment(uuid.uuid4(), actor=ACTOR)


async def test_list_experiments(
    service: ExperimentService,
    plugin_repository: FakePluginRepository,
    agent_id: uuid.UUID,
) -> None:
    """List experiments newest-first with a name filter."""
    await _register_evaluator(plugin_repository)
    for name in ["assistant", "reviewer"]:
        command = ExperimentCreate(
            name=name,
            agent_id=agent_id,
            evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
        )
        await service.create_experiment(command, actor=ACTOR)

    pairs, next_cursor = await service.list_experiments(ExperimentFilter(), actor=ACTOR)
    assert next_cursor is None
    assert [experiment.name for experiment, _ in pairs] == ["reviewer", "assistant"]
    for _, config in pairs:
        assert config.evaluators[0].evaluator == "accuracy"


async def test_list_experiments_skips_concurrently_deleted_config(
    service: ExperimentService,
    repository: FakeExperimentRepository,
    plugin_repository: FakePluginRepository,
    agent_id: uuid.UUID,
) -> None:
    """Skip experiments whose replay config a concurrent delete removed."""
    await _register_evaluator(plugin_repository)
    kept, _ = await service.create_experiment(
        ExperimentCreate(
            name="kept",
            agent_id=agent_id,
            evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
        ),
        actor=ACTOR,
    )
    _, racing_config = await service.create_experiment(
        ExperimentCreate(
            name="racing",
            agent_id=agent_id,
            evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
        ),
        actor=ACTOR,
    )
    await repository.delete_replay_config(racing_config.id)

    pairs, next_cursor = await service.list_experiments(ExperimentFilter(), actor=ACTOR)
    assert next_cursor is None
    assert [experiment.id for experiment, _ in pairs] == [kept.id]


async def test_list_experiments_by_agent(
    services: ReplayServices,
    service: ExperimentService,
    plugin_repository: FakePluginRepository,
    agent_id: uuid.UUID,
) -> None:
    """Filter experiments by agent id."""
    await _register_evaluator(plugin_repository)
    other_agent = await create_agent(services.agents, ACTOR.account.id, name="other")
    await service.create_experiment(
        ExperimentCreate(
            name="mine",
            agent_id=agent_id,
            evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
        ),
        actor=ACTOR,
    )
    await service.create_experiment(
        ExperimentCreate(
            name="theirs",
            agent_id=other_agent.id,
            evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
        ),
        actor=ACTOR,
    )

    pairs, next_cursor = await service.list_experiments(
        ExperimentFilter(
            expression=FilterCondition(field="agent_id", op=FilterOp.EQ, value=agent_id)
        ),
        actor=ACTOR,
    )
    assert next_cursor is None
    assert [experiment.name for experiment, _ in pairs] == ["mine"]


async def test_list_experiments_by_tag(
    services: ReplayServices,
    service: ExperimentService,
    plugin_repository: FakePluginRepository,
    agent_id: uuid.UUID,
) -> None:
    """Filter experiments by a tag linked to the resource."""
    tag_repository = services.tags
    await _register_evaluator(plugin_repository)
    tagged_command = ExperimentCreate(
        name="tagged",
        agent_id=agent_id,
        evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
    )
    untagged_command = ExperimentCreate(
        name="untagged",
        agent_id=agent_id,
        evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
    )
    tagged, _ = await service.create_experiment(tagged_command, actor=ACTOR)
    await service.create_experiment(untagged_command, actor=ACTOR)

    tag = await tag_repository.create(Tag(owner_id=ACTOR.account.id, name="smoke"))
    await tag_repository.create_link(
        TagLink(
            tag_id=tag.id,
            resource_type=TagResourceType.EXPERIMENT,
            resource_id=tagged.id,
        )
    )

    pairs, next_cursor = await service.list_experiments(
        ExperimentFilter(
            expression=FilterCondition(field="tag", op=FilterOp.EQ, value="smoke")
        ),
        actor=ACTOR,
    )
    assert next_cursor is None
    assert [experiment.name for experiment, _ in pairs] == ["tagged"]


async def test_update_experiment_name(
    service: ExperimentService,
    plugin_repository: FakePluginRepository,
    agent_id: uuid.UUID,
) -> None:
    """Update an experiment's name without touching its replay config."""
    await _register_evaluator(plugin_repository)
    command = ExperimentCreate(
        name="exp1",
        agent_id=agent_id,
        evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
    )
    created, created_config = await service.create_experiment(command, actor=ACTOR)
    updated, updated_config = await service.update_experiment(
        created.id, ExperimentUpdate(name="renamed"), actor=ACTOR
    )
    assert updated.name == "renamed"
    assert updated_config.id == created_config.id


async def test_update_experiment_cannot_clear_name(
    service: ExperimentService,
    plugin_repository: FakePluginRepository,
    agent_id: uuid.UUID,
) -> None:
    """Reject clearing the experiment name with an explicit null."""
    await _register_evaluator(plugin_repository)
    command = ExperimentCreate(
        name="exp1",
        agent_id=agent_id,
        evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
    )
    created, _ = await service.create_experiment(command, actor=ACTOR)
    with pytest.raises(ValidationError, match="Experiment name cannot be cleared"):
        await service.update_experiment(
            created.id, ExperimentUpdate(name=None), actor=ACTOR
        )


async def test_update_experiment_new_evaluators_replaces_config(
    service: ExperimentService,
    plugin_repository: FakePluginRepository,
    repository: FakeExperimentRepository,
    agent_id: uuid.UUID,
) -> None:
    """Build a new replay config and delete the old one when evaluators change."""
    await _register_evaluator(plugin_repository)
    await _register_evaluator(plugin_repository, name="relevance")
    command = ExperimentCreate(
        name="exp1",
        agent_id=agent_id,
        evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
    )
    created, old_config = await service.create_experiment(command, actor=ACTOR)

    updated, new_config = await service.update_experiment(
        created.id,
        ExperimentUpdate(evaluators=[EvaluatorConfigInput(evaluator="relevance")]),
        actor=ACTOR,
    )
    assert new_config.id != old_config.id
    assert updated.replay_config_id == new_config.id
    assert new_config.evaluators[0].evaluator == "relevance"
    with pytest.raises(ReplayConfigNotFound):
        await repository.get_replay_config(old_config.id)


async def test_update_experiment_old_config_survives_when_a_replay_references_it(
    service: ExperimentService,
    plugin_repository: FakePluginRepository,
    repository: FakeExperimentRepository,
    services: ReplayServices,
    agent_id: uuid.UUID,
) -> None:
    """Keep the old replay config when a replay still points at it."""
    await _register_evaluator(plugin_repository)
    await _register_evaluator(plugin_repository, name="relevance")
    command = ExperimentCreate(
        name="exp1",
        agent_id=agent_id,
        evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
    )
    created, old_config = await service.create_experiment(command, actor=ACTOR)
    await create_replay(
        services.replays,
        ACTOR.account.id,
        job_id=uuid.uuid4(),
        replay_config_id=old_config.id,
        baseline_session_id=uuid.uuid4(),
    )

    await service.update_experiment(
        created.id,
        ExperimentUpdate(evaluators=[EvaluatorConfigInput(evaluator="relevance")]),
        actor=ACTOR,
    )
    # Survives because a replay still references it, unlike the ordinary case.
    survived = await repository.get_replay_config(old_config.id)
    assert survived.id == old_config.id


async def test_update_experiment_cannot_clear_evaluators(
    service: ExperimentService,
    plugin_repository: FakePluginRepository,
    agent_id: uuid.UUID,
) -> None:
    """Reject clearing every evaluator with an explicit null or empty list."""
    await _register_evaluator(plugin_repository)
    command = ExperimentCreate(
        name="exp1",
        agent_id=agent_id,
        evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
    )
    created, _ = await service.create_experiment(command, actor=ACTOR)
    with pytest.raises(ValidationError, match="evaluators cannot be cleared"):
        await service.update_experiment(
            created.id, ExperimentUpdate(evaluators=[]), actor=ACTOR
        )


async def test_update_experiment_cannot_clear_tool_policy(
    service: ExperimentService,
    plugin_repository: FakePluginRepository,
    agent_id: uuid.UUID,
) -> None:
    """Reject clearing the tool policy with an explicit null."""
    await _register_evaluator(plugin_repository)
    command = ExperimentCreate(
        name="exp1",
        agent_id=agent_id,
        evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
    )
    created, _ = await service.create_experiment(command, actor=ACTOR)
    with pytest.raises(ValidationError, match="tool policy cannot be cleared"):
        await service.update_experiment(
            created.id, ExperimentUpdate(tool_policy=None), actor=ACTOR
        )


async def test_update_experiment_explicit_null_override_clears(
    service: ExperimentService,
    plugin_repository: FakePluginRepository,
    agent_id: uuid.UUID,
) -> None:
    """Clear the override with an explicit null."""
    await _register_evaluator(plugin_repository)
    command = ExperimentCreate(
        name="exp1",
        agent_id=agent_id,
        override=ReplayOverride(prompt="hi"),
        evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
    )
    created, _ = await service.create_experiment(command, actor=ACTOR)
    _, config = await service.update_experiment(
        created.id, ExperimentUpdate(override=None), actor=ACTOR
    )
    assert config.override is None


async def test_update_experiment_omitted_fields_unchanged(
    service: ExperimentService,
    plugin_repository: FakePluginRepository,
    agent_id: uuid.UUID,
) -> None:
    """Leave the replay config untouched when the command sets none of it."""
    await _register_evaluator(plugin_repository)
    command = ExperimentCreate(
        name="exp1",
        agent_id=agent_id,
        override=ReplayOverride(prompt="hi"),
        evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
    )
    created, created_config = await service.create_experiment(command, actor=ACTOR)
    updated, config = await service.update_experiment(
        created.id, ExperimentUpdate(description="new"), actor=ACTOR
    )
    assert updated.description == "new"
    assert config.id == created_config.id
    assert config.override == created_config.override


async def test_delete_experiment_removes_config(
    service: ExperimentService,
    plugin_repository: FakePluginRepository,
    repository: FakeExperimentRepository,
    agent_id: uuid.UUID,
) -> None:
    """Delete an experiment and its replay config."""
    await _register_evaluator(plugin_repository)
    command = ExperimentCreate(
        name="exp1",
        agent_id=agent_id,
        evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
    )
    created, config = await service.create_experiment(command, actor=ACTOR)
    await service.delete_experiment(created.id, actor=ACTOR)
    with pytest.raises(ExperimentNotFound):
        await service.get_experiment(created.id, actor=ACTOR)
    with pytest.raises(ReplayConfigNotFound):
        await repository.get_replay_config(config.id)


async def test_delete_experiment_not_found(service: ExperimentService) -> None:
    """Raise for an unknown experiment id."""
    with pytest.raises(ExperimentNotFound):
        await service.delete_experiment(uuid.uuid4(), actor=ACTOR)


async def test_update_experiment_config_frozen_once_it_has_runs(
    service: ExperimentService,
    plugin_repository: FakePluginRepository,
    services: ReplayServices,
    agent_id: uuid.UUID,
) -> None:
    """Reject a replay config update once the experiment has a run."""
    await _register_evaluator(plugin_repository)
    await _register_evaluator(plugin_repository, name="relevance")
    command = ExperimentCreate(
        name="exp1",
        agent_id=agent_id,
        evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
    )
    created, _ = await service.create_experiment(command, actor=ACTOR)
    await create_experiment_run(
        services.experiment_runs,
        ACTOR.account.id,
        experiment_id=created.id,
        cohort_version_id=uuid.uuid4(),
        agent_version_id=uuid.uuid4(),
    )
    with pytest.raises(ExperimentFrozen):
        await service.update_experiment(
            created.id,
            ExperimentUpdate(evaluators=[EvaluatorConfigInput(evaluator="relevance")]),
            actor=ACTOR,
        )


async def test_update_experiment_name_unaffected_by_runs(
    service: ExperimentService,
    plugin_repository: FakePluginRepository,
    services: ReplayServices,
    agent_id: uuid.UUID,
) -> None:
    """A name-only update stays legal once the experiment has a run."""
    await _register_evaluator(plugin_repository)
    command = ExperimentCreate(
        name="exp1",
        agent_id=agent_id,
        evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
    )
    created, _ = await service.create_experiment(command, actor=ACTOR)
    await create_experiment_run(
        services.experiment_runs,
        ACTOR.account.id,
        experiment_id=created.id,
        cohort_version_id=uuid.uuid4(),
        agent_version_id=uuid.uuid4(),
    )
    updated, _ = await service.update_experiment(
        created.id, ExperimentUpdate(name="renamed"), actor=ACTOR
    )
    assert updated.name == "renamed"


async def test_delete_experiment_cascades_runs(
    service: ExperimentService,
    plugin_repository: FakePluginRepository,
    services: ReplayServices,
    agent_id: uuid.UUID,
) -> None:
    """Deleting an experiment cascades its runs."""
    await _register_evaluator(plugin_repository)
    command = ExperimentCreate(
        name="exp1",
        agent_id=agent_id,
        evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
    )
    created, _ = await service.create_experiment(command, actor=ACTOR)
    run = await create_experiment_run(
        services.experiment_runs,
        ACTOR.account.id,
        experiment_id=created.id,
        cohort_version_id=uuid.uuid4(),
        agent_version_id=uuid.uuid4(),
    )

    await service.delete_experiment(created.id, actor=ACTOR)

    with pytest.raises(ExperimentNotFound):
        await service.get_experiment(created.id, actor=ACTOR)
    with pytest.raises(ExperimentRunNotFound):
        await services.experiment_runs.get(run.id)


async def test_delete_experiment_cancels_replay_jobs(
    service: ExperimentService,
    plugin_repository: FakePluginRepository,
    services: ReplayServices,
    agent_id: uuid.UUID,
) -> None:
    """Deleting an experiment cancels its runs' replay jobs and leaves them stored."""
    await _register_evaluator(plugin_repository)
    command = ExperimentCreate(
        name="exp1",
        agent_id=agent_id,
        evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
    )
    created, config = await service.create_experiment(command, actor=ACTOR)
    run = await create_experiment_run(
        services.experiment_runs,
        ACTOR.account.id,
        experiment_id=created.id,
        cohort_version_id=uuid.uuid4(),
        agent_version_id=uuid.uuid4(),
    )
    job = await services.jobs.create(
        Job(owner_id=ACTOR.account.id, kind=JobKind.REPLAY, status=JobStatus.PENDING)
    )
    await create_replay(
        services.replays,
        ACTOR.account.id,
        job_id=job.id,
        replay_config_id=config.id,
        baseline_session_id=uuid.uuid4(),
        experiment_run_id=run.id,
    )

    await service.delete_experiment(created.id, actor=ACTOR)

    kept = await services.jobs.get(job.id)
    assert kept.cancel_requested_at is not None


async def test_create_experiment_tracks_experiment_created(
    services: ReplayServices,
    plugin_repository: FakePluginRepository,
    agent_id: uuid.UUID,
) -> None:
    """Fire EXPERIMENT_CREATED with the override, tool policy and evaluator info."""
    await _register_evaluator(plugin_repository)
    analytics = _RecordingAnalytics()
    service = ExperimentService(
        repository=services.experiments,
        plugin_repository=services.plugins,
        experiment_run_repository=services.experiment_runs,
        agent_repository=services.agents,
        cohort_version_repository=services.cohort_versions,
        session_repository=services.sessions,
        agent_version_repository=services.agent_versions,
        replay_repository=services.replays,
        job_repository=services.jobs,
        task_repository=services.tasks,
        evaluation_repository=services.evaluations,
        transitions=services.transitions,
        payload_store=services.payload_store,
        analytics=analytics,
    )
    command = ExperimentCreate(
        name="exp1",
        agent_id=agent_id,
        evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
        override=ReplayOverride(system_prompt="be terse"),
        tool_policy=ToolPolicy(
            default=PassthroughConfig(),
            tools={
                "search": HistoryConfig(
                    scope=HistoryScope.BASELINE, on_miss=ToolPolicyOnMiss.FAIL
                )
            },
        ),
    )

    await service.create_experiment(command, actor=ACTOR)

    assert len(analytics.tracked) == 1
    user_id, event, properties = analytics.tracked[0]
    assert user_id == ACTOR.account.id
    assert event == AnalyticsEvent.EXPERIMENT_CREATED
    assert properties == {
        "model_override": False,
        "system_prompt_override": True,
        "prompt_override": False,
        "model_params_override": False,
        "tool_policy_default": "passthrough",
        "tool_override_count": 1,
        "tool_override_types": ["history"],
        "evaluator_count": 1,
    }


async def test_create_experiment_without_analytics_tracker(
    service: ExperimentService,
    plugin_repository: FakePluginRepository,
    agent_id: uuid.UUID,
) -> None:
    """Create an experiment normally when no analytics tracker is configured."""
    await _register_evaluator(plugin_repository)
    command = ExperimentCreate(
        name="exp1",
        agent_id=agent_id,
        evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
    )
    experiment, _ = await service.create_experiment(command, actor=ACTOR)
    assert experiment.owner_id == ACTOR.account.id
