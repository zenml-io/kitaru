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

import pytest

from conftest import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeCohortRepository,
    FakeExperimentRepository,
    FakeExperimentRunRepository,
    FakeJobRepository,
    FakeReplayConfigRepository,
    FakeSessionRepository,
    FakeTagRepository,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.experiments import (
    ExperimentCreate,
    ExperimentFilter,
    ExperimentUpdate,
)
from kitaru.server.application.models.jobs import JobFilter
from kitaru.server.application.services.experiment_service import (
    ExperimentService,
)
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.agent_version import (
    AgentVersion,
    AgentVersionNotFound,
    AgentVersionNotRunnable,
    MissingRunImage,
    NoRunnableAgentVersion,
    RunSpec,
)
from kitaru.server.domain.cohort import Cohort, CohortNotFound
from kitaru.server.domain.execution import ExecutionTarget
from kitaru.server.domain.experiment import (
    DuplicateExperimentName,
    ExperimentFrozen,
    ExperimentInUse,
    ExperimentNotFound,
    InvalidExperiment,
)
from kitaru.server.domain.experiment_run import (
    ExperimentRunStatus,
    InvalidExperimentRun,
)
from kitaru.server.domain.job import JobStatus
from kitaru.server.domain.replay_config import (
    HistoryPolicy,
    PassthroughPolicy,
    ReplayConfigNotFound,
    ReplayOverride,
    ScorerConfig,
    ScoringPolicy,
    SourceRef,
    ToolPolicyConfig,
)
from kitaru.server.domain.session import Session, SessionOrigin, SessionStatus
from kitaru.server.domain.tag import Tag, TagLink, TagResourceType

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))

SCORING_POLICY = ScoringPolicy(
    scorers=[
        ScorerConfig(
            name="conciseness",
            source=SourceRef(module="my_pkg.scorers", attribute="conciseness"),
        )
    ],
    pass_threshold=0.5,
)


@pytest.fixture
def agent_repository() -> FakeAgentRepository:
    """Provide a fake agent repository."""
    return FakeAgentRepository()


@pytest.fixture
def tag_repository() -> FakeTagRepository:
    """Provide a fake tag repository."""
    return FakeTagRepository()


@pytest.fixture
def version_repository(
    agent_repository: FakeAgentRepository,
) -> FakeAgentVersionRepository:
    """Provide a fake agent version repository."""
    return FakeAgentVersionRepository(agent_repository)


@pytest.fixture
def session_repository(
    agent_repository: FakeAgentRepository,
    version_repository: FakeAgentVersionRepository,
    tag_repository: FakeTagRepository,
) -> FakeSessionRepository:
    """Provide a fake session repository."""
    return FakeSessionRepository(agent_repository, version_repository, tag_repository)


@pytest.fixture
def cohort_repository(
    session_repository: FakeSessionRepository,
    agent_repository: FakeAgentRepository,
    tag_repository: FakeTagRepository,
) -> FakeCohortRepository:
    """Provide a fake cohort repository."""
    return FakeCohortRepository(session_repository, agent_repository, tag_repository)


@pytest.fixture
def config_repository() -> FakeReplayConfigRepository:
    """Provide a fake replay config repository."""
    return FakeReplayConfigRepository()


@pytest.fixture
def repository(
    cohort_repository: FakeCohortRepository,
    config_repository: FakeReplayConfigRepository,
    tag_repository: FakeTagRepository,
) -> FakeExperimentRepository:
    """Provide a fake experiment repository."""
    return FakeExperimentRepository(
        cohort_repository, config_repository, tag_repository
    )


@pytest.fixture
def job_repository(
    session_repository: FakeSessionRepository,
    version_repository: FakeAgentVersionRepository,
    config_repository: FakeReplayConfigRepository,
) -> FakeJobRepository:
    """Provide a fake job repository."""
    return FakeJobRepository(session_repository, version_repository, config_repository)


@pytest.fixture
def run_repository(
    repository: FakeExperimentRepository,
    job_repository: FakeJobRepository,
    tag_repository: FakeTagRepository,
) -> FakeExperimentRunRepository:
    """Provide a fake experiment run repository."""
    return FakeExperimentRunRepository(repository, job_repository, tag_repository)


@pytest.fixture
def service(
    repository: FakeExperimentRepository,
    run_repository: FakeExperimentRunRepository,
    cohort_repository: FakeCohortRepository,
    version_repository: FakeAgentVersionRepository,
    config_repository: FakeReplayConfigRepository,
) -> ExperimentService:
    """Provide an experiment service backed by the fake repositories."""
    return ExperimentService(
        repository=repository,
        run_repository=run_repository,
        cohort_repository=cohort_repository,
        agent_version_repository=version_repository,
        replay_config_repository=config_repository,
    )


@pytest.fixture
async def agent(agent_repository: FakeAgentRepository) -> Agent:
    """Provide a stored agent."""
    return await agent_repository.create(
        Agent(owner_id=ACTOR.account.id, name="support-bot")
    )


async def create_runnable_version(
    repository: FakeAgentVersionRepository,
    agent_id: uuid.UUID,
    version: str = "v1",
    run_spec: RunSpec | None = None,
) -> AgentVersion:
    """Store a runnable agent version.

    Args:
        repository: Fake agent version repository.
        agent_id: Id of the agent.
        version: Version label.
        run_spec: Run specification, ``None`` uses a minimal spec.

    Returns:
        Stored agent version.
    """
    return await repository.create(
        AgentVersion(
            owner_id=ACTOR.account.id,
            agent_id=agent_id,
            version=version,
            run_spec=run_spec
            or RunSpec(command="python agent.py", timeout_seconds=600),
        )
    )


async def create_cohort(
    cohort_repository: FakeCohortRepository,
    session_repository: FakeSessionRepository,
    agent_id: uuid.UUID,
    session_count: int = 1,
    name: str = "baseline",
) -> tuple[Cohort, list[Session]]:
    """Store a cohort with completed member sessions.

    Args:
        cohort_repository: Fake cohort repository.
        session_repository: Fake session repository.
        agent_id: Id of the agent.
        session_count: Number of member sessions.
        name: Cohort name.

    Returns:
        Stored cohort and its member sessions.
    """
    sessions = [
        await session_repository.create(
            Session(
                owner_id=ACTOR.account.id,
                agent_id=agent_id,
                origin=SessionOrigin.RECORDED,
                status=SessionStatus.COMPLETED,
            )
        )
        for _ in range(session_count)
    ]
    cohort = await cohort_repository.create(
        Cohort(
            owner_id=ACTOR.account.id,
            name=name,
            agent_id=agent_id,
            session_count=session_count,
        ),
        [session.id for session in sessions],
    )
    return cohort, sessions


def experiment_create(cohort_id: uuid.UUID, **overrides: object) -> ExperimentCreate:
    """Build an experiment create command.

    Args:
        cohort_id: Id of the cohort.
        **overrides: Field overrides.

    Returns:
        Experiment create command.
    """
    values: dict[str, object] = {
        "name": "swap-model",
        "cohort_id": cohort_id,
        "scoring_policy": SCORING_POLICY,
        **overrides,
    }
    return ExperimentCreate.model_validate(values)


async def test_create_experiment_defaults(
    service: ExperimentService,
    cohort_repository: FakeCohortRepository,
    session_repository: FakeSessionRepository,
    agent: Agent,
) -> None:
    """Create an experiment and default the tool policy to passthrough."""
    cohort, _ = await create_cohort(cohort_repository, session_repository, agent.id)
    experiment, config = await service.create_experiment(
        experiment_create(cohort.id, description="Swap the model"), actor=ACTOR
    )
    assert experiment.owner_id == ACTOR.account.id
    assert experiment.name == "swap-model"
    assert experiment.description == "Swap the model"
    assert experiment.cohort_id == cohort.id
    assert experiment.replay_config_id == config.id
    assert experiment.created is not None
    assert experiment.updated is not None
    assert config.override is None
    assert config.tool_policy == ToolPolicyConfig(default=PassthroughPolicy())
    assert config.scoring_policy == SCORING_POLICY


async def test_create_experiment_with_config(
    service: ExperimentService,
    cohort_repository: FakeCohortRepository,
    session_repository: FakeSessionRepository,
    config_repository: FakeReplayConfigRepository,
    agent: Agent,
) -> None:
    """Normalize the inline config into a replay config row."""
    cohort, _ = await create_cohort(cohort_repository, session_repository, agent.id)
    override = ReplayOverride(model={"gpt-4o": "claude-sonnet-5"})
    tool_policy = ToolPolicyConfig(
        default=HistoryPolicy(), tools={"search": PassthroughPolicy()}
    )
    _, config = await service.create_experiment(
        experiment_create(cohort.id, override=override, tool_policy=tool_policy),
        actor=ACTOR,
    )
    stored = await config_repository.get(config.id)
    assert stored.override == override
    assert stored.tool_policy == tool_policy
    assert stored.scoring_policy == SCORING_POLICY


async def test_create_experiment_unknown_cohort(service: ExperimentService) -> None:
    """Raise for an unknown cohort id."""
    missing_id = uuid.uuid4()
    with pytest.raises(CohortNotFound, match=f"Cohort {missing_id} was not found"):
        await service.create_experiment(experiment_create(missing_id), actor=ACTOR)


async def test_create_experiment_duplicate_name(
    service: ExperimentService,
    cohort_repository: FakeCohortRepository,
    session_repository: FakeSessionRepository,
    agent: Agent,
) -> None:
    """Reject a second experiment with the same name."""
    cohort, _ = await create_cohort(cohort_repository, session_repository, agent.id)
    await service.create_experiment(experiment_create(cohort.id), actor=ACTOR)
    with pytest.raises(
        DuplicateExperimentName,
        match="Experiment name 'swap-model' is already registered",
    ):
        await service.create_experiment(experiment_create(cohort.id), actor=ACTOR)


async def test_get_experiment(
    service: ExperimentService,
    cohort_repository: FakeCohortRepository,
    session_repository: FakeSessionRepository,
    agent: Agent,
) -> None:
    """Load a stored experiment with its config."""
    cohort, _ = await create_cohort(cohort_repository, session_repository, agent.id)
    created, created_config = await service.create_experiment(
        experiment_create(cohort.id), actor=ACTOR
    )
    experiment, config = await service.get_experiment(created.id, actor=ACTOR)
    assert experiment == created
    assert config == created_config


async def test_get_experiment_not_found(service: ExperimentService) -> None:
    """Raise for an unknown experiment id."""
    missing_id = uuid.uuid4()
    with pytest.raises(
        ExperimentNotFound, match=f"Experiment {missing_id} was not found"
    ):
        await service.get_experiment(missing_id, actor=ACTOR)


async def test_list_experiments(
    service: ExperimentService,
    cohort_repository: FakeCohortRepository,
    session_repository: FakeSessionRepository,
    agent: Agent,
) -> None:
    """List experiments with filters and pagination."""
    cohort, _ = await create_cohort(cohort_repository, session_repository, agent.id)
    for name in ["one", "two", "three"]:
        await service.create_experiment(
            experiment_create(cohort.id, name=name), actor=ACTOR
        )

    experiments, total = await service.list_experiments(ExperimentFilter(), actor=ACTOR)
    assert total == 3
    assert [experiment.name for experiment, _ in experiments] == [
        "one",
        "two",
        "three",
    ]

    experiments, total = await service.list_experiments(
        ExperimentFilter(page=2, page_size=2), actor=ACTOR
    )
    assert total == 3
    assert [experiment.name for experiment, _ in experiments] == ["three"]

    experiments, total = await service.list_experiments(
        ExperimentFilter(name="two"), actor=ACTOR
    )
    assert total == 1


async def test_list_experiments_by_tag(
    service: ExperimentService,
    cohort_repository: FakeCohortRepository,
    session_repository: FakeSessionRepository,
    tag_repository: FakeTagRepository,
    agent: Agent,
) -> None:
    """List experiments attached to a tag name."""
    cohort, _ = await create_cohort(cohort_repository, session_repository, agent.id)
    tagged, _ = await service.create_experiment(
        experiment_create(cohort.id, name="tagged"), actor=ACTOR
    )
    await service.create_experiment(
        experiment_create(cohort.id, name="other"), actor=ACTOR
    )
    tag = await tag_repository.create(Tag(owner_id=ACTOR.account.id, name="prod"))
    await tag_repository.create_link(
        TagLink(
            tag_id=tag.id,
            resource_type=TagResourceType.EXPERIMENT,
            resource_id=tagged.id,
        )
    )

    experiments, total = await service.list_experiments(
        ExperimentFilter(tag="prod"), actor=ACTOR
    )
    assert total == 1
    assert experiments[0][0].id == tagged.id


async def test_update_experiment_name_and_description(
    service: ExperimentService,
    cohort_repository: FakeCohortRepository,
    session_repository: FakeSessionRepository,
    agent: Agent,
) -> None:
    """Update name and description without touching the config."""
    cohort, _ = await create_cohort(cohort_repository, session_repository, agent.id)
    created, created_config = await service.create_experiment(
        experiment_create(cohort.id), actor=ACTOR
    )
    updated, config = await service.update_experiment(
        created.id,
        ExperimentUpdate(name="swap-model-v2", description="Second try"),
        actor=ACTOR,
    )
    assert updated.name == "swap-model-v2"
    assert updated.description == "Second try"
    assert updated.replay_config_id == created.replay_config_id
    assert config == created_config


async def test_update_experiment_absent_fields_unchanged(
    service: ExperimentService,
    cohort_repository: FakeCohortRepository,
    session_repository: FakeSessionRepository,
    agent: Agent,
) -> None:
    """Keep every field on an update without set fields."""
    cohort, _ = await create_cohort(cohort_repository, session_repository, agent.id)
    created, created_config = await service.create_experiment(
        experiment_create(cohort.id, description="First try"), actor=ACTOR
    )
    updated, config = await service.update_experiment(
        created.id, ExperimentUpdate(), actor=ACTOR
    )
    assert updated.name == "swap-model"
    assert updated.description == "First try"
    assert updated.replay_config_id == created.replay_config_id
    assert config == created_config


async def test_update_experiment_null_clears_description(
    service: ExperimentService,
    cohort_repository: FakeCohortRepository,
    session_repository: FakeSessionRepository,
    agent: Agent,
) -> None:
    """Clear the description on an explicit null."""
    cohort, _ = await create_cohort(cohort_repository, session_repository, agent.id)
    created, _ = await service.create_experiment(
        experiment_create(cohort.id, description="First try"), actor=ACTOR
    )
    updated, _ = await service.update_experiment(
        created.id, ExperimentUpdate(description=None), actor=ACTOR
    )
    assert updated.description is None


async def test_update_experiment_null_clears_override(
    service: ExperimentService,
    cohort_repository: FakeCohortRepository,
    session_repository: FakeSessionRepository,
    agent: Agent,
) -> None:
    """Clear the override on an explicit null via a new config row."""
    cohort, _ = await create_cohort(cohort_repository, session_repository, agent.id)
    created, _ = await service.create_experiment(
        experiment_create(cohort.id, override=ReplayOverride(model="claude-sonnet-5")),
        actor=ACTOR,
    )
    updated, config = await service.update_experiment(
        created.id, ExperimentUpdate(override=None), actor=ACTOR
    )
    assert updated.replay_config_id != created.replay_config_id
    assert config.override is None
    assert config.scoring_policy == SCORING_POLICY


async def test_update_experiment_null_required_fields_rejected(
    service: ExperimentService,
    cohort_repository: FakeCohortRepository,
    session_repository: FakeSessionRepository,
    agent: Agent,
) -> None:
    """Reject explicit nulls for name, cohort id, tool policy, and scoring policy."""
    cohort, _ = await create_cohort(cohort_repository, session_repository, agent.id)
    created, _ = await service.create_experiment(
        experiment_create(cohort.id), actor=ACTOR
    )
    with pytest.raises(InvalidExperiment, match="Experiment name cannot be null"):
        await service.update_experiment(
            created.id, ExperimentUpdate(name=None), actor=ACTOR
        )
    with pytest.raises(InvalidExperiment, match="Experiment cohort id cannot be null"):
        await service.update_experiment(
            created.id, ExperimentUpdate(cohort_id=None), actor=ACTOR
        )
    with pytest.raises(
        InvalidExperiment, match="Experiment tool policy cannot be null"
    ):
        await service.update_experiment(
            created.id, ExperimentUpdate(tool_policy=None), actor=ACTOR
        )
    with pytest.raises(
        InvalidExperiment, match="Experiment scoring policy cannot be null"
    ):
        await service.update_experiment(
            created.id, ExperimentUpdate(scoring_policy=None), actor=ACTOR
        )


async def test_update_experiment_config_repoints_and_collects(
    service: ExperimentService,
    cohort_repository: FakeCohortRepository,
    session_repository: FakeSessionRepository,
    config_repository: FakeReplayConfigRepository,
    agent: Agent,
) -> None:
    """Insert a new config row on a config change and delete the old row."""
    cohort, _ = await create_cohort(cohort_repository, session_repository, agent.id)
    created, _ = await service.create_experiment(
        experiment_create(cohort.id), actor=ACTOR
    )
    old_config_id = created.replay_config_id
    override = ReplayOverride(model="claude-sonnet-5")
    updated, config = await service.update_experiment(
        created.id, ExperimentUpdate(override=override), actor=ACTOR
    )
    assert updated.replay_config_id == config.id
    assert updated.replay_config_id != old_config_id
    assert config.override == override
    # Unchanged config parts carry over from the old row.
    assert config.tool_policy == ToolPolicyConfig(default=PassthroughPolicy())
    assert config.scoring_policy == SCORING_POLICY
    with pytest.raises(ReplayConfigNotFound):
        await config_repository.get(old_config_id)


async def test_update_experiment_cohort(
    service: ExperimentService,
    cohort_repository: FakeCohortRepository,
    session_repository: FakeSessionRepository,
    agent: Agent,
) -> None:
    """Repoint the experiment at another cohort before any run."""
    cohort, _ = await create_cohort(cohort_repository, session_repository, agent.id)
    other, _ = await create_cohort(
        cohort_repository, session_repository, agent.id, name="other"
    )
    created, _ = await service.create_experiment(
        experiment_create(cohort.id), actor=ACTOR
    )
    updated, _ = await service.update_experiment(
        created.id, ExperimentUpdate(cohort_id=other.id), actor=ACTOR
    )
    assert updated.cohort_id == other.id

    missing_id = uuid.uuid4()
    with pytest.raises(CohortNotFound, match=f"Cohort {missing_id} was not found"):
        await service.update_experiment(
            created.id, ExperimentUpdate(cohort_id=missing_id), actor=ACTOR
        )


async def test_update_experiment_frozen_after_run(
    service: ExperimentService,
    cohort_repository: FakeCohortRepository,
    session_repository: FakeSessionRepository,
    version_repository: FakeAgentVersionRepository,
    agent: Agent,
) -> None:
    """Reject cohort and config changes once a run exists."""
    cohort, _ = await create_cohort(cohort_repository, session_repository, agent.id)
    await create_runnable_version(version_repository, agent.id)
    created, _ = await service.create_experiment(
        experiment_create(cohort.id), actor=ACTOR
    )
    await service.start_run(
        created.id, agent_version_id=None, score_baselines=False, actor=ACTOR
    )

    frozen_message = f"Experiment {created.id} is frozen by existing runs"
    with pytest.raises(ExperimentFrozen, match=frozen_message):
        await service.update_experiment(
            created.id,
            ExperimentUpdate(override=ReplayOverride(model="claude-sonnet-5")),
            actor=ACTOR,
        )
    with pytest.raises(ExperimentFrozen, match=frozen_message):
        await service.update_experiment(
            created.id, ExperimentUpdate(cohort_id=cohort.id), actor=ACTOR
        )

    updated, _ = await service.update_experiment(
        created.id, ExperimentUpdate(name="renamed"), actor=ACTOR
    )
    assert updated.name == "renamed"


async def test_delete_experiment(
    service: ExperimentService,
    cohort_repository: FakeCohortRepository,
    session_repository: FakeSessionRepository,
    config_repository: FakeReplayConfigRepository,
    tag_repository: FakeTagRepository,
    agent: Agent,
) -> None:
    """Delete a run-less experiment with its config row and tag links."""
    cohort, _ = await create_cohort(cohort_repository, session_repository, agent.id)
    created, config = await service.create_experiment(
        experiment_create(cohort.id), actor=ACTOR
    )
    tag = await tag_repository.create(Tag(owner_id=ACTOR.account.id, name="prod"))
    await tag_repository.create_link(
        TagLink(
            tag_id=tag.id,
            resource_type=TagResourceType.EXPERIMENT,
            resource_id=created.id,
        )
    )

    await service.delete_experiment(created.id, actor=ACTOR)
    with pytest.raises(ExperimentNotFound):
        await service.get_experiment(created.id, actor=ACTOR)
    with pytest.raises(ReplayConfigNotFound):
        await config_repository.get(config.id)
    assert (
        tag_repository.linked_resource_ids("prod", TagResourceType.EXPERIMENT) == set()
    )


async def test_delete_experiment_with_runs(
    service: ExperimentService,
    cohort_repository: FakeCohortRepository,
    session_repository: FakeSessionRepository,
    version_repository: FakeAgentVersionRepository,
    agent: Agent,
) -> None:
    """Reject deleting an experiment that has runs."""
    cohort, _ = await create_cohort(cohort_repository, session_repository, agent.id)
    await create_runnable_version(version_repository, agent.id)
    created, _ = await service.create_experiment(
        experiment_create(cohort.id), actor=ACTOR
    )
    await service.start_run(
        created.id, agent_version_id=None, score_baselines=False, actor=ACTOR
    )
    with pytest.raises(
        ExperimentInUse,
        match=f"Experiment {created.id} is referenced by experiment runs",
    ):
        await service.delete_experiment(created.id, actor=ACTOR)


async def test_start_run_fans_out_jobs(
    service: ExperimentService,
    cohort_repository: FakeCohortRepository,
    session_repository: FakeSessionRepository,
    version_repository: FakeAgentVersionRepository,
    job_repository: FakeJobRepository,
    agent: Agent,
) -> None:
    """Create one pending job per cohort session with the stamped config."""
    cohort, sessions = await create_cohort(
        cohort_repository, session_repository, agent.id, session_count=3
    )
    version = await create_runnable_version(version_repository, agent.id)
    created, config = await service.create_experiment(
        experiment_create(cohort.id), actor=ACTOR
    )
    run, progress = await service.start_run(
        created.id, agent_version_id=None, score_baselines=True, actor=ACTOR
    )
    assert run.experiment_id == created.id
    assert run.number == 1
    assert run.status is ExperimentRunStatus.PENDING
    assert run.agent_version_id == version.id
    assert run.score_baselines is True
    assert run.execution_target is ExecutionTarget.POOL
    assert run.executor_handle is None
    assert progress.pending == 3
    assert progress.total == 3

    jobs, total = await job_repository.query(JobFilter(experiment_run_id=run.id))
    assert total == 3
    assert {job.original_session_id for job in jobs} == {
        session.id for session in sessions
    }
    for job in jobs:
        assert job.status is JobStatus.PENDING
        assert job.attempt == 1
        assert job.replay_config_id == config.id
        assert job.agent_version_id == version.id


async def test_start_run_increments_number(
    service: ExperimentService,
    cohort_repository: FakeCohortRepository,
    session_repository: FakeSessionRepository,
    version_repository: FakeAgentVersionRepository,
    agent: Agent,
) -> None:
    """Count run numbers per experiment."""
    cohort, _ = await create_cohort(cohort_repository, session_repository, agent.id)
    await create_runnable_version(version_repository, agent.id)
    first, _ = await service.create_experiment(
        experiment_create(cohort.id, name="first"), actor=ACTOR
    )
    second, _ = await service.create_experiment(
        experiment_create(cohort.id, name="second"), actor=ACTOR
    )
    run_one, _ = await service.start_run(
        first.id, agent_version_id=None, score_baselines=False, actor=ACTOR
    )
    run_two, _ = await service.start_run(
        first.id, agent_version_id=None, score_baselines=False, actor=ACTOR
    )
    other_run, _ = await service.start_run(
        second.id, agent_version_id=None, score_baselines=False, actor=ACTOR
    )
    assert run_one.number == 1
    assert run_two.number == 2
    assert other_run.number == 1


async def test_start_run_resolves_latest_runnable_version(
    service: ExperimentService,
    cohort_repository: FakeCohortRepository,
    session_repository: FakeSessionRepository,
    version_repository: FakeAgentVersionRepository,
    agent: Agent,
) -> None:
    """Pick the most recently created version with a run spec."""
    cohort, _ = await create_cohort(cohort_repository, session_repository, agent.id)
    await create_runnable_version(version_repository, agent.id, version="v1")
    latest = await create_runnable_version(version_repository, agent.id, version="v2")
    await version_repository.create(
        AgentVersion(owner_id=ACTOR.account.id, agent_id=agent.id, version="v3")
    )
    created, _ = await service.create_experiment(
        experiment_create(cohort.id), actor=ACTOR
    )
    run, _ = await service.start_run(
        created.id, agent_version_id=None, score_baselines=False, actor=ACTOR
    )
    assert run.agent_version_id == latest.id


async def test_start_run_no_runnable_version(
    service: ExperimentService,
    cohort_repository: FakeCohortRepository,
    session_repository: FakeSessionRepository,
    version_repository: FakeAgentVersionRepository,
    agent: Agent,
) -> None:
    """Raise when the cohort's agent has no runnable version."""
    cohort, _ = await create_cohort(cohort_repository, session_repository, agent.id)
    await version_repository.create(
        AgentVersion(owner_id=ACTOR.account.id, agent_id=agent.id, version="v1")
    )
    created, _ = await service.create_experiment(
        experiment_create(cohort.id), actor=ACTOR
    )
    with pytest.raises(
        NoRunnableAgentVersion, match=f"Agent {agent.id} has no runnable version"
    ):
        await service.start_run(
            created.id, agent_version_id=None, score_baselines=False, actor=ACTOR
        )


async def test_start_run_explicit_version(
    service: ExperimentService,
    cohort_repository: FakeCohortRepository,
    session_repository: FakeSessionRepository,
    version_repository: FakeAgentVersionRepository,
    agent: Agent,
) -> None:
    """Run an explicitly selected runnable version."""
    cohort, _ = await create_cohort(cohort_repository, session_repository, agent.id)
    pinned = await create_runnable_version(version_repository, agent.id, version="v1")
    await create_runnable_version(version_repository, agent.id, version="v2")
    created, _ = await service.create_experiment(
        experiment_create(cohort.id), actor=ACTOR
    )
    run, _ = await service.start_run(
        created.id, agent_version_id=pinned.id, score_baselines=False, actor=ACTOR
    )
    assert run.agent_version_id == pinned.id


async def test_start_run_cross_agent_version(
    service: ExperimentService,
    cohort_repository: FakeCohortRepository,
    session_repository: FakeSessionRepository,
    version_repository: FakeAgentVersionRepository,
    agent_repository: FakeAgentRepository,
    agent: Agent,
) -> None:
    """Reject a version that belongs to another agent."""
    cohort, _ = await create_cohort(cohort_repository, session_repository, agent.id)
    other = await agent_repository.create(
        Agent(owner_id=ACTOR.account.id, name="triage-bot")
    )
    version = await create_runnable_version(version_repository, other.id)
    created, _ = await service.create_experiment(
        experiment_create(cohort.id), actor=ACTOR
    )
    with pytest.raises(
        InvalidExperimentRun,
        match=f"Agent version {version.id} does not belong to agent {agent.id}",
    ):
        await service.start_run(
            created.id, agent_version_id=version.id, score_baselines=False, actor=ACTOR
        )


async def test_start_run_version_without_run_spec(
    service: ExperimentService,
    cohort_repository: FakeCohortRepository,
    session_repository: FakeSessionRepository,
    version_repository: FakeAgentVersionRepository,
    agent: Agent,
) -> None:
    """Reject an explicit version without a run spec."""
    cohort, _ = await create_cohort(cohort_repository, session_repository, agent.id)
    version = await version_repository.create(
        AgentVersion(owner_id=ACTOR.account.id, agent_id=agent.id, version="v1")
    )
    created, _ = await service.create_experiment(
        experiment_create(cohort.id), actor=ACTOR
    )
    with pytest.raises(
        AgentVersionNotRunnable, match=f"Agent version {version.id} has no run spec"
    ):
        await service.start_run(
            created.id, agent_version_id=version.id, score_baselines=False, actor=ACTOR
        )


async def test_start_run_unknown_version(
    service: ExperimentService,
    cohort_repository: FakeCohortRepository,
    session_repository: FakeSessionRepository,
    agent: Agent,
) -> None:
    """Raise for an unknown explicit version id."""
    cohort, _ = await create_cohort(cohort_repository, session_repository, agent.id)
    created, _ = await service.create_experiment(
        experiment_create(cohort.id), actor=ACTOR
    )
    missing_id = uuid.uuid4()
    with pytest.raises(
        AgentVersionNotFound, match=f"Agent version {missing_id} was not found"
    ):
        await service.start_run(
            created.id, agent_version_id=missing_id, score_baselines=False, actor=ACTOR
        )


async def test_start_run_explicit_execution_target(
    service: ExperimentService,
    cohort_repository: FakeCohortRepository,
    session_repository: FakeSessionRepository,
    version_repository: FakeAgentVersionRepository,
    agent: Agent,
) -> None:
    """Prefer the requested execution target over the run spec default."""
    cohort, _ = await create_cohort(cohort_repository, session_repository, agent.id)
    await create_runnable_version(
        version_repository,
        agent.id,
        run_spec=RunSpec(
            command="python agent.py",
            timeout_seconds=600,
            image="ghcr.io/acme/agent:v1",
        ),
    )
    created, _ = await service.create_experiment(
        experiment_create(cohort.id), actor=ACTOR
    )
    run, _ = await service.start_run(
        created.id,
        agent_version_id=None,
        score_baselines=False,
        actor=ACTOR,
        execution_target=ExecutionTarget.ON_DEMAND,
    )
    assert run.execution_target is ExecutionTarget.ON_DEMAND


async def test_start_run_execution_target_from_run_spec(
    service: ExperimentService,
    cohort_repository: FakeCohortRepository,
    session_repository: FakeSessionRepository,
    version_repository: FakeAgentVersionRepository,
    agent: Agent,
) -> None:
    """Fall back to the run spec default when no target is requested."""
    cohort, _ = await create_cohort(cohort_repository, session_repository, agent.id)
    await create_runnable_version(
        version_repository,
        agent.id,
        run_spec=RunSpec(
            command="python agent.py",
            timeout_seconds=600,
            image="ghcr.io/acme/agent:v1",
            default_execution_target=ExecutionTarget.ON_DEMAND,
        ),
    )
    created, _ = await service.create_experiment(
        experiment_create(cohort.id), actor=ACTOR
    )
    run, _ = await service.start_run(
        created.id, agent_version_id=None, score_baselines=False, actor=ACTOR
    )
    assert run.execution_target is ExecutionTarget.ON_DEMAND


async def test_start_run_on_demand_without_image(
    service: ExperimentService,
    cohort_repository: FakeCohortRepository,
    session_repository: FakeSessionRepository,
    version_repository: FakeAgentVersionRepository,
    agent: Agent,
) -> None:
    """Reject an on demand run when the version has no image."""
    cohort, _ = await create_cohort(cohort_repository, session_repository, agent.id)
    version = await create_runnable_version(version_repository, agent.id)
    created, _ = await service.create_experiment(
        experiment_create(cohort.id), actor=ACTOR
    )
    with pytest.raises(
        MissingRunImage, match=f"Agent version {version.id} has no run image"
    ):
        await service.start_run(
            created.id,
            agent_version_id=None,
            score_baselines=False,
            actor=ACTOR,
            execution_target=ExecutionTarget.ON_DEMAND,
        )
