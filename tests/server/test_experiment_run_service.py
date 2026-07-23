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
"""Tests for experiment run use cases."""

import uuid

import pytest

from conftest import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeCohortRepository,
    FakeExperimentRepository,
    FakeExperimentRunRepository,
    FakeReplayConfigRepository,
    FakeReplayRepository,
    FakeSessionRepository,
    FakeTagRepository,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.experiment_runs import (
    ExperimentRunFilter,
    ExperimentRunReplaysFilter,
)
from kitaru.server.application.models.experiments import ExperimentCreate
from kitaru.server.application.services.experiment_run_service import (
    ExperimentRunService,
)
from kitaru.server.application.services.experiment_service import (
    ExperimentService,
)
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.agent_version import AgentVersion, RunSpec
from kitaru.server.domain.cohort import Cohort
from kitaru.server.domain.experiment import Experiment, ExperimentNotFound
from kitaru.server.domain.experiment_run import ExperimentRunNotFound
from kitaru.server.domain.replay import ReplayStatus
from kitaru.server.domain.replay_config import (
    ScorerConfig,
    ScoringPolicy,
    SourceRef,
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
def experiment_repository(
    cohort_repository: FakeCohortRepository,
    config_repository: FakeReplayConfigRepository,
    tag_repository: FakeTagRepository,
) -> FakeExperimentRepository:
    """Provide a fake experiment repository."""
    return FakeExperimentRepository(
        cohort_repository, config_repository, tag_repository
    )


@pytest.fixture
def replay_repository(
    session_repository: FakeSessionRepository,
    version_repository: FakeAgentVersionRepository,
    config_repository: FakeReplayConfigRepository,
) -> FakeReplayRepository:
    """Provide a fake replay repository."""
    return FakeReplayRepository(
        session_repository, version_repository, config_repository
    )


@pytest.fixture
def repository(
    experiment_repository: FakeExperimentRepository,
    replay_repository: FakeReplayRepository,
    tag_repository: FakeTagRepository,
) -> FakeExperimentRunRepository:
    """Provide a fake experiment run repository."""
    return FakeExperimentRunRepository(
        experiment_repository, replay_repository, tag_repository
    )


@pytest.fixture
def service(
    repository: FakeExperimentRunRepository,
    replay_repository: FakeReplayRepository,
    config_repository: FakeReplayConfigRepository,
    experiment_repository: FakeExperimentRepository,
) -> ExperimentRunService:
    """Provide an experiment run service backed by the fake repositories."""
    return ExperimentRunService(
        repository=repository,
        replay_repository=replay_repository,
        replay_config_repository=config_repository,
        experiment_repository=experiment_repository,
    )


@pytest.fixture
def experiment_service(
    experiment_repository: FakeExperimentRepository,
    repository: FakeExperimentRunRepository,
    cohort_repository: FakeCohortRepository,
    version_repository: FakeAgentVersionRepository,
    config_repository: FakeReplayConfigRepository,
) -> ExperimentService:
    """Provide an experiment service backed by the fake repositories."""
    return ExperimentService(
        repository=experiment_repository,
        run_repository=repository,
        cohort_repository=cohort_repository,
        agent_version_repository=version_repository,
        replay_config_repository=config_repository,
    )


@pytest.fixture
async def experiment(
    experiment_service: ExperimentService,
    agent_repository: FakeAgentRepository,
    version_repository: FakeAgentVersionRepository,
    session_repository: FakeSessionRepository,
    cohort_repository: FakeCohortRepository,
) -> Experiment:
    """Provide a stored experiment over a two-session cohort."""
    agent = await agent_repository.create(
        Agent(owner_id=ACTOR.account.id, name="support-bot")
    )
    await version_repository.create(
        AgentVersion(
            owner_id=ACTOR.account.id,
            agent_id=agent.id,
            version="v1",
            run_spec=RunSpec(command="python agent.py", timeout_seconds=600),
        )
    )
    sessions = [
        await session_repository.create(
            Session(
                owner_id=ACTOR.account.id,
                agent_id=agent.id,
                origin=SessionOrigin.RECORDED,
                status=SessionStatus.COMPLETED,
            )
        )
        for _ in range(2)
    ]
    cohort = await cohort_repository.create(
        Cohort(
            owner_id=ACTOR.account.id,
            name="baseline",
            agent_id=agent.id,
            session_count=2,
        ),
        [session.id for session in sessions],
    )
    experiment, _ = await experiment_service.create_experiment(
        ExperimentCreate(
            name="swap-model", cohort_id=cohort.id, scoring_policy=SCORING_POLICY
        ),
        actor=ACTOR,
    )
    return experiment


async def test_get_run_with_progress(
    service: ExperimentRunService,
    experiment_service: ExperimentService,
    experiment: Experiment,
) -> None:
    """Load a run with its computed replay counts."""
    created, _ = await experiment_service.start_run(
        experiment.id, agent_version_id=None, score_baselines=False, actor=ACTOR
    )
    run, progress = await service.get_run(created.id, actor=ACTOR)
    assert run == created
    assert progress.pending == 2
    assert progress.completed == 0
    assert progress.total == 2


async def test_get_run_not_found(service: ExperimentRunService) -> None:
    """Raise for an unknown experiment run id."""
    missing_id = uuid.uuid4()
    with pytest.raises(
        ExperimentRunNotFound, match=f"Experiment run {missing_id} was not found"
    ):
        await service.get_run(missing_id, actor=ACTOR)


async def test_list_runs(
    service: ExperimentRunService,
    experiment_service: ExperimentService,
    experiment: Experiment,
) -> None:
    """List runs globally and per experiment."""
    first, _ = await experiment_service.start_run(
        experiment.id, agent_version_id=None, score_baselines=False, actor=ACTOR
    )
    second, _ = await experiment_service.start_run(
        experiment.id, agent_version_id=None, score_baselines=False, actor=ACTOR
    )

    runs, total = await service.list_runs(ExperimentRunFilter(), actor=ACTOR)
    assert total == 2
    assert [run.id for run, _ in runs] == [first.id, second.id]
    assert all(progress.total == 2 for _, progress in runs)

    runs, total = await service.list_runs(
        ExperimentRunFilter(experiment_id=experiment.id, page=2, page_size=1),
        actor=ACTOR,
    )
    assert total == 2
    assert [run.id for run, _ in runs] == [second.id]


async def test_list_runs_unknown_experiment(service: ExperimentRunService) -> None:
    """Raise for an unknown filtered experiment id."""
    missing_id = uuid.uuid4()
    with pytest.raises(
        ExperimentNotFound, match=f"Experiment {missing_id} was not found"
    ):
        await service.list_runs(
            ExperimentRunFilter(experiment_id=missing_id), actor=ACTOR
        )


async def test_list_runs_by_tag(
    service: ExperimentRunService,
    experiment_service: ExperimentService,
    tag_repository: FakeTagRepository,
    experiment: Experiment,
) -> None:
    """List runs attached to a tag name."""
    tagged, _ = await experiment_service.start_run(
        experiment.id, agent_version_id=None, score_baselines=False, actor=ACTOR
    )
    await experiment_service.start_run(
        experiment.id, agent_version_id=None, score_baselines=False, actor=ACTOR
    )
    tag = await tag_repository.create(Tag(owner_id=ACTOR.account.id, name="prod"))
    await tag_repository.create_link(
        TagLink(
            tag_id=tag.id,
            resource_type=TagResourceType.EXPERIMENT_RUN,
            resource_id=tagged.id,
        )
    )

    runs, total = await service.list_runs(ExperimentRunFilter(tag="prod"), actor=ACTOR)
    assert total == 1
    assert runs[0][0].id == tagged.id

    runs, total = await service.list_runs(
        ExperimentRunFilter(tag="missing"), actor=ACTOR
    )
    assert total == 0


async def test_list_run_replays(
    service: ExperimentRunService,
    experiment_service: ExperimentService,
    experiment: Experiment,
) -> None:
    """List the replays of a run with their inlined config."""
    created, _ = await experiment_service.start_run(
        experiment.id, agent_version_id=None, score_baselines=False, actor=ACTOR
    )
    replays, total = await service.list_run_replays(
        created.id, ExperimentRunReplaysFilter(), actor=ACTOR
    )
    assert total == 2
    for replay, config in replays:
        assert replay.experiment_run_id == created.id
        assert replay.status is ReplayStatus.PENDING
        assert config.id == replay.replay_config_id
        assert config.scoring_policy == SCORING_POLICY

    replays, total = await service.list_run_replays(
        created.id, ExperimentRunReplaysFilter(page=2, page_size=1), actor=ACTOR
    )
    assert total == 2
    assert len(replays) == 1


async def test_list_run_replays_not_found(service: ExperimentRunService) -> None:
    """Raise for an unknown experiment run id."""
    missing_id = uuid.uuid4()
    with pytest.raises(
        ExperimentRunNotFound, match=f"Experiment run {missing_id} was not found"
    ):
        await service.list_run_replays(
            missing_id, ExperimentRunReplaysFilter(), actor=ACTOR
        )
