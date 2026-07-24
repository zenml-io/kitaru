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
from kitaru.server.application.models.replays import ReplayFilter
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
from kitaru.server.domain.experiment_run import (
    ExperimentRun,
    ExperimentRunActive,
    ExperimentRunNotFound,
    ExperimentRunStatus,
    InvalidExperimentRunTransition,
)
from kitaru.server.domain.replay import HEARTBEAT_TIMEOUT_ERROR, ReplayStatus
from kitaru.server.domain.replay_config import (
    ReplayConfigNotFound,
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
    session_repository: FakeSessionRepository,
) -> ExperimentRunService:
    """Provide an experiment run service backed by the fake repositories."""
    return ExperimentRunService(
        repository=repository,
        replay_repository=replay_repository,
        replay_config_repository=config_repository,
        experiment_repository=experiment_repository,
        session_repository=session_repository,
        heartbeat_timeout_seconds=60,
        max_attempts=3,
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


def build_service(
    repository: FakeExperimentRunRepository,
    replay_repository: FakeReplayRepository,
    config_repository: FakeReplayConfigRepository,
    experiment_repository: FakeExperimentRepository,
    session_repository: FakeSessionRepository,
    heartbeat_timeout_seconds: int = 60,
    max_attempts: int = 3,
) -> ExperimentRunService:
    """Build an experiment run service with explicit staleness settings.

    Args:
        repository: Fake experiment run repository.
        replay_repository: Fake replay repository.
        config_repository: Fake replay config repository.
        experiment_repository: Fake experiment repository.
        session_repository: Fake session repository.
        heartbeat_timeout_seconds: Heartbeat timeout, negative values mark
            every claim stale immediately.
        max_attempts: Attempt count at which a stale replay times out.

    Returns:
        Experiment run service.
    """
    return ExperimentRunService(
        repository=repository,
        replay_repository=replay_repository,
        replay_config_repository=config_repository,
        experiment_repository=experiment_repository,
        session_repository=session_repository,
        heartbeat_timeout_seconds=heartbeat_timeout_seconds,
        max_attempts=max_attempts,
    )


async def test_claim_replays(
    service: ExperimentRunService,
    experiment_service: ExperimentService,
    experiment: Experiment,
) -> None:
    """Claim pending replays and move the run to running."""
    run, _ = await experiment_service.start_run(
        experiment.id, agent_version_id=None, score_baselines=False, actor=ACTOR
    )
    claimed = await service.claim_replays(
        run.id, worker_id="worker-1", max_replays=1, actor=ACTOR
    )
    assert len(claimed) == 1
    replay, config = claimed[0]
    assert replay.status is ReplayStatus.CLAIMED
    assert replay.worker_id == "worker-1"
    assert replay.claimed_at is not None
    assert config.scoring_policy == SCORING_POLICY
    started, _ = await service.get_run(run.id, actor=ACTOR)
    assert started.status is ExperimentRunStatus.RUNNING
    assert started.started_at is not None

    remaining = await service.claim_replays(
        run.id, worker_id="worker-2", max_replays=5, actor=ACTOR
    )
    assert len(remaining) == 1
    assert remaining[0][0].worker_id == "worker-2"

    assert (
        await service.claim_replays(
            run.id, worker_id="worker-2", max_replays=5, actor=ACTOR
        )
        == []
    )


async def test_claim_unknown_run(service: ExperimentRunService) -> None:
    """Raise for an unknown experiment run id."""
    missing_id = uuid.uuid4()
    with pytest.raises(
        ExperimentRunNotFound, match=f"Experiment run {missing_id} was not found"
    ):
        await service.claim_replays(
            missing_id, worker_id="worker-1", max_replays=1, actor=ACTOR
        )


async def test_claim_canceling_run_returns_empty(
    service: ExperimentRunService,
    experiment_service: ExperimentService,
    experiment: Experiment,
) -> None:
    """Yield no replays from a canceling or terminal run."""
    run, _ = await experiment_service.start_run(
        experiment.id, agent_version_id=None, score_baselines=False, actor=ACTOR
    )
    canceled, _ = await service.cancel_run(run.id, actor=ACTOR)
    assert canceled.status is ExperimentRunStatus.CANCELED
    assert (
        await service.claim_replays(
            run.id, worker_id="worker-1", max_replays=5, actor=ACTOR
        )
        == []
    )


async def test_claim_requeues_stale_replays(
    repository: FakeExperimentRunRepository,
    replay_repository: FakeReplayRepository,
    config_repository: FakeReplayConfigRepository,
    experiment_repository: FakeExperimentRepository,
    session_repository: FakeSessionRepository,
    experiment_service: ExperimentService,
    experiment: Experiment,
) -> None:
    """Requeue stale claims for another worker and increment the attempt."""
    stale_service = build_service(
        repository,
        replay_repository,
        config_repository,
        experiment_repository,
        session_repository,
        heartbeat_timeout_seconds=-60,
    )
    run, _ = await experiment_service.start_run(
        experiment.id, agent_version_id=None, score_baselines=False, actor=ACTOR
    )
    first = await stale_service.claim_replays(
        run.id, worker_id="worker-1", max_replays=5, actor=ACTOR
    )
    assert len(first) == 2
    second = await stale_service.claim_replays(
        run.id, worker_id="worker-2", max_replays=5, actor=ACTOR
    )
    assert len(second) == 2
    for replay, _ in second:
        assert replay.worker_id == "worker-2"
        assert replay.attempt == 2


async def test_claim_times_out_stale_replays_at_max_attempts(
    repository: FakeExperimentRunRepository,
    replay_repository: FakeReplayRepository,
    config_repository: FakeReplayConfigRepository,
    experiment_repository: FakeExperimentRepository,
    session_repository: FakeSessionRepository,
    experiment_service: ExperimentService,
    experiment: Experiment,
) -> None:
    """Time out stale claims at the attempt limit and finalize the run."""
    stale_service = build_service(
        repository,
        replay_repository,
        config_repository,
        experiment_repository,
        session_repository,
        heartbeat_timeout_seconds=-60,
        max_attempts=1,
    )
    run, _ = await experiment_service.start_run(
        experiment.id, agent_version_id=None, score_baselines=False, actor=ACTOR
    )
    claimed = await stale_service.claim_replays(
        run.id, worker_id="worker-1", max_replays=5, actor=ACTOR
    )
    assert len(claimed) == 2
    assert (
        await stale_service.claim_replays(
            run.id, worker_id="worker-2", max_replays=5, actor=ACTOR
        )
        == []
    )
    replays, _ = await replay_repository.query(ReplayFilter(experiment_run_id=run.id))
    assert all(replay.status is ReplayStatus.TIMED_OUT for replay in replays)
    assert all(replay.error == HEARTBEAT_TIMEOUT_ERROR for replay in replays)
    finalized = await repository.get(run.id)
    assert finalized.status is ExperimentRunStatus.FAILED
    assert finalized.error == "2 of 2 replays timed out"
    assert finalized.summary is not None
    assert finalized.summary["replay_counts_by_status"] == {"timed_out": 2}


async def test_cancel_run_cancels_pending_and_claimed(
    service: ExperimentRunService,
    replay_repository: FakeReplayRepository,
    repository: FakeExperimentRunRepository,
    experiment_service: ExperimentService,
    experiment: Experiment,
) -> None:
    """Cancel pending and claimed replays and land on canceled directly."""
    run, _ = await experiment_service.start_run(
        experiment.id, agent_version_id=None, score_baselines=False, actor=ACTOR
    )
    await service.claim_replays(
        run.id, worker_id="worker-1", max_replays=1, actor=ACTOR
    )
    canceled, progress = await service.cancel_run(run.id, actor=ACTOR)
    assert canceled.status is ExperimentRunStatus.CANCELED
    assert canceled.ended_at is not None
    assert canceled.summary is not None
    assert canceled.summary["replay_counts_by_status"] == {"canceled": 2}
    assert progress.canceled == 2
    replays, _ = await replay_repository.query(ReplayFilter(experiment_run_id=run.id))
    assert all(replay.status is ReplayStatus.CANCELED for replay in replays)


async def test_cancel_run_keeps_running_replays(
    service: ExperimentRunService,
    replay_repository: FakeReplayRepository,
    experiment_service: ExperimentService,
    experiment: Experiment,
) -> None:
    """Leave running replays to the heartbeat path and stay canceling."""
    run, _ = await experiment_service.start_run(
        experiment.id, agent_version_id=None, score_baselines=False, actor=ACTOR
    )
    claimed = await service.claim_replays(
        run.id, worker_id="worker-1", max_replays=1, actor=ACTOR
    )
    running = claimed[0][0]
    running.start()
    await replay_repository.update(running)

    canceling, progress = await service.cancel_run(run.id, actor=ACTOR)
    assert canceling.status is ExperimentRunStatus.CANCELING
    assert canceling.summary is None
    assert progress.running == 1
    assert progress.canceled == 1
    loaded = await replay_repository.get(running.id)
    assert loaded.status is ReplayStatus.RUNNING

    # The run lands on canceled once the running replay drains.
    loaded.cancel()
    await replay_repository.update(loaded)
    drained, _ = await service.cancel_run(run.id, actor=ACTOR)
    assert drained.status is ExperimentRunStatus.CANCELED
    assert drained.summary is not None


async def test_cancel_terminal_run(
    service: ExperimentRunService,
    experiment_service: ExperimentService,
    experiment: Experiment,
) -> None:
    """Reject canceling a terminal run."""
    run, _ = await experiment_service.start_run(
        experiment.id, agent_version_id=None, score_baselines=False, actor=ACTOR
    )
    canceled, _ = await service.cancel_run(run.id, actor=ACTOR)
    assert canceled.status is ExperimentRunStatus.CANCELED
    with pytest.raises(
        InvalidExperimentRunTransition,
        match=f"Experiment run {run.id} cannot transition from 'canceled' "
        f"to 'canceling'",
    ):
        await service.cancel_run(run.id, actor=ACTOR)


def running_run() -> ExperimentRun:
    """Build a running experiment run entity."""
    return ExperimentRun(
        owner_id=uuid.uuid4(),
        experiment_id=uuid.uuid4(),
        agent_version_id=uuid.uuid4(),
        status=ExperimentRunStatus.RUNNING,
    )


def test_run_finalize_decides_status() -> None:
    """Land finalize on canceled, failed, or completed with the counts."""
    completed = running_run()
    completed.finalize({}, [ReplayStatus.COMPLETED, ReplayStatus.CANCELED])
    assert completed.status is ExperimentRunStatus.COMPLETED
    assert completed.error is None
    assert completed.ended_at is not None

    failed = running_run()
    failed.finalize({}, [ReplayStatus.COMPLETED] * 7 + [ReplayStatus.FAILED] * 3)
    assert failed.status is ExperimentRunStatus.FAILED
    assert failed.error == "3 of 10 replays failed"

    timed_out = running_run()
    timed_out.finalize({}, [ReplayStatus.COMPLETED] * 8 + [ReplayStatus.TIMED_OUT] * 2)
    assert timed_out.status is ExperimentRunStatus.FAILED
    assert timed_out.error == "2 of 10 replays timed out"

    mixed = running_run()
    mixed.finalize(
        {},
        [ReplayStatus.COMPLETED] * 5
        + [ReplayStatus.FAILED] * 3
        + [ReplayStatus.TIMED_OUT] * 2,
    )
    assert mixed.status is ExperimentRunStatus.FAILED
    assert mixed.error == "3 of 10 replays failed, 2 timed out"

    canceling = running_run()
    canceling.cancel()
    canceling.finalize({}, [ReplayStatus.FAILED, ReplayStatus.CANCELED])
    assert canceling.status is ExperimentRunStatus.CANCELED
    assert canceling.error is None

    with pytest.raises(InvalidExperimentRunTransition):
        canceling.finalize({}, [ReplayStatus.CANCELED])


async def test_delete_run(
    service: ExperimentRunService,
    repository: FakeExperimentRunRepository,
    replay_repository: FakeReplayRepository,
    config_repository: FakeReplayConfigRepository,
    experiment_service: ExperimentService,
    experiment: Experiment,
) -> None:
    """Delete a terminal run with its replays, keeping the referenced config."""
    run, _ = await experiment_service.start_run(
        experiment.id, agent_version_id=None, score_baselines=False, actor=ACTOR
    )
    canceled, _ = await service.cancel_run(run.id, actor=ACTOR)
    assert canceled.status is ExperimentRunStatus.CANCELED

    await service.delete_run(run.id, actor=ACTOR)
    with pytest.raises(ExperimentRunNotFound):
        await repository.get(run.id)
    _, total = await replay_repository.query(ReplayFilter(experiment_run_id=run.id))
    assert total == 0
    # The experiment still references the config.
    await config_repository.get(experiment.replay_config_id)

    await experiment_service.delete_experiment(experiment.id, actor=ACTOR)
    with pytest.raises(ReplayConfigNotFound):
        await config_repository.get(experiment.replay_config_id)


async def test_delete_run_rejects_non_terminal(
    service: ExperimentRunService,
    experiment_service: ExperimentService,
    experiment: Experiment,
) -> None:
    """Reject deleting a run that is not terminal."""
    run, _ = await experiment_service.start_run(
        experiment.id, agent_version_id=None, score_baselines=False, actor=ACTOR
    )
    with pytest.raises(
        ExperimentRunActive, match=f"Experiment run {run.id} is not terminal"
    ):
        await service.delete_run(run.id, actor=ACTOR)


async def test_delete_run_not_found(service: ExperimentRunService) -> None:
    """Raise for an unknown experiment run id."""
    missing_id = uuid.uuid4()
    with pytest.raises(
        ExperimentRunNotFound, match=f"Experiment run {missing_id} was not found"
    ):
        await service.delete_run(missing_id, actor=ACTOR)
