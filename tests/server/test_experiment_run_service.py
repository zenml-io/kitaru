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
    FakeBlobRepository,
    FakeCohortRepository,
    FakeExperimentRepository,
    FakeExperimentRunRepository,
    FakeJobRepository,
    FakePluginRepository,
    FakeReplayConfigRepository,
    FakeReplayRepository,
    FakeSessionRepository,
    FakeTagRepository,
    FakeWorkerRepository,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.experiment_runs import (
    ExperimentRunFilter,
    ExperimentRunJobsFilter,
)
from kitaru.server.application.models.experiments import ExperimentCreate
from kitaru.server.application.models.jobs import JobFilter
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
from kitaru.server.domain.job import JobStatus, WorkerScope
from kitaru.server.domain.replay_config import (
    ReplayConfigNotFound,
    ScoringPolicy,
    SourceRef,
    SourceScorerConfig,
)
from kitaru.server.domain.session import Session, SessionOrigin, SessionStatus
from kitaru.server.domain.tag import Tag, TagLink, TagResourceType

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))

WORKER_ID = uuid.uuid4()

SCORING_POLICY = ScoringPolicy(
    scorers=[
        SourceScorerConfig(
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
def job_repository(
    session_repository: FakeSessionRepository,
    version_repository: FakeAgentVersionRepository,
    config_repository: FakeReplayConfigRepository,
) -> FakeJobRepository:
    """Provide a fake job repository."""
    return FakeJobRepository(session_repository, version_repository)


@pytest.fixture
def replay_repository(
    job_repository: FakeJobRepository,
    config_repository: FakeReplayConfigRepository,
    session_repository: FakeSessionRepository,
) -> FakeReplayRepository:
    """Provide a fake replay repository."""
    return FakeReplayRepository(job_repository, config_repository, session_repository)


@pytest.fixture
def repository(
    experiment_repository: FakeExperimentRepository,
    job_repository: FakeJobRepository,
    replay_repository: FakeReplayRepository,
    tag_repository: FakeTagRepository,
) -> FakeExperimentRunRepository:
    """Provide a fake experiment run repository."""
    run_repository = FakeExperimentRunRepository(
        experiment_repository, job_repository, tag_repository
    )
    run_repository.replay_repository = replay_repository
    return run_repository


@pytest.fixture
def service(
    repository: FakeExperimentRunRepository,
    job_repository: FakeJobRepository,
    replay_repository: FakeReplayRepository,
    config_repository: FakeReplayConfigRepository,
    experiment_repository: FakeExperimentRepository,
    session_repository: FakeSessionRepository,
) -> ExperimentRunService:
    """Provide an experiment run service backed by the fake repositories."""
    return ExperimentRunService(
        repository=repository,
        job_repository=job_repository,
        replay_repository=replay_repository,
        replay_config_repository=config_repository,
        experiment_repository=experiment_repository,
        session_repository=session_repository,
        heartbeat_timeout_seconds=60,
        max_attempts=3,
    )


@pytest.fixture
def worker_repository() -> FakeWorkerRepository:
    """Provide a fake worker repository."""
    return FakeWorkerRepository()


@pytest.fixture
def plugin_repository() -> FakePluginRepository:
    """Provide a fake plugin repository."""
    return FakePluginRepository(FakeBlobRepository())


@pytest.fixture
def experiment_service(
    experiment_repository: FakeExperimentRepository,
    repository: FakeExperimentRunRepository,
    cohort_repository: FakeCohortRepository,
    version_repository: FakeAgentVersionRepository,
    config_repository: FakeReplayConfigRepository,
    worker_repository: FakeWorkerRepository,
    plugin_repository: FakePluginRepository,
) -> ExperimentService:
    """Provide an experiment service backed by the fake repositories."""
    return ExperimentService(
        repository=experiment_repository,
        run_repository=repository,
        cohort_repository=cohort_repository,
        agent_version_repository=version_repository,
        replay_config_repository=config_repository,
        worker_repository=worker_repository,
        plugin_repository=plugin_repository,
        worker_liveness_timeout_seconds=60,
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
    """Load a run with its computed job counts."""
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


async def test_list_run_jobs(
    service: ExperimentRunService,
    experiment_service: ExperimentService,
    experiment: Experiment,
) -> None:
    """List the jobs of a run."""
    created, _ = await experiment_service.start_run(
        experiment.id, agent_version_id=None, score_baselines=False, actor=ACTOR
    )
    jobs, total = await service.list_run_jobs(
        created.id, ExperimentRunJobsFilter(), actor=ACTOR
    )
    assert total == 2
    for job in jobs:
        assert job.experiment_run_id == created.id
        assert job.status is JobStatus.PENDING

    jobs, total = await service.list_run_jobs(
        created.id, ExperimentRunJobsFilter(page=2, page_size=1), actor=ACTOR
    )
    assert total == 2
    assert len(jobs) == 1


async def test_list_run_jobs_not_found(service: ExperimentRunService) -> None:
    """Raise for an unknown experiment run id."""
    missing_id = uuid.uuid4()
    with pytest.raises(
        ExperimentRunNotFound, match=f"Experiment run {missing_id} was not found"
    ):
        await service.list_run_jobs(missing_id, ExperimentRunJobsFilter(), actor=ACTOR)


async def test_cancel_run_cancels_pending_and_claimed(
    service: ExperimentRunService,
    job_repository: FakeJobRepository,
    repository: FakeExperimentRunRepository,
    experiment_service: ExperimentService,
    experiment: Experiment,
) -> None:
    """Cancel pending and claimed jobs and land on canceled directly."""
    run, _ = await experiment_service.start_run(
        experiment.id, agent_version_id=None, score_baselines=False, actor=ACTOR
    )
    await job_repository.claim_pending(
        WORKER_ID, 1, WorkerScope(experiment_run_id=run.id)
    )
    canceled, progress = await service.cancel_run(run.id, actor=ACTOR)
    assert canceled.status is ExperimentRunStatus.CANCELED
    assert canceled.ended_at is not None
    assert canceled.summary is not None
    assert canceled.summary["replay_counts_by_status"] == {"canceled": 2}
    assert progress.canceled == 2
    jobs, _ = await job_repository.query(JobFilter(experiment_run_id=run.id))
    assert all(job.status is JobStatus.CANCELED for job in jobs)


async def test_cancel_run_keeps_running_jobs(
    service: ExperimentRunService,
    job_repository: FakeJobRepository,
    experiment_service: ExperimentService,
    experiment: Experiment,
) -> None:
    """Leave running jobs to the heartbeat path and stay canceling."""
    run, _ = await experiment_service.start_run(
        experiment.id, agent_version_id=None, score_baselines=False, actor=ACTOR
    )
    claimed = await job_repository.claim_pending(
        WORKER_ID, 1, WorkerScope(experiment_run_id=run.id)
    )
    running = claimed[0]
    running.start()
    await job_repository.update(running)

    canceling, progress = await service.cancel_run(run.id, actor=ACTOR)
    assert canceling.status is ExperimentRunStatus.CANCELING
    assert canceling.summary is None
    assert progress.running == 1
    assert progress.canceled == 1
    loaded = await job_repository.get(running.id)
    assert loaded.status is JobStatus.RUNNING

    # The run lands on canceled once the running job drains.
    loaded.cancel()
    await job_repository.update(loaded)
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
    completed.finalize({}, [JobStatus.COMPLETED, JobStatus.CANCELED])
    assert completed.status is ExperimentRunStatus.COMPLETED
    assert completed.error is None
    assert completed.ended_at is not None

    failed = running_run()
    failed.finalize({}, [JobStatus.COMPLETED] * 7 + [JobStatus.FAILED] * 3)
    assert failed.status is ExperimentRunStatus.FAILED
    assert failed.error == "3 of 10 jobs failed"

    timed_out = running_run()
    timed_out.finalize({}, [JobStatus.COMPLETED] * 8 + [JobStatus.TIMED_OUT] * 2)
    assert timed_out.status is ExperimentRunStatus.FAILED
    assert timed_out.error == "2 of 10 jobs timed out"

    mixed = running_run()
    mixed.finalize(
        {},
        [JobStatus.COMPLETED] * 5 + [JobStatus.FAILED] * 3 + [JobStatus.TIMED_OUT] * 2,
    )
    assert mixed.status is ExperimentRunStatus.FAILED
    assert mixed.error == "3 of 10 jobs failed, 2 timed out"

    canceling = running_run()
    canceling.cancel()
    canceling.finalize({}, [JobStatus.FAILED, JobStatus.CANCELED])
    assert canceling.status is ExperimentRunStatus.CANCELED
    assert canceling.error is None

    with pytest.raises(InvalidExperimentRunTransition):
        canceling.finalize({}, [JobStatus.CANCELED])


async def test_delete_run(
    service: ExperimentRunService,
    repository: FakeExperimentRunRepository,
    job_repository: FakeJobRepository,
    config_repository: FakeReplayConfigRepository,
    experiment_service: ExperimentService,
    experiment: Experiment,
) -> None:
    """Delete a terminal run with its jobs, keeping the referenced config."""
    run, _ = await experiment_service.start_run(
        experiment.id, agent_version_id=None, score_baselines=False, actor=ACTOR
    )
    canceled, _ = await service.cancel_run(run.id, actor=ACTOR)
    assert canceled.status is ExperimentRunStatus.CANCELED

    await service.delete_run(run.id, actor=ACTOR)
    with pytest.raises(ExperimentRunNotFound):
        await repository.get(run.id)
    _, total = await job_repository.query(JobFilter(experiment_run_id=run.id))
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
