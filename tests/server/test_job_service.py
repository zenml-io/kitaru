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
"""Tests for job use cases and the job state machine."""

import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import SecretStr

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
    FakeSecretRepository,
    FakeSessionNodeRepository,
    FakeSessionRepository,
    FakeWorkerRepository,
    create_worker,
)
from kitaru.hashing import tool_call_cache_key
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.jobs import (
    ImportCreate,
    JobFilter,
    JobUpdate,
    SessionRunCreate,
)
from kitaru.server.application.models.replays import ReplayCreate
from kitaru.server.application.services.job_service import JobService
from kitaru.server.application.services.replay_service import ReplayService
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent, AgentNotFound
from kitaru.server.domain.agent_version import (
    AgentVersion,
    AgentVersionNotRunnable,
    MissingRunImage,
    NoRunnableAgentVersion,
    RunSpec,
)
from kitaru.server.domain.blob import Blob, BlobNotFound
from kitaru.server.domain.cohort import Cohort
from kitaru.server.domain.execution import ExecutionTarget
from kitaru.server.domain.experiment import Experiment
from kitaru.server.domain.experiment_run import (
    ExperimentRun,
    ExperimentRunNotFound,
    ExperimentRunStatus,
)
from kitaru.server.domain.job import (
    HEARTBEAT_TIMEOUT_ERROR,
    DuplicateScoreJob,
    Import,
    InvalidJob,
    InvalidJobTransition,
    InvalidToolLookup,
    JobActive,
    JobAlreadyLinked,
    JobKind,
    JobKindMismatch,
    JobMissingResult,
    JobMissingResultSession,
    JobMissingScore,
    JobNotFound,
    JobNotRunning,
    JobNotStandalone,
    JobResultSessionNotCompleted,
    JobStatus,
    ReplayJob,
    Score,
    WorkerScope,
)
from kitaru.server.domain.plugin import (
    Plugin,
    PluginFormat,
    PluginKind,
    PluginNameNotFound,
    PluginVersion,
    PluginVersionNotFound,
)
from kitaru.server.domain.replay import Replay
from kitaru.server.domain.replay_config import (
    HistoryPolicy,
    HistoryScope,
    InvalidReplayConfig,
    PassthroughPolicy,
    RegistryScorerConfig,
    ReplayConfig,
    ReplayConfigNotFound,
    ReplayOverride,
    ScoringPolicy,
    SourceRef,
    SourceScorerConfig,
    ToolPolicyConfig,
)
from kitaru.server.domain.secret import Secret, SecretNotFound
from kitaru.server.domain.session import (
    Session,
    SessionOrigin,
    SessionProvider,
    SessionStatus,
)
from kitaru.server.domain.session_node import (
    NodeStatus,
    NodeType,
    SessionNode,
)
from kitaru.server.domain.worker import Worker, WorkerNotFound

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))

WORKER_ID = uuid.uuid4()
OTHER_WORKER_ID = uuid.uuid4()

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
def version_repository(
    agent_repository: FakeAgentRepository,
) -> FakeAgentVersionRepository:
    """Provide a fake agent version repository."""
    return FakeAgentVersionRepository(agent_repository)


@pytest.fixture
def session_repository(
    agent_repository: FakeAgentRepository,
    version_repository: FakeAgentVersionRepository,
) -> FakeSessionRepository:
    """Provide a fake session repository."""
    return FakeSessionRepository(agent_repository, version_repository)


@pytest.fixture
def config_repository() -> FakeReplayConfigRepository:
    """Provide a fake replay config repository."""
    return FakeReplayConfigRepository()


@pytest.fixture
def repository(
    session_repository: FakeSessionRepository,
    version_repository: FakeAgentVersionRepository,
    config_repository: FakeReplayConfigRepository,
    plugin_repository: FakePluginRepository,
    blob_repository: FakeBlobRepository,
) -> FakeJobRepository:
    """Provide a fake job repository."""
    return FakeJobRepository(
        session_repository,
        version_repository,
        plugin_repository,
        blob_repository,
    )


@pytest.fixture
def node_repository(
    session_repository: FakeSessionRepository,
) -> FakeSessionNodeRepository:
    """Provide a fake session node repository."""
    return FakeSessionNodeRepository(session_repository)


@pytest.fixture
def cohort_repository(
    session_repository: FakeSessionRepository,
    agent_repository: FakeAgentRepository,
) -> FakeCohortRepository:
    """Provide a fake cohort repository."""
    return FakeCohortRepository(session_repository, agent_repository)


@pytest.fixture
def experiment_repository(
    cohort_repository: FakeCohortRepository,
    config_repository: FakeReplayConfigRepository,
) -> FakeExperimentRepository:
    """Provide a fake experiment repository."""
    return FakeExperimentRepository(cohort_repository, config_repository)


@pytest.fixture
def replay_repository(
    repository: FakeJobRepository,
    config_repository: FakeReplayConfigRepository,
    session_repository: FakeSessionRepository,
) -> FakeReplayRepository:
    """Provide a fake replay repository."""
    return FakeReplayRepository(repository, config_repository, session_repository)


@pytest.fixture
def run_repository(
    experiment_repository: FakeExperimentRepository,
    repository: FakeJobRepository,
    replay_repository: FakeReplayRepository,
) -> FakeExperimentRunRepository:
    """Provide a fake experiment run repository."""
    run_repository = FakeExperimentRunRepository(experiment_repository, repository)
    run_repository.replay_repository = replay_repository
    return run_repository


@pytest.fixture
def secret_repository() -> FakeSecretRepository:
    """Provide a fake secret repository."""
    return FakeSecretRepository()


@pytest.fixture
def blob_repository() -> FakeBlobRepository:
    """Provide a fake blob repository."""
    return FakeBlobRepository()


@pytest.fixture
def plugin_repository(blob_repository: FakeBlobRepository) -> FakePluginRepository:
    """Provide a fake plugin repository."""
    return FakePluginRepository(blob_repository)


@pytest.fixture
def worker_repository() -> FakeWorkerRepository:
    """Provide a fake worker repository."""
    return FakeWorkerRepository()


@pytest.fixture
async def worker(worker_repository: FakeWorkerRepository) -> Worker:
    """Provide a stored worker."""
    return await create_worker(worker_repository, ACTOR.account.id)


@pytest.fixture
async def other_worker(worker_repository: FakeWorkerRepository) -> Worker:
    """Provide a second stored worker."""
    return await create_worker(worker_repository, ACTOR.account.id, name="runner-2")


@pytest.fixture
def service(
    repository: FakeJobRepository,
    replay_repository: FakeReplayRepository,
    config_repository: FakeReplayConfigRepository,
    session_repository: FakeSessionRepository,
    agent_repository: FakeAgentRepository,
    version_repository: FakeAgentVersionRepository,
    node_repository: FakeSessionNodeRepository,
    run_repository: FakeExperimentRunRepository,
    experiment_repository: FakeExperimentRepository,
    cohort_repository: FakeCohortRepository,
    secret_repository: FakeSecretRepository,
    worker_repository: FakeWorkerRepository,
    plugin_repository: FakePluginRepository,
    blob_repository: FakeBlobRepository,
) -> JobService:
    """Provide a job service backed by the fake repositories."""
    return JobService(
        repository=repository,
        replay_repository=replay_repository,
        replay_config_repository=config_repository,
        session_repository=session_repository,
        agent_repository=agent_repository,
        agent_version_repository=version_repository,
        session_node_repository=node_repository,
        experiment_run_repository=run_repository,
        experiment_repository=experiment_repository,
        cohort_repository=cohort_repository,
        secret_repository=secret_repository,
        worker_repository=worker_repository,
        plugin_repository=plugin_repository,
        blob_repository=blob_repository,
        heartbeat_timeout_seconds=60,
        max_attempts=3,
        worker_liveness_timeout_seconds=60,
    )


@pytest.fixture
def replay_service(
    replay_repository: FakeReplayRepository,
    repository: FakeJobRepository,
    config_repository: FakeReplayConfigRepository,
    session_repository: FakeSessionRepository,
    node_repository: FakeSessionNodeRepository,
    version_repository: FakeAgentVersionRepository,
    plugin_repository: FakePluginRepository,
) -> ReplayService:
    """Provide a replay service backed by the fake repositories."""
    return ReplayService(
        repository=replay_repository,
        job_repository=repository,
        replay_config_repository=config_repository,
        session_repository=session_repository,
        session_node_repository=node_repository,
        agent_version_repository=version_repository,
        plugin_repository=plugin_repository,
    )


@pytest.fixture
async def agent(agent_repository: FakeAgentRepository) -> Agent:
    """Provide a stored agent."""
    return await agent_repository.create(
        Agent(owner_id=ACTOR.account.id, name="support-bot")
    )


@pytest.fixture
async def version(
    version_repository: FakeAgentVersionRepository, agent: Agent
) -> AgentVersion:
    """Provide a stored runnable agent version."""
    return await version_repository.create(
        AgentVersion(
            owner_id=ACTOR.account.id,
            agent_id=agent.id,
            version="v1",
            run_spec=RunSpec(command="python agent.py", timeout_seconds=600),
        )
    )


async def create_session(
    repository: FakeSessionRepository,
    agent_id: uuid.UUID,
    status: SessionStatus = SessionStatus.COMPLETED,
) -> Session:
    """Store a recorded session for job tests.

    Args:
        repository: Fake session repository.
        agent_id: Id of the agent.
        status: Session status.

    Returns:
        Stored session.
    """
    return await repository.create(
        Session(
            owner_id=ACTOR.account.id,
            agent_id=agent_id,
            origin=SessionOrigin.RECORDED,
            status=status,
        )
    )


def replay_create(session_id: uuid.UUID, **overrides: object) -> ReplayCreate:
    """Build a replay create command.

    Args:
        session_id: Id of the session to replay.
        **overrides: Field overrides.

    Returns:
        Replay create command.
    """
    values: dict[str, object] = {
        "input_session_id": session_id,
        "scoring_policy": SCORING_POLICY,
        **overrides,
    }
    return ReplayCreate.model_validate(values)


async def create_replay(
    replay_service: ReplayService, command: ReplayCreate
) -> tuple[ReplayJob, Replay]:
    """Create a standalone replay with its job.

    Args:
        replay_service: Replay service.
        command: Replay create command.

    Returns:
        Created job and replay.
    """
    replay, job, _ = await replay_service.create_replay(command, actor=ACTOR)
    return job, replay


async def test_get_job(
    service: JobService,
    replay_service: ReplayService,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Load a stored job."""
    session = await create_session(session_repository, agent.id)
    created, _ = await create_replay(replay_service, replay_create(session.id))
    job = await service.get_job(created.id, actor=ACTOR)
    assert job == created


async def test_get_job_not_found(service: JobService) -> None:
    """Raise for an unknown job id."""
    missing_id = uuid.uuid4()
    with pytest.raises(JobNotFound, match=f"Job {missing_id} was not found"):
        await service.get_job(missing_id, actor=ACTOR)


async def test_list_jobs_filters(
    service: JobService,
    replay_service: ReplayService,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """List jobs filtered by session, status, and standalone."""
    first = await create_session(session_repository, agent.id)
    second = await create_session(session_repository, agent.id)
    job_one, _ = await create_replay(replay_service, replay_create(first.id))
    await create_replay(replay_service, replay_create(second.id))

    jobs, total = await service.list_jobs(JobFilter(), actor=ACTOR)
    assert total == 2

    jobs, total = await service.list_jobs(
        JobFilter(input_session_id=first.id), actor=ACTOR
    )
    assert total == 1
    assert jobs[0].id == job_one.id

    jobs, total = await service.list_jobs(
        JobFilter(status=JobStatus.PENDING), actor=ACTOR
    )
    assert total == 2
    jobs, total = await service.list_jobs(
        JobFilter(status=JobStatus.RUNNING), actor=ACTOR
    )
    assert total == 0

    jobs, total = await service.list_jobs(JobFilter(standalone=True), actor=ACTOR)
    assert total == 2
    jobs, total = await service.list_jobs(JobFilter(standalone=False), actor=ACTOR)
    assert total == 0

    jobs, total = await service.list_jobs(JobFilter(page=2, page_size=1), actor=ACTOR)
    assert total == 2
    assert len(jobs) == 1


def run_job(**overrides: object) -> ReplayJob:
    """Build a run-created job entity.

    Args:
        **overrides: Field overrides.

    Returns:
        Replay job entity.
    """
    values: dict[str, object] = {
        "experiment_run_id": uuid.uuid4(),
        "agent_version_id": uuid.uuid4(),
        "input_session_id": uuid.uuid4(),
        "execution_target": ExecutionTarget.POOL,
        **overrides,
    }
    return ReplayJob.model_validate(values)


def test_job_claim_and_start() -> None:
    """Walk a run-created job from pending through running."""
    job = run_job()
    job.claim(WORKER_ID)
    assert job.status is JobStatus.CLAIMED
    assert job.worker_id == WORKER_ID
    assert job.claimed_at is not None
    assert job.heartbeat_at is not None
    job.start()
    assert job.status is JobStatus.RUNNING
    assert job.started_at is not None


def test_job_claim_requires_pending() -> None:
    """Reject claiming a job that is not pending."""
    job = run_job()
    job.claim(WORKER_ID)
    with pytest.raises(
        InvalidJobTransition,
        match=f"Job {job.id} cannot transition from 'claimed' to 'claimed'",
    ):
        job.claim(OTHER_WORKER_ID)


def test_job_standalone_claim_and_start() -> None:
    """Walk a standalone job from pending through running."""
    job = run_job(experiment_run_id=None)
    job.claim(WORKER_ID)
    assert job.status is JobStatus.CLAIMED
    assert job.worker_id == WORKER_ID
    job.start()
    assert job.status is JobStatus.RUNNING


def test_job_standalone_starts_from_pending() -> None:
    """Skip the claim for standalone jobs."""
    job = run_job(experiment_run_id=None)
    job.start()
    assert job.status is JobStatus.RUNNING
    assert job.heartbeat_at is not None


def test_job_run_created_start_requires_claim() -> None:
    """Reject starting a run-created job that was not claimed."""
    job = run_job()
    with pytest.raises(InvalidJobTransition):
        job.start()


def test_job_requeue_increments_attempt() -> None:
    """Requeue a claimed job and clear the claim state."""
    job = run_job()
    job.claim(WORKER_ID)
    job.requeue()
    assert job.status is JobStatus.PENDING
    assert job.attempt == 2
    assert job.worker_id is None
    assert job.claimed_at is None
    assert job.heartbeat_at is None
    assert job.started_at is None
    with pytest.raises(InvalidJobTransition):
        job.requeue()


def finished_job(status: JobStatus) -> ReplayJob:
    """Build a standalone job finished in a status.

    Args:
        status: Failed, timed out, or canceled.

    Returns:
        Replay job entity.
    """
    job = run_job(experiment_run_id=None)
    job.claim(WORKER_ID)
    job.start()
    job.link_result_session(uuid.uuid4())
    if status is JobStatus.FAILED:
        job.fail("agent exited with code 1")
    elif status is JobStatus.TIMED_OUT:
        job.time_out("wall clock limit exceeded")
    else:
        job.cancel()
    return job


def test_job_retry_clears_attempt_state() -> None:
    """Retry a finished job and clear its attempt state."""
    for status in (
        JobStatus.FAILED,
        JobStatus.TIMED_OUT,
        JobStatus.CANCELED,
    ):
        job = finished_job(status)
        job.retry()
        assert job.status is JobStatus.PENDING
        assert job.attempt == 2
        assert job.worker_id is None
        assert job.claimed_at is None
        assert job.heartbeat_at is None
        assert job.started_at is None
        assert job.ended_at is None
        assert job.error is None
        assert job.result_session_id is None


def test_job_retry_requires_finished() -> None:
    """Reject retrying a job that is not failed, timed out, or canceled."""
    pending = run_job(experiment_run_id=None)
    with pytest.raises(
        InvalidJobTransition,
        match=f"Job {pending.id} cannot transition from 'pending' to 'pending'",
    ):
        pending.retry()

    completed = run_job(experiment_run_id=None)
    completed.start()
    completed.link_result_session(uuid.uuid4())
    completed.complete(None)
    with pytest.raises(InvalidJobTransition):
        completed.retry()


def test_job_complete_requires_a_result_session() -> None:
    """Complete a running replay job once it recorded a result session."""
    job = run_job()
    job.claim(WORKER_ID)
    job.start()
    with pytest.raises(
        JobMissingResultSession, match=f"Job {job.id} has no result session"
    ):
        job.complete(None)
    job.link_result_session(uuid.uuid4())
    job.complete(None)
    assert job.status is JobStatus.COMPLETED
    assert job.ended_at is not None


def test_job_complete_requires_running() -> None:
    """Reject completing a job that is not running."""
    job = run_job()
    with pytest.raises(InvalidJobTransition):
        job.complete(None)


def test_job_fail_and_time_out() -> None:
    """Fail or time out a claimed or running job."""
    job = run_job()
    job.claim(WORKER_ID)
    job.fail("agent exited with code 1")
    assert job.status is JobStatus.FAILED
    assert job.error == "agent exited with code 1"

    other = run_job()
    other.claim(WORKER_ID)
    other.start()
    other.time_out("wall clock limit exceeded")
    assert other.status is JobStatus.TIMED_OUT
    with pytest.raises(InvalidJobTransition):
        other.fail("late failure")


def test_job_cancel() -> None:
    """Cancel a job in any non-terminal status."""
    job = run_job()
    job.cancel()
    assert job.status is JobStatus.CANCELED
    with pytest.raises(InvalidJobTransition):
        job.cancel()


def test_job_link_result_session() -> None:
    """Link the result session while running, once."""
    job = run_job()
    job.claim(WORKER_ID)
    with pytest.raises(JobNotRunning, match=f"Job {job.id} is not running"):
        job.link_result_session(uuid.uuid4())
    job.start()

    session_id = uuid.uuid4()
    job.link_result_session(session_id)
    assert job.result_session_id == session_id
    with pytest.raises(
        JobAlreadyLinked,
        match=f"Job {job.id} already has a result session",
    ):
        job.link_result_session(uuid.uuid4())

    idle = run_job()
    with pytest.raises(JobNotRunning, match=f"Job {idle.id} is not running"):
        idle.link_result_session(uuid.uuid4())


def test_job_with_staleness() -> None:
    """Report stale claims as pending or timed out without mutating."""
    job = run_job()
    job.claim(WORKER_ID)
    fresh = datetime.now(UTC) - timedelta(seconds=60)
    assert job.with_staleness(fresh, 3) is job

    stale = datetime.now(UTC) + timedelta(seconds=60)
    reported = job.with_staleness(stale, 3)
    assert reported is not job
    assert reported.status is JobStatus.PENDING
    assert reported.attempt == 2
    assert reported.worker_id is None
    assert job.status is JobStatus.CLAIMED

    exhausted = run_job(attempt=3)
    exhausted.claim(WORKER_ID)
    reported = exhausted.with_staleness(stale, 3)
    assert reported.status is JobStatus.TIMED_OUT
    assert reported.error == HEARTBEAT_TIMEOUT_ERROR

    terminal = run_job()
    terminal.cancel()
    assert terminal.with_staleness(stale, 3) is terminal


async def seed_run(
    session_repository: FakeSessionRepository,
    cohort_repository: FakeCohortRepository,
    config_repository: FakeReplayConfigRepository,
    experiment_repository: FakeExperimentRepository,
    run_repository: FakeExperimentRunRepository,
    job_repository: FakeJobRepository,
    agent: Agent,
    version: AgentVersion,
    sessions: list[Session],
    tool_policy: ToolPolicyConfig | None = None,
    score_baselines: bool = False,
    name: str = "swap-model",
) -> tuple[ExperimentRun, list[ReplayJob]]:
    """Store a run with one pending job per session.

    Args:
        session_repository: Fake session repository.
        cohort_repository: Fake cohort repository.
        config_repository: Fake replay config repository.
        experiment_repository: Fake experiment repository.
        run_repository: Fake experiment run repository.
        job_repository: Fake job repository.
        agent: Agent of the sessions.
        version: Agent version to execute.
        sessions: Original sessions.
        tool_policy: Tool policy of the config.
        score_baselines: Baseline scoring flag of the run.
        name: Experiment and cohort name.

    Returns:
        Stored run and its jobs.
    """
    cohort = await cohort_repository.create(
        Cohort(
            owner_id=ACTOR.account.id,
            name=f"{name}-cohort",
            agent_id=agent.id,
            session_count=len(sessions),
        ),
        [session.id for session in sessions],
    )
    config = await config_repository.create(
        ReplayConfig(
            owner_id=ACTOR.account.id,
            tool_policy=tool_policy or ToolPolicyConfig(default=HistoryPolicy()),
            scoring_policy=SCORING_POLICY,
        )
    )
    experiment = await experiment_repository.create(
        Experiment(
            owner_id=ACTOR.account.id,
            name=name,
            cohort_id=cohort.id,
            replay_config_id=config.id,
        )
    )
    run = ExperimentRun(
        owner_id=ACTOR.account.id,
        experiment_id=experiment.id,
        agent_version_id=version.id,
        score_baselines=score_baselines,
    )
    jobs = [
        ReplayJob(
            experiment_run_id=run.id,
            agent_version_id=version.id,
            input_session_id=session.id,
            execution_target=run.execution_target,
        )
        for session in sessions
    ]
    replays = [
        Replay(
            owner_id=ACTOR.account.id,
            job_id=job.id,
            experiment_run_id=run.id,
            replay_config_id=config.id,
            input_session_id=job.input_session_id,
        )
        for job in jobs
    ]
    run = await run_repository.create(run, jobs, replays)
    stored, _ = await job_repository.query(JobFilter(experiment_run_id=run.id))
    return run, [job for job in stored if isinstance(job, ReplayJob)]


async def test_get_spec_standalone(
    service: JobService,
    replay_service: ReplayService,
    session_repository: FakeSessionRepository,
    version_repository: FakeAgentVersionRepository,
    secret_repository: FakeSecretRepository,
    agent: Agent,
) -> None:
    """Resolve a standalone spec with the merged secret environment."""
    first = await secret_repository.create(
        Secret(
            owner_id=ACTOR.account.id,
            name="openai",
            values={
                "OPENAI_API_KEY": SecretStr("sk-1"),
                "SHARED": SecretStr("first"),
            },
        )
    )
    second = await secret_repository.create(
        Secret(
            owner_id=ACTOR.account.id,
            name="shared",
            values={"SHARED": SecretStr("second")},
        )
    )
    version = await version_repository.create(
        AgentVersion(
            owner_id=ACTOR.account.id,
            agent_id=agent.id,
            version="v2",
            run_spec=RunSpec(
                command="python agent.py",
                working_dir="/srv/agent",
                env={"MODE": "replay"},
                secret_ids=[first.id, second.id],
                timeout_seconds=600,
            ),
        )
    )
    session = await session_repository.create(
        Session(
            owner_id=ACTOR.account.id,
            agent_id=agent.id,
            origin=SessionOrigin.RECORDED,
            status=SessionStatus.COMPLETED,
            inputs={"prompt": "hi"},
        )
    )
    override = ReplayOverride(model="claude-sonnet-5")
    _, job, config = await replay_service.create_replay(
        replay_create(session.id, override=override), actor=ACTOR
    )
    spec = await service.get_spec(job.id, actor=ACTOR)
    assert spec.job_id == job.id
    assert spec.inputs == {"prompt": "hi"}
    assert spec.override == override
    assert spec.tool_policy == config.tool_policy
    assert spec.scorer is None
    assert spec.run_spec == version.run_spec
    assert spec.input_session_id == session.id
    assert {
        name: value.get_secret_value() for name, value in spec.secret_env.items()
    } == {"OPENAI_API_KEY": "sk-1", "SHARED": "second"}


async def test_get_spec_applies_prompt_override(
    service: JobService,
    replay_service: ReplayService,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Resolve the spec inputs from the prompt override."""
    session = await session_repository.create(
        Session(
            owner_id=ACTOR.account.id,
            agent_id=agent.id,
            origin=SessionOrigin.RECORDED,
            status=SessionStatus.COMPLETED,
            inputs={"prompt": "hi"},
        )
    )
    override = ReplayOverride(prompt="rewritten task")
    job, _ = await create_replay(
        replay_service, replay_create(session.id, override=override)
    )
    spec = await service.get_spec(job.id, actor=ACTOR)
    assert spec.inputs == "rewritten task"
    assert spec.run_spec == version.run_spec


async def test_get_spec_version_without_run_spec(
    service: JobService,
    replay_service: ReplayService,
    session_repository: FakeSessionRepository,
    version_repository: FakeAgentVersionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Raise when the stamped version lost its run spec."""
    session = await create_session(session_repository, agent.id)
    job, _ = await create_replay(replay_service, replay_create(session.id))
    await version_repository.update(version.model_copy(update={"run_spec": None}))
    with pytest.raises(
        AgentVersionNotRunnable, match=f"Agent version {version.id} has no run spec"
    ):
        await service.get_spec(job.id, actor=ACTOR)


async def test_get_spec_deleted_secret(
    service: JobService,
    replay_service: ReplayService,
    session_repository: FakeSessionRepository,
    version_repository: FakeAgentVersionRepository,
    agent: Agent,
) -> None:
    """Raise when a run spec secret no longer resolves."""
    missing_id = uuid.uuid4()
    await version_repository.create(
        AgentVersion(
            owner_id=ACTOR.account.id,
            agent_id=agent.id,
            version="v1",
            run_spec=RunSpec(
                command="python agent.py",
                secret_ids=[missing_id],
                timeout_seconds=600,
            ),
        )
    )
    session = await create_session(session_repository, agent.id)
    job, _ = await create_replay(replay_service, replay_create(session.id))
    with pytest.raises(SecretNotFound, match=f"Secret {missing_id} was not found"):
        await service.get_spec(job.id, actor=ACTOR)


async def link_result_session(
    repository: FakeJobRepository,
    session_repository: FakeSessionRepository,
    job_id: uuid.UUID,
    agent_id: uuid.UUID,
) -> Session:
    """Link a completed result session to a running job.

    Args:
        repository: Fake job repository.
        session_repository: Fake session repository.
        job_id: Id of the job.
        agent_id: Id of the agent.

    Returns:
        Linked result session.
    """
    result = await session_repository.create(
        Session(
            owner_id=ACTOR.account.id,
            agent_id=agent_id,
            origin=SessionOrigin.REPLAY,
            status=SessionStatus.COMPLETED,
        )
    )
    running = await repository.get(job_id)
    running.link_result_session(result.id)
    await repository.update(running)
    return result


async def run_score_jobs(
    service: JobService,
    repository: FakeJobRepository,
    job_id: uuid.UUID,
    scores: dict[str, float],
) -> None:
    """Run every score job of a replay to completion.

    Args:
        service: Job service.
        repository: Fake job repository.
        job_id: Id of the parent replay.
        scores: Score values by scorer name.
    """
    for child in await repository.list_children(job_id):
        assert isinstance(child, Score)
        await service.update_job(
            child.id, JobUpdate(status=JobStatus.RUNNING), actor=ACTOR
        )
        await service.update_job(
            child.id,
            JobUpdate(
                status=JobStatus.COMPLETED,
                result=scores[child.scorer_config.name],
            ),
            actor=ACTOR,
        )


async def test_update_job_standalone_lifecycle(
    replay_repository: FakeReplayRepository,
    service: JobService,
    replay_service: ReplayService,
    repository: FakeJobRepository,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Run a standalone job from pending to completed with a diff."""
    session = await create_session(session_repository, agent.id)
    job, replay = await create_replay(replay_service, replay_create(session.id))
    updated = await service.update_job(
        job.id, JobUpdate(status=JobStatus.RUNNING), actor=ACTOR
    )
    assert updated.status is JobStatus.RUNNING
    assert updated.started_at is not None

    result = await link_result_session(repository, session_repository, job.id, agent.id)
    completed = await service.update_job(
        job.id, JobUpdate(status=JobStatus.COMPLETED), actor=ACTOR
    )
    assert completed.status is JobStatus.COMPLETED
    assert completed.result_session_id == result.id
    assert completed.ended_at is not None
    # The score jobs exist before the completion response returns.
    assert len(await repository.list_children(job.id)) == 2

    await run_score_jobs(service, repository, job.id, {"conciseness": 0.8})
    settled = await replay_repository.get(replay.id)
    assert settled.passed is True
    assert settled.score == 0.8
    assert settled.scores == {"conciseness": 0.8}
    assert settled.diff is not None
    assert settled.diff["status_changed"] is False
    assert settled.diff["tool_calls"] == {
        "matched": 0,
        "mocked": 0,
        "added": 0,
        "removed": 0,
    }


async def test_update_job_completion_requires_a_completed_result_session(
    service: JobService,
    replay_service: ReplayService,
    repository: FakeJobRepository,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Reject completing a replay without a completed result session."""
    session = await create_session(session_repository, agent.id)
    job, _ = await create_replay(replay_service, replay_create(session.id))
    await service.update_job(job.id, JobUpdate(status=JobStatus.RUNNING), actor=ACTOR)
    with pytest.raises(
        JobMissingResultSession,
        match=f"Job {job.id} has no result session",
    ):
        await service.update_job(
            job.id, JobUpdate(status=JobStatus.COMPLETED), actor=ACTOR
        )
    open_session = await session_repository.create(
        Session(
            owner_id=ACTOR.account.id,
            agent_id=agent.id,
            origin=SessionOrigin.REPLAY,
            status=SessionStatus.IN_PROGRESS,
        )
    )
    running = await repository.get(job.id)
    running.link_result_session(open_session.id)
    await repository.update(running)
    with pytest.raises(
        JobResultSessionNotCompleted,
        match=f"Result session {open_session.id} of job {job.id} is not completed",
    ):
        await service.update_job(
            job.id, JobUpdate(status=JobStatus.COMPLETED), actor=ACTOR
        )


async def test_update_job_illegal_transitions(
    service: JobService,
    replay_service: ReplayService,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Reject illegal runner transitions."""
    session = await create_session(session_repository, agent.id)
    job, _ = await create_replay(replay_service, replay_create(session.id))
    with pytest.raises(
        InvalidJobTransition,
        match=f"Job {job.id} cannot transition from 'pending' to 'completed'",
    ):
        await service.update_job(
            job.id, JobUpdate(status=JobStatus.COMPLETED), actor=ACTOR
        )
    with pytest.raises(InvalidJobTransition):
        await service.update_job(
            job.id, JobUpdate(status=JobStatus.PENDING), actor=ACTOR
        )
    with pytest.raises(InvalidJob, match="Failing a job requires an error"):
        await service.update_job(
            job.id, JobUpdate(status=JobStatus.FAILED), actor=ACTOR
        )
    with pytest.raises(InvalidJob, match="Timing out a job requires an error"):
        await service.update_job(
            job.id, JobUpdate(status=JobStatus.TIMED_OUT), actor=ACTOR
        )


async def test_update_job_finalizes_run(
    service: JobService,
    repository: FakeJobRepository,
    session_repository: FakeSessionRepository,
    cohort_repository: FakeCohortRepository,
    config_repository: FakeReplayConfigRepository,
    experiment_repository: FakeExperimentRepository,
    run_repository: FakeExperimentRunRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Finalize the run when its last job goes terminal."""
    sessions = [await create_session(session_repository, agent.id) for _ in range(2)]
    run, jobs = await seed_run(
        session_repository,
        cohort_repository,
        config_repository,
        experiment_repository,
        run_repository,
        repository,
        agent,
        version,
        sessions,
    )
    first, second = jobs
    await repository.claim_pending(WORKER_ID, 2, WorkerScope(experiment_run_id=run.id))
    failed = await service.update_job(
        first.id,
        JobUpdate(status=JobStatus.FAILED, error="agent exited with code 1"),
        actor=ACTOR,
    )
    assert failed.status is JobStatus.FAILED
    # One terminal job does not finalize the run.
    assert (await run_repository.get(run.id)).status is ExperimentRunStatus.PENDING

    await service.update_job(
        second.id, JobUpdate(status=JobStatus.RUNNING), actor=ACTOR
    )
    result = await session_repository.create(
        Session(
            owner_id=ACTOR.account.id,
            agent_id=agent.id,
            origin=SessionOrigin.REPLAY,
            status=SessionStatus.COMPLETED,
            cost=Decimal("0.10"),
        )
    )
    running = await repository.get(second.id)
    running.link_result_session(result.id)
    await repository.update(running)
    await service.update_job(
        second.id, JobUpdate(status=JobStatus.COMPLETED), actor=ACTOR
    )
    await run_score_jobs(service, repository, second.id, {"conciseness": 0.9})
    finalized = await run_repository.get(run.id)
    assert finalized.status is ExperimentRunStatus.FAILED
    assert finalized.error == "1 of 2 jobs failed"
    assert finalized.ended_at is not None
    assert finalized.summary is not None
    assert finalized.summary["replay_counts_by_status"] == {
        "failed": 1,
        "completed": 1,
    }
    assert finalized.summary["pass_rate"] == 0.5
    assert finalized.summary["total_cost"]["replay"] == pytest.approx(0.1)


async def test_heartbeat_worker_touches_owned_active_jobs(
    service: JobService,
    repository: FakeJobRepository,
    session_repository: FakeSessionRepository,
    cohort_repository: FakeCohortRepository,
    config_repository: FakeReplayConfigRepository,
    experiment_repository: FakeExperimentRepository,
    run_repository: FakeExperimentRunRepository,
    worker_repository: FakeWorkerRepository,
    worker: Worker,
    other_worker: Worker,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Record one heartbeat per owned active job and bump the worker."""
    sessions = [await create_session(session_repository, agent.id) for _ in range(2)]
    run, jobs = await seed_run(
        session_repository,
        cohort_repository,
        config_repository,
        experiment_repository,
        run_repository,
        repository,
        agent,
        version,
        sessions,
    )
    owned, foreign = jobs
    await repository.claim_pending(worker.id, 1, WorkerScope(experiment_run_id=run.id))
    await repository.claim_pending(
        other_worker.id, 1, WorkerScope(experiment_run_id=run.id)
    )
    before = worker.last_seen_at

    abandon = await service.heartbeat_worker(
        worker.id, [owned.id, foreign.id], actor=ACTOR
    )

    assert abandon == [foreign.id]
    assert (await repository.get(owned.id)).heartbeat_at is not None
    assert (await worker_repository.get(worker.id)).last_seen_at > before


async def test_heartbeat_worker_abandons_terminal_and_unknown_jobs(
    service: JobService,
    replay_service: ReplayService,
    repository: FakeJobRepository,
    session_repository: FakeSessionRepository,
    worker: Worker,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Abandon reported jobs the heartbeat does not reach."""
    session = await create_session(session_repository, agent.id)
    job, _ = await create_replay(replay_service, replay_create(session.id))
    await repository.claim_pending(worker.id, 1, WorkerScope())
    terminal = await repository.get(job.id)
    terminal.cancel()
    await repository.update(terminal)
    missing_id = uuid.uuid4()

    abandon = await service.heartbeat_worker(
        worker.id, [job.id, missing_id], actor=ACTOR
    )

    assert abandon == [job.id, missing_id]


async def test_heartbeat_worker_abandons_jobs_of_a_canceling_run(
    service: JobService,
    repository: FakeJobRepository,
    session_repository: FakeSessionRepository,
    cohort_repository: FakeCohortRepository,
    config_repository: FakeReplayConfigRepository,
    experiment_repository: FakeExperimentRepository,
    run_repository: FakeExperimentRunRepository,
    worker: Worker,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Abandon a reached job whose experiment run is canceling."""
    session = await create_session(session_repository, agent.id)
    run, jobs = await seed_run(
        session_repository,
        cohort_repository,
        config_repository,
        experiment_repository,
        run_repository,
        repository,
        agent,
        version,
        [session],
    )
    job = jobs[0]
    await repository.claim_pending(worker.id, 1, WorkerScope(experiment_run_id=run.id))
    assert await service.heartbeat_worker(worker.id, [job.id], actor=ACTOR) == []

    canceling = await run_repository.get(run.id)
    canceling.cancel()
    await run_repository.update(canceling)

    assert await service.heartbeat_worker(worker.id, [job.id], actor=ACTOR) == [job.id]


async def test_heartbeat_worker_requires_a_registered_worker(
    service: JobService,
) -> None:
    """Reject a heartbeat from an unknown worker."""
    worker_id = uuid.uuid4()
    with pytest.raises(WorkerNotFound, match=f"Worker {worker_id} was not found"):
        await service.heartbeat_worker(worker_id, [], actor=ACTOR)


def build_service(
    repository: FakeJobRepository,
    replay_repository: FakeReplayRepository,
    config_repository: FakeReplayConfigRepository,
    session_repository: FakeSessionRepository,
    agent_repository: FakeAgentRepository,
    version_repository: FakeAgentVersionRepository,
    node_repository: FakeSessionNodeRepository,
    run_repository: FakeExperimentRunRepository,
    experiment_repository: FakeExperimentRepository,
    cohort_repository: FakeCohortRepository,
    secret_repository: FakeSecretRepository,
    worker_repository: FakeWorkerRepository,
    plugin_repository: FakePluginRepository,
    blob_repository: FakeBlobRepository,
    heartbeat_timeout_seconds: int = 60,
    max_attempts: int = 3,
) -> JobService:
    """Build a job service with explicit staleness settings.

    Args:
        repository: Fake job repository.
        replay_repository: Fake replay repository.
        config_repository: Fake replay config repository.
        session_repository: Fake session repository.
        agent_repository: Fake agent repository.
        version_repository: Fake agent version repository.
        node_repository: Fake session node repository.
        run_repository: Fake experiment run repository.
        experiment_repository: Fake experiment repository.
        cohort_repository: Fake cohort repository.
        secret_repository: Fake secret repository.
        worker_repository: Fake worker repository.
        plugin_repository: Fake plugin repository.
        blob_repository: Fake blob repository.
        heartbeat_timeout_seconds: Heartbeat timeout, negative values mark
            every claim stale immediately.
        max_attempts: Attempt count at which a stale job times out.

    Returns:
        Job service.
    """
    return JobService(
        repository=repository,
        replay_repository=replay_repository,
        replay_config_repository=config_repository,
        session_repository=session_repository,
        agent_repository=agent_repository,
        agent_version_repository=version_repository,
        session_node_repository=node_repository,
        experiment_run_repository=run_repository,
        experiment_repository=experiment_repository,
        cohort_repository=cohort_repository,
        secret_repository=secret_repository,
        worker_repository=worker_repository,
        plugin_repository=plugin_repository,
        blob_repository=blob_repository,
        heartbeat_timeout_seconds=heartbeat_timeout_seconds,
        max_attempts=max_attempts,
        worker_liveness_timeout_seconds=60,
    )


async def claim_one(service: JobService, worker: Worker, job_id: uuid.UUID) -> None:
    """Claim one job through a job-pinned scope.

    Args:
        service: Job service.
        worker: Claiming worker.
        job_id: Id of the job.
    """
    await service.claim_jobs(
        worker_id=worker.id,
        max_jobs=1,
        scope=WorkerScope(job_id=job_id),
        actor=ACTOR,
    )


async def test_claim_jobs_job_scope(
    service: JobService,
    repository: FakeJobRepository,
    replay_service: ReplayService,
    session_repository: FakeSessionRepository,
    worker: Worker,
    other_worker: Worker,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Claim a pending job through a job-pinned scope, once."""
    session = await create_session(session_repository, agent.id)
    job, _ = await create_replay(replay_service, replay_create(session.id))
    other = await create_session(session_repository, agent.id)
    await create_replay(replay_service, replay_create(other.id))
    claimed = await service.claim_jobs(
        worker_id=worker.id,
        max_jobs=10,
        scope=WorkerScope(job_id=job.id),
        actor=ACTOR,
    )
    assert [entry.id for entry, _ in claimed] == [job.id]
    assert claimed[0][0].worker_id == worker.id
    assert (await repository.get(job.id)).status is JobStatus.CLAIMED

    assert (
        await service.claim_jobs(
            worker_id=other_worker.id,
            max_jobs=10,
            scope=WorkerScope(job_id=job.id),
            actor=ACTOR,
        )
        == []
    )


async def test_claim_jobs_resolves_stale_claim(
    replay_repository: FakeReplayRepository,
    replay_service: ReplayService,
    repository: FakeJobRepository,
    config_repository: FakeReplayConfigRepository,
    session_repository: FakeSessionRepository,
    agent_repository: FakeAgentRepository,
    version_repository: FakeAgentVersionRepository,
    node_repository: FakeSessionNodeRepository,
    run_repository: FakeExperimentRunRepository,
    experiment_repository: FakeExperimentRepository,
    cohort_repository: FakeCohortRepository,
    secret_repository: FakeSecretRepository,
    worker_repository: FakeWorkerRepository,
    plugin_repository: FakePluginRepository,
    blob_repository: FakeBlobRepository,
    worker: Worker,
    other_worker: Worker,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Reclaim a standalone job whose worker lost its heartbeat."""
    stale_service = build_service(
        repository,
        replay_repository,
        config_repository,
        session_repository,
        agent_repository,
        version_repository,
        node_repository,
        run_repository,
        experiment_repository,
        cohort_repository,
        secret_repository,
        worker_repository,
        plugin_repository,
        blob_repository,
        heartbeat_timeout_seconds=-60,
    )
    session = await create_session(session_repository, agent.id)
    job, _ = await create_replay(replay_service, replay_create(session.id))
    await claim_one(stale_service, worker, job.id)

    claimed = await stale_service.claim_jobs(
        worker_id=other_worker.id,
        max_jobs=1,
        scope=WorkerScope(job_id=job.id),
        actor=ACTOR,
    )
    assert len(claimed) == 1
    assert claimed[0][0].status is JobStatus.CLAIMED
    assert claimed[0][0].worker_id == other_worker.id
    assert claimed[0][0].attempt == 2


async def test_claim_jobs_times_out_exhausted_stale_claim(
    replay_repository: FakeReplayRepository,
    replay_service: ReplayService,
    repository: FakeJobRepository,
    config_repository: FakeReplayConfigRepository,
    session_repository: FakeSessionRepository,
    agent_repository: FakeAgentRepository,
    version_repository: FakeAgentVersionRepository,
    node_repository: FakeSessionNodeRepository,
    run_repository: FakeExperimentRunRepository,
    experiment_repository: FakeExperimentRepository,
    cohort_repository: FakeCohortRepository,
    secret_repository: FakeSecretRepository,
    worker_repository: FakeWorkerRepository,
    plugin_repository: FakePluginRepository,
    blob_repository: FakeBlobRepository,
    worker: Worker,
    other_worker: Worker,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Persist the time-out of a stale claim out of attempts and reject."""
    stale_service = build_service(
        repository,
        replay_repository,
        config_repository,
        session_repository,
        agent_repository,
        version_repository,
        node_repository,
        run_repository,
        experiment_repository,
        cohort_repository,
        secret_repository,
        worker_repository,
        plugin_repository,
        blob_repository,
        heartbeat_timeout_seconds=-60,
        max_attempts=1,
    )
    session = await create_session(session_repository, agent.id)
    job, _ = await create_replay(replay_service, replay_create(session.id))
    await claim_one(stale_service, worker, job.id)

    assert (
        await stale_service.claim_jobs(
            worker_id=other_worker.id,
            max_jobs=1,
            scope=WorkerScope(job_id=job.id),
            actor=ACTOR,
        )
        == []
    )
    stored = await repository.get(job.id)
    assert stored.status is JobStatus.TIMED_OUT
    assert stored.error == HEARTBEAT_TIMEOUT_ERROR


async def test_release_job(
    service: JobService,
    replay_service: ReplayService,
    repository: FakeJobRepository,
    session_repository: FakeSessionRepository,
    worker: Worker,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Requeue a claimed or running job and reject other statuses."""
    session = await create_session(session_repository, agent.id)
    job, _ = await create_replay(replay_service, replay_create(session.id))
    with pytest.raises(
        InvalidJobTransition,
        match=f"Job {job.id} cannot transition from 'pending' to 'pending'",
    ):
        await service.release_job(job.id, actor=ACTOR)

    await claim_one(service, worker, job.id)
    released = await service.release_job(job.id, actor=ACTOR)
    assert released.status is JobStatus.PENDING
    assert released.attempt == 2
    assert released.worker_id is None
    assert released.claimed_at is None
    assert released.heartbeat_at is None

    await service.update_job(job.id, JobUpdate(status=JobStatus.RUNNING), actor=ACTOR)
    released = await service.release_job(job.id, actor=ACTOR)
    assert released.status is JobStatus.PENDING
    assert released.attempt == 3

    failed = await repository.get(job.id)
    failed = failed.model_copy(update={"status": JobStatus.FAILED})
    await repository.update(failed)
    with pytest.raises(InvalidJobTransition):
        await service.release_job(job.id, actor=ACTOR)


async def test_release_run_job(
    service: JobService,
    repository: FakeJobRepository,
    session_repository: FakeSessionRepository,
    cohort_repository: FakeCohortRepository,
    config_repository: FakeReplayConfigRepository,
    experiment_repository: FakeExperimentRepository,
    run_repository: FakeExperimentRunRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Requeue a claimed run job through release."""
    session = await create_session(session_repository, agent.id)
    run, jobs = await seed_run(
        session_repository,
        cohort_repository,
        config_repository,
        experiment_repository,
        run_repository,
        repository,
        agent,
        version,
        [session],
    )
    await repository.claim_pending(WORKER_ID, 1, WorkerScope(experiment_run_id=run.id))
    released = await service.release_job(jobs[0].id, actor=ACTOR)
    assert released.status is JobStatus.PENDING
    assert released.attempt == 2
    assert released.worker_id is None


async def test_retry_job(
    service: JobService,
    replay_service: ReplayService,
    repository: FakeJobRepository,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Requeue a failed standalone job and clear its attempt state."""
    session = await create_session(session_repository, agent.id)
    job, _ = await create_replay(replay_service, replay_create(session.id))
    await service.update_job(job.id, JobUpdate(status=JobStatus.RUNNING), actor=ACTOR)
    await link_result_session(repository, session_repository, job.id, agent.id)
    await service.update_job(
        job.id,
        JobUpdate(status=JobStatus.FAILED, error="agent exited with code 1"),
        actor=ACTOR,
    )

    retried = await service.retry_job(job.id, actor=ACTOR)
    assert retried.status is JobStatus.PENDING
    assert retried.attempt == 2
    assert retried.error is None
    assert retried.result_session_id is None
    assert retried.started_at is None
    assert retried.ended_at is None
    stored = await repository.get(job.id)
    assert stored.result_session_id is None

    with pytest.raises(
        InvalidJobTransition,
        match=f"Job {job.id} cannot transition from 'pending' to 'pending'",
    ):
        await service.retry_job(job.id, actor=ACTOR)


async def test_retry_job_rejects_run_job(
    service: JobService,
    repository: FakeJobRepository,
    session_repository: FakeSessionRepository,
    cohort_repository: FakeCohortRepository,
    config_repository: FakeReplayConfigRepository,
    experiment_repository: FakeExperimentRepository,
    run_repository: FakeExperimentRunRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Reject retrying a run job."""
    session = await create_session(session_repository, agent.id)
    run, jobs = await seed_run(
        session_repository,
        cohort_repository,
        config_repository,
        experiment_repository,
        run_repository,
        repository,
        agent,
        version,
        [session],
    )
    await repository.claim_pending(WORKER_ID, 1, WorkerScope(experiment_run_id=run.id))
    failed = await repository.get(jobs[0].id)
    failed.fail("agent exited with code 1")
    await repository.update(failed)
    with pytest.raises(
        JobNotStandalone,
        match=f"Job {failed.id} belongs to an experiment run",
    ):
        await service.retry_job(failed.id, actor=ACTOR)


async def test_delete_job(
    service: JobService,
    replay_service: ReplayService,
    repository: FakeJobRepository,
    config_repository: FakeReplayConfigRepository,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Delete a pending standalone job and its unreferenced config."""
    session = await create_session(session_repository, agent.id)
    job, config = await create_replay(replay_service, replay_create(session.id))
    await service.delete_job(job.id, actor=ACTOR)
    with pytest.raises(JobNotFound):
        await repository.get(job.id)
    with pytest.raises(ReplayConfigNotFound):
        await config_repository.get(config.id)


async def test_delete_job_conflicts(
    service: JobService,
    replay_service: ReplayService,
    repository: FakeJobRepository,
    session_repository: FakeSessionRepository,
    cohort_repository: FakeCohortRepository,
    config_repository: FakeReplayConfigRepository,
    experiment_repository: FakeExperimentRepository,
    run_repository: FakeExperimentRunRepository,
    worker: Worker,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Reject deleting a claimed or running job or a run job."""
    session = await create_session(session_repository, agent.id)
    job, _ = await create_replay(replay_service, replay_create(session.id))
    await claim_one(service, worker, job.id)
    with pytest.raises(JobActive, match=f"Job {job.id} is claimed or running"):
        await service.delete_job(job.id, actor=ACTOR)
    await service.update_job(job.id, JobUpdate(status=JobStatus.RUNNING), actor=ACTOR)
    with pytest.raises(JobActive):
        await service.delete_job(job.id, actor=ACTOR)

    other = await create_session(session_repository, agent.id)
    _, jobs = await seed_run(
        session_repository,
        cohort_repository,
        config_repository,
        experiment_repository,
        run_repository,
        repository,
        agent,
        version,
        [other],
    )
    with pytest.raises(
        JobNotStandalone,
        match=f"Job {jobs[0].id} belongs to an experiment run",
    ):
        await service.delete_job(jobs[0].id, actor=ACTOR)


async def store_tool_result(
    node_repository: FakeSessionNodeRepository,
    session_id: uuid.UUID,
    tool_name: str = "get_weather",
    inputs: object = None,
    outputs: object = None,
    mocked: bool = False,
    sequence: int = 0,
) -> SessionNode:
    """Store a completed tool call node with its computed cache key.

    Args:
        node_repository: Fake session node repository.
        session_id: Id of the session.
        tool_name: Name of the tool.
        inputs: Tool call inputs.
        outputs: Tool call outputs.
        mocked: Whether the node is marked mocked.
        sequence: Node sequence.

    Returns:
        Stored node.
    """
    stored = await node_repository.upsert(
        [
            SessionNode(
                session_id=session_id,
                key=f"tool_call:{tool_name}#{sequence + 1}",
                sequence=sequence,
                node_type=NodeType.TOOL_CALL,
                name=tool_name,
                status=NodeStatus.COMPLETED,
                tool_name=tool_name,
                cache_key=tool_call_cache_key(tool_name, inputs),
                inputs=inputs,
                outputs=outputs,
                attributes={"mocked": True} if mocked else {},
            )
        ]
    )
    return stored[0]


async def test_tool_lookup_original_session_scope(
    service: JobService,
    replay_service: ReplayService,
    session_repository: FakeSessionRepository,
    node_repository: FakeSessionNodeRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Find recorded tool results within the original session."""
    session = await create_session(session_repository, agent.id)
    other = await create_session(session_repository, agent.id)
    inputs = {"city": "Berlin"}
    await store_tool_result(
        node_repository, session.id, inputs=inputs, outputs={"temp": 21}
    )
    await store_tool_result(
        node_repository, other.id, inputs=inputs, outputs={"temp": 99}
    )
    job, _ = await create_replay(replay_service, replay_create(session.id))
    cache_key = tool_call_cache_key("get_weather", inputs)
    found = await service.tool_lookup(
        job.id, "get_weather", inputs, cache_key, actor=ACTOR
    )
    assert found is not None
    assert found.outputs == {"temp": 21}

    miss = await service.tool_lookup(
        job.id,
        "get_weather",
        {"city": "Paris"},
        tool_call_cache_key("get_weather", {"city": "Paris"}),
        actor=ACTOR,
    )
    assert miss is None


async def test_tool_lookup_agent_scope(
    service: JobService,
    replay_service: ReplayService,
    session_repository: FakeSessionRepository,
    node_repository: FakeSessionNodeRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Find recorded tool results across the agent's sessions."""
    session = await create_session(session_repository, agent.id)
    other = await create_session(session_repository, agent.id)
    inputs = {"city": "Berlin"}
    await store_tool_result(
        node_repository, other.id, inputs=inputs, outputs={"temp": 21}
    )
    job, _ = await create_replay(
        replay_service,
        replay_create(
            session.id,
            tool_policy=ToolPolicyConfig(
                default=HistoryPolicy(scope=HistoryScope.AGENT)
            ),
        ),
    )
    found = await service.tool_lookup(
        job.id,
        "get_weather",
        inputs,
        tool_call_cache_key("get_weather", inputs),
        actor=ACTOR,
    )
    assert found is not None
    assert found.session_id == other.id


async def test_tool_lookup_cohort_scope(
    service: JobService,
    repository: FakeJobRepository,
    session_repository: FakeSessionRepository,
    node_repository: FakeSessionNodeRepository,
    cohort_repository: FakeCohortRepository,
    config_repository: FakeReplayConfigRepository,
    experiment_repository: FakeExperimentRepository,
    run_repository: FakeExperimentRunRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Find recorded tool results across the experiment's cohort."""
    sessions = [await create_session(session_repository, agent.id) for _ in range(2)]
    outside = await create_session(session_repository, agent.id)
    inputs = {"city": "Berlin"}
    await store_tool_result(
        node_repository, sessions[1].id, inputs=inputs, outputs={"temp": 21}
    )
    await store_tool_result(
        node_repository, outside.id, inputs={"city": "Paris"}, outputs={"temp": 9}
    )
    _, jobs = await seed_run(
        session_repository,
        cohort_repository,
        config_repository,
        experiment_repository,
        run_repository,
        repository,
        agent,
        version,
        sessions,
        tool_policy=ToolPolicyConfig(default=HistoryPolicy(scope=HistoryScope.COHORT)),
    )
    job = next(entry for entry in jobs if entry.input_session_id == sessions[0].id)
    found = await service.tool_lookup(
        job.id,
        "get_weather",
        inputs,
        tool_call_cache_key("get_weather", inputs),
        actor=ACTOR,
    )
    assert found is not None
    assert found.session_id == sessions[1].id

    paris = {"city": "Paris"}
    miss = await service.tool_lookup(
        job.id,
        "get_weather",
        paris,
        tool_call_cache_key("get_weather", paris),
        actor=ACTOR,
    )
    assert miss is None


async def test_tool_lookup_cohort_scope_standalone(
    replay_repository: FakeReplayRepository,
    service: JobService,
    repository: FakeJobRepository,
    session_repository: FakeSessionRepository,
    config_repository: FakeReplayConfigRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Reject a cohort scope on a standalone job."""
    session = await create_session(session_repository, agent.id)
    config = await config_repository.create(
        ReplayConfig(
            owner_id=ACTOR.account.id,
            tool_policy=ToolPolicyConfig(
                default=HistoryPolicy(scope=HistoryScope.COHORT)
            ),
            scoring_policy=SCORING_POLICY,
        )
    )
    job = await repository.create(
        ReplayJob(
            agent_version_id=version.id,
            input_session_id=session.id,
            execution_target=ExecutionTarget.POOL,
        )
    )
    await replay_repository.create(
        Replay(
            owner_id=ACTOR.account.id,
            job_id=job.id,
            replay_config_id=config.id,
            input_session_id=session.id,
        )
    )
    inputs = {"city": "Berlin"}
    with pytest.raises(
        InvalidReplayConfig,
        match="Standalone replays cannot use history scope 'cohort'",
    ):
        await service.tool_lookup(
            job.id,
            "get_weather",
            inputs,
            tool_call_cache_key("get_weather", inputs),
            actor=ACTOR,
        )


async def test_tool_lookup_rejects_mismatch_and_non_history(
    service: JobService,
    replay_service: ReplayService,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Reject cache key mismatches and tools without a history policy."""
    session = await create_session(session_repository, agent.id)
    job, _ = await create_replay(
        replay_service,
        replay_create(
            session.id,
            tool_policy=ToolPolicyConfig(
                default=HistoryPolicy(),
                tools={"search": PassthroughPolicy()},
            ),
        ),
    )
    with pytest.raises(
        InvalidToolLookup, match="Cache key does not match the tool name and inputs"
    ):
        await service.tool_lookup(
            job.id, "get_weather", {"city": "Berlin"}, "a" * 64, actor=ACTOR
        )
    inputs = {"query": "kitaru"}
    with pytest.raises(
        InvalidToolLookup, match="Tool 'search' resolves to no history policy"
    ):
        await service.tool_lookup(
            job.id,
            "search",
            inputs,
            tool_call_cache_key("search", inputs),
            actor=ACTOR,
        )


async def test_get_job_reports_staleness(
    replay_repository: FakeReplayRepository,
    repository: FakeJobRepository,
    config_repository: FakeReplayConfigRepository,
    session_repository: FakeSessionRepository,
    agent_repository: FakeAgentRepository,
    version_repository: FakeAgentVersionRepository,
    node_repository: FakeSessionNodeRepository,
    run_repository: FakeExperimentRunRepository,
    experiment_repository: FakeExperimentRepository,
    cohort_repository: FakeCohortRepository,
    secret_repository: FakeSecretRepository,
    worker_repository: FakeWorkerRepository,
    plugin_repository: FakePluginRepository,
    blob_repository: FakeBlobRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Report stale claims as pending on reads without writing."""
    stale_service = build_service(
        repository,
        replay_repository,
        config_repository,
        session_repository,
        agent_repository,
        version_repository,
        node_repository,
        run_repository,
        experiment_repository,
        cohort_repository,
        secret_repository,
        worker_repository,
        plugin_repository,
        blob_repository,
        heartbeat_timeout_seconds=-60,
    )
    session = await create_session(session_repository, agent.id)
    _, jobs = await seed_run(
        session_repository,
        cohort_repository,
        config_repository,
        experiment_repository,
        run_repository,
        repository,
        agent,
        version,
        [session],
    )
    job = jobs[0]
    assert job.experiment_run_id is not None
    await repository.claim_pending(
        WORKER_ID, 1, WorkerScope(experiment_run_id=job.experiment_run_id)
    )
    reported = await stale_service.get_job(job.id, actor=ACTOR)
    assert reported.status is JobStatus.PENDING
    assert reported.attempt == 2
    assert reported.worker_id is None
    # Reporting never writes.
    stored = await repository.get(job.id)
    assert stored.status is JobStatus.CLAIMED


def session_run_create(**overrides: object) -> SessionRunCreate:
    """Build a session run create command.

    Args:
        **overrides: Field overrides.

    Returns:
        Session run create command.
    """
    return SessionRunCreate.model_validate(dict(overrides))


async def test_create_session_run_resolves_latest_runnable(
    service: JobService,
    version_repository: FakeAgentVersionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Resolve the latest runnable version of the agent."""
    latest = await version_repository.create(
        AgentVersion(
            owner_id=ACTOR.account.id,
            agent_id=agent.id,
            version="v2",
            run_spec=RunSpec(command="python agent.py", timeout_seconds=600),
        )
    )
    job = await service.create_session_run(
        session_run_create(agent_id=agent.id, inputs={"prompt": "hi"}, name="smoke"),
        actor=ACTOR,
    )
    assert job.agent_version_id == latest.id
    assert job.kind is JobKind.SESSION_RUN
    assert job.inputs == {"prompt": "hi"}
    assert job.name == "smoke"
    assert job.status is JobStatus.PENDING
    assert job.execution_target is ExecutionTarget.POOL
    assert job.result_session_id is None
    assert job.created is not None


async def test_create_session_run_explicit_version(
    service: JobService,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Resolve an explicit agent version without an agent id."""
    job = await service.create_session_run(
        session_run_create(agent_version_id=version.id), actor=ACTOR
    )
    assert job.agent_version_id == version.id
    assert job.inputs is None
    assert job.name is None


async def test_create_session_run_cross_agent_version(
    service: JobService,
    agent_repository: FakeAgentRepository,
    version_repository: FakeAgentVersionRepository,
    agent: Agent,
) -> None:
    """Reject a version that belongs to another agent."""
    other = await agent_repository.create(
        Agent(owner_id=ACTOR.account.id, name="triage-bot")
    )
    other_version = await version_repository.create(
        AgentVersion(
            owner_id=ACTOR.account.id,
            agent_id=other.id,
            version="v1",
            run_spec=RunSpec(command="python agent.py", timeout_seconds=600),
        )
    )
    with pytest.raises(
        InvalidJob,
        match=f"Agent version {other_version.id} does not belong to agent {agent.id}",
    ):
        await service.create_session_run(
            session_run_create(agent_id=agent.id, agent_version_id=other_version.id),
            actor=ACTOR,
        )


async def test_create_session_run_no_runnable_version(
    service: JobService, agent: Agent
) -> None:
    """Raise when the agent has no runnable version."""
    with pytest.raises(
        NoRunnableAgentVersion, match=f"Agent {agent.id} has no runnable version"
    ):
        await service.create_session_run(
            session_run_create(agent_id=agent.id), actor=ACTOR
        )


async def test_create_session_run_version_without_run_spec(
    service: JobService,
    version_repository: FakeAgentVersionRepository,
    agent: Agent,
) -> None:
    """Reject an explicit version without a run spec."""
    bare = await version_repository.create(
        AgentVersion(owner_id=ACTOR.account.id, agent_id=agent.id, version="v1")
    )
    with pytest.raises(
        AgentVersionNotRunnable, match=f"Agent version {bare.id} has no run spec"
    ):
        await service.create_session_run(
            session_run_create(agent_version_id=bare.id), actor=ACTOR
        )


async def test_create_session_run_on_demand_without_image(
    service: JobService,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Reject an on demand target without a run image."""
    with pytest.raises(
        MissingRunImage, match=f"Agent version {version.id} has no run image"
    ):
        await service.create_session_run(
            session_run_create(
                agent_id=agent.id, execution_target=ExecutionTarget.ON_DEMAND
            ),
            actor=ACTOR,
        )


async def test_create_session_run_on_demand_with_image(
    service: JobService,
    version_repository: FakeAgentVersionRepository,
    agent: Agent,
) -> None:
    """Stamp an explicit on demand target on the job."""
    version = await version_repository.create(
        AgentVersion(
            owner_id=ACTOR.account.id,
            agent_id=agent.id,
            version="v1",
            run_spec=RunSpec(
                command="python agent.py",
                timeout_seconds=600,
                image="agent:v1",
            ),
        )
    )
    job = await service.create_session_run(
        session_run_create(
            agent_version_id=version.id, execution_target=ExecutionTarget.ON_DEMAND
        ),
        actor=ACTOR,
    )
    assert job.execution_target is ExecutionTarget.ON_DEMAND


async def test_create_session_run_warns_without_live_worker(
    service: JobService,
    worker_repository: FakeWorkerRepository,
    agent: Agent,
    version: AgentVersion,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Warn on a pool target when no live worker serves the agent version."""
    with caplog.at_level("WARNING"):
        await service.create_session_run(
            session_run_create(agent_id=agent.id), actor=ACTOR
        )
    assert f"No live worker serves agent version {version.id}" in caplog.text

    caplog.clear()
    await create_worker(worker_repository, ACTOR.account.id, name="catch-all")
    with caplog.at_level("WARNING"):
        await service.create_session_run(
            session_run_create(agent_id=agent.id), actor=ACTOR
        )
    assert "No live worker" not in caplog.text


async def test_get_spec_session_run(
    service: JobService,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Resolve a kind-aware spec for a session run."""
    job = await service.create_session_run(
        session_run_create(agent_id=agent.id, inputs={"prompt": "hi"}, name="smoke"),
        actor=ACTOR,
    )
    spec = await service.get_spec(job.id, actor=ACTOR)
    assert spec.job_id == job.id
    assert spec.kind is JobKind.SESSION_RUN
    assert spec.inputs == {"prompt": "hi"}
    assert spec.name == "smoke"
    assert spec.override is None
    assert spec.tool_policy is None
    assert spec.scorer is None
    assert spec.input_session_id is None
    assert spec.run_spec == version.run_spec


async def test_claim_session_run_through_a_job_scope(
    service: JobService,
    worker: Worker,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Claim a session run through a job-pinned scope."""
    job = await service.create_session_run(
        session_run_create(agent_id=agent.id), actor=ACTOR
    )
    claimed = await service.claim_jobs(
        worker_id=worker.id,
        max_jobs=1,
        scope=WorkerScope(job_id=job.id),
        actor=ACTOR,
    )
    assert len(claimed) == 1
    assert claimed[0][0].status is JobStatus.CLAIMED
    assert claimed[0][0].worker_id == worker.id


async def test_update_session_run_lifecycle(
    service: JobService,
    repository: FakeJobRepository,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Run a session run from pending to completed without scoring."""
    job = await service.create_session_run(
        session_run_create(agent_id=agent.id), actor=ACTOR
    )
    updated = await service.update_job(
        job.id, JobUpdate(status=JobStatus.RUNNING), actor=ACTOR
    )
    assert updated.status is JobStatus.RUNNING

    with pytest.raises(
        JobMissingResultSession, match=f"Job {job.id} has no result session"
    ):
        await service.update_job(
            job.id, JobUpdate(status=JobStatus.COMPLETED), actor=ACTOR
        )

    await link_result_session(repository, session_repository, job.id, agent.id)
    completed = await service.update_job(
        job.id, JobUpdate(status=JobStatus.COMPLETED), actor=ACTOR
    )
    assert completed.status is JobStatus.COMPLETED
    assert completed.ended_at is not None


async def test_tool_lookup_rejects_session_run(
    service: JobService,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Reject a tool lookup on a session run."""
    job = await service.create_session_run(
        session_run_create(agent_id=agent.id), actor=ACTOR
    )
    inputs = {"city": "Berlin"}
    with pytest.raises(JobKindMismatch, match=f"Job {job.id} is not of kind 'replay'"):
        await service.tool_lookup(
            job.id,
            "get_weather",
            inputs,
            tool_call_cache_key("get_weather", inputs),
            actor=ACTOR,
        )


async def test_delete_session_run(
    service: JobService,
    repository: FakeJobRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Delete a pending session run."""
    job = await service.create_session_run(
        session_run_create(agent_id=agent.id), actor=ACTOR
    )
    await service.delete_job(job.id, actor=ACTOR)
    with pytest.raises(JobNotFound):
        await repository.get(job.id)


async def test_claim_jobs_run_scope(
    service: JobService,
    repository: FakeJobRepository,
    session_repository: FakeSessionRepository,
    cohort_repository: FakeCohortRepository,
    config_repository: FakeReplayConfigRepository,
    experiment_repository: FakeExperimentRepository,
    run_repository: FakeExperimentRunRepository,
    worker_repository: FakeWorkerRepository,
    worker: Worker,
    other_worker: Worker,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Claim run jobs, move the run to running, and bump the worker."""
    sessions = [await create_session(session_repository, agent.id) for _ in range(2)]
    run, _ = await seed_run(
        session_repository,
        cohort_repository,
        config_repository,
        experiment_repository,
        run_repository,
        repository,
        agent,
        version,
        sessions,
    )
    before = (await worker_repository.get(worker.id)).last_seen_at
    claimed = await service.claim_jobs(
        worker_id=worker.id,
        max_jobs=1,
        scope=WorkerScope(experiment_run_id=run.id),
        actor=ACTOR,
    )
    assert len(claimed) == 1
    job, spec = claimed[0]
    assert job.status is JobStatus.CLAIMED
    assert job.worker_id == worker.id
    assert job.claimed_at is not None
    assert spec.job_id == job.id
    assert spec.kind is JobKind.REPLAY
    assert spec.run_spec is not None
    started = await run_repository.get(run.id)
    assert started.status is ExperimentRunStatus.RUNNING
    assert started.started_at is not None
    assert (await worker_repository.get(worker.id)).last_seen_at > before

    remaining = await service.claim_jobs(
        worker_id=other_worker.id,
        max_jobs=5,
        scope=WorkerScope(experiment_run_id=run.id),
        actor=ACTOR,
    )
    assert len(remaining) == 1
    assert remaining[0][0].worker_id == other_worker.id

    assert (
        await service.claim_jobs(
            worker_id=other_worker.id,
            max_jobs=5,
            scope=WorkerScope(experiment_run_id=run.id),
            actor=ACTOR,
        )
        == []
    )


async def test_claim_jobs_unknown_worker(service: JobService) -> None:
    """Raise for an unknown worker id."""
    missing_id = uuid.uuid4()
    with pytest.raises(WorkerNotFound, match=f"Worker {missing_id} was not found"):
        await service.claim_jobs(
            worker_id=missing_id,
            max_jobs=1,
            scope=WorkerScope(),
            actor=ACTOR,
        )


async def test_claim_jobs_unknown_run(service: JobService, worker: Worker) -> None:
    """Raise for an unknown experiment run id."""
    missing_id = uuid.uuid4()
    with pytest.raises(
        ExperimentRunNotFound, match=f"Experiment run {missing_id} was not found"
    ):
        await service.claim_jobs(
            worker_id=worker.id,
            max_jobs=1,
            scope=WorkerScope(experiment_run_id=missing_id),
            actor=ACTOR,
        )


async def test_claim_jobs_canceling_run_returns_empty(
    service: JobService,
    repository: FakeJobRepository,
    session_repository: FakeSessionRepository,
    cohort_repository: FakeCohortRepository,
    config_repository: FakeReplayConfigRepository,
    experiment_repository: FakeExperimentRepository,
    run_repository: FakeExperimentRunRepository,
    worker: Worker,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Yield no jobs from a canceling run."""
    session = await create_session(session_repository, agent.id)
    run, _ = await seed_run(
        session_repository,
        cohort_repository,
        config_repository,
        experiment_repository,
        run_repository,
        repository,
        agent,
        version,
        [session],
    )
    canceling = await run_repository.get(run.id)
    canceling.cancel()
    await run_repository.update(canceling)
    assert (
        await service.claim_jobs(
            worker_id=worker.id,
            max_jobs=5,
            scope=WorkerScope(experiment_run_id=run.id),
            actor=ACTOR,
        )
        == []
    )


async def test_claim_jobs_requeues_and_times_out_stale_jobs(
    replay_repository: FakeReplayRepository,
    repository: FakeJobRepository,
    config_repository: FakeReplayConfigRepository,
    session_repository: FakeSessionRepository,
    agent_repository: FakeAgentRepository,
    version_repository: FakeAgentVersionRepository,
    node_repository: FakeSessionNodeRepository,
    run_repository: FakeExperimentRunRepository,
    experiment_repository: FakeExperimentRepository,
    cohort_repository: FakeCohortRepository,
    secret_repository: FakeSecretRepository,
    worker_repository: FakeWorkerRepository,
    plugin_repository: FakePluginRepository,
    blob_repository: FakeBlobRepository,
    worker: Worker,
    other_worker: Worker,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Requeue stale claims, then time them out and finalize the run."""
    stale_service = build_service(
        repository,
        replay_repository,
        config_repository,
        session_repository,
        agent_repository,
        version_repository,
        node_repository,
        run_repository,
        experiment_repository,
        cohort_repository,
        secret_repository,
        worker_repository,
        plugin_repository,
        blob_repository,
        heartbeat_timeout_seconds=-60,
        max_attempts=2,
    )
    sessions = [await create_session(session_repository, agent.id) for _ in range(2)]
    run, _ = await seed_run(
        session_repository,
        cohort_repository,
        config_repository,
        experiment_repository,
        run_repository,
        repository,
        agent,
        version,
        sessions,
    )
    first = await stale_service.claim_jobs(
        worker_id=worker.id,
        max_jobs=5,
        scope=WorkerScope(experiment_run_id=run.id),
        actor=ACTOR,
    )
    assert len(first) == 2
    second = await stale_service.claim_jobs(
        worker_id=other_worker.id,
        max_jobs=5,
        scope=WorkerScope(experiment_run_id=run.id),
        actor=ACTOR,
    )
    assert len(second) == 2
    for job, _ in second:
        assert job.worker_id == other_worker.id
        assert job.attempt == 2

    assert (
        await stale_service.claim_jobs(
            worker_id=worker.id,
            max_jobs=5,
            scope=WorkerScope(experiment_run_id=run.id),
            actor=ACTOR,
        )
        == []
    )
    jobs, _ = await repository.query(JobFilter(experiment_run_id=run.id))
    assert all(job.status is JobStatus.TIMED_OUT for job in jobs)
    finalized = await run_repository.get(run.id)
    assert finalized.status is ExperimentRunStatus.FAILED


async def test_claim_jobs_unfiltered_yields_pool_work_only(
    service: JobService,
    repository: FakeJobRepository,
    session_repository: FakeSessionRepository,
    cohort_repository: FakeCohortRepository,
    config_repository: FakeReplayConfigRepository,
    experiment_repository: FakeExperimentRepository,
    run_repository: FakeExperimentRunRepository,
    version_repository: FakeAgentVersionRepository,
    worker: Worker,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Claim pool-target jobs, including run-created ones, and nothing else."""
    image_version = await version_repository.create(
        AgentVersion(
            owner_id=ACTOR.account.id,
            agent_id=agent.id,
            version="v2",
            run_spec=RunSpec(
                command="python agent.py",
                timeout_seconds=600,
                image="agent:v2",
            ),
        )
    )
    pool_job = await service.create_session_run(
        session_run_create(agent_version_id=version.id), actor=ACTOR
    )
    on_demand_job = await service.create_session_run(
        session_run_create(
            agent_version_id=image_version.id,
            execution_target=ExecutionTarget.ON_DEMAND,
        ),
        actor=ACTOR,
    )
    session = await create_session(session_repository, agent.id)
    _, run_jobs = await seed_run(
        session_repository,
        cohort_repository,
        config_repository,
        experiment_repository,
        run_repository,
        repository,
        agent,
        version,
        [session],
    )
    claimed = await service.claim_jobs(
        worker_id=worker.id,
        max_jobs=10,
        scope=WorkerScope(),
        actor=ACTOR,
    )
    claimed_ids = {job.id for job, _ in claimed}
    assert claimed_ids == {pool_job.id, run_jobs[0].id}
    assert on_demand_job.id not in claimed_ids


async def test_claim_jobs_agent_scope(
    service: JobService,
    agent_repository: FakeAgentRepository,
    version_repository: FakeAgentVersionRepository,
    worker: Worker,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Claim only jobs whose agent version belongs to the scoped agents."""
    other = await agent_repository.create(
        Agent(owner_id=ACTOR.account.id, name="triage-bot")
    )
    other_version = await version_repository.create(
        AgentVersion(
            owner_id=ACTOR.account.id,
            agent_id=other.id,
            version="v1",
            run_spec=RunSpec(command="python agent.py", timeout_seconds=600),
        )
    )
    mine = await service.create_session_run(
        session_run_create(agent_id=agent.id), actor=ACTOR
    )
    await service.create_session_run(
        session_run_create(agent_version_id=other_version.id), actor=ACTOR
    )
    claimed = await service.claim_jobs(
        worker_id=worker.id,
        max_jobs=10,
        scope=WorkerScope(agent_version_ids=[version.id]),
        actor=ACTOR,
    )
    assert {job.id for job, _ in claimed} == {mine.id}


async def test_list_jobs_kind_and_target_filters(
    service: JobService,
    replay_service: ReplayService,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """List jobs filtered by kind and execution target."""
    session = await create_session(session_repository, agent.id)
    replay_job, _ = await create_replay(replay_service, replay_create(session.id))
    session_run_job = await service.create_session_run(
        session_run_create(agent_id=agent.id), actor=ACTOR
    )
    jobs, total = await service.list_jobs(
        JobFilter(kind=JobKind.SESSION_RUN), actor=ACTOR
    )
    assert total == 1
    assert jobs[0].id == session_run_job.id

    jobs, total = await service.list_jobs(JobFilter(kind=JobKind.REPLAY), actor=ACTOR)
    assert total == 1
    assert jobs[0].id == replay_job.id

    _, total = await service.list_jobs(
        JobFilter(execution_target=ExecutionTarget.POOL), actor=ACTOR
    )
    assert total == 2
    _, total = await service.list_jobs(
        JobFilter(execution_target=ExecutionTarget.ON_DEMAND), actor=ACTOR
    )
    assert total == 0


async def register_scorer(
    plugin_repository: FakePluginRepository,
    blob_repository: FakeBlobRepository,
    name: str,
    versions: int = 1,
) -> Plugin:
    """Register a scorer plugin with code versions.

    Args:
        plugin_repository: Fake plugin repository.
        blob_repository: Fake blob repository.
        name: Scorer name.
        versions: Number of versions to register.

    Returns:
        Stored plugin with the latest version counter set.
    """
    plugin = await plugin_repository.create(
        Plugin(owner_id=ACTOR.account.id, kind=PluginKind.SCORER, name=name)
    )
    for index in range(versions):
        blob = await blob_repository.create(
            Blob(
                owner_id=ACTOR.account.id,
                sha256=f"{name}{index}".ljust(64, "0"),
                size=4,
                media_type="text/x-python",
                data=b"code",
            )
        )
        await plugin_repository.create_version(
            PluginVersion(
                plugin_id=plugin.id,
                format=PluginFormat.INLINE,
                blob_id=blob.id,
                entrypoint="score",
            )
        )
    return await plugin_repository.get(plugin.id)


def registry_policy(
    name: str = "relevance", version: int | None = None
) -> ScoringPolicy:
    """Build a scoring policy with one registry scorer.

    Args:
        name: Scorer name.
        version: Registered version to run.

    Returns:
        Scoring policy.
    """
    return ScoringPolicy(
        scorers=[RegistryScorerConfig(name=name, version=version)],
        pass_threshold=0.5,
    )


async def complete_replay(
    service: JobService,
    repository: FakeJobRepository,
    session_repository: FakeSessionRepository,
    job_id: uuid.UUID,
    agent_id: uuid.UUID,
) -> Session:
    """Run a replay job to completion, fanning out its score jobs.

    Args:
        service: Job service.
        repository: Fake job repository.
        session_repository: Fake session repository.
        job_id: Id of the replay job.
        agent_id: Id of the agent.

    Returns:
        Linked result session.
    """
    await service.update_job(job_id, JobUpdate(status=JobStatus.RUNNING), actor=ACTOR)
    result = await link_result_session(repository, session_repository, job_id, agent_id)
    await service.update_job(job_id, JobUpdate(status=JobStatus.COMPLETED), actor=ACTOR)
    return result


def test_scoring_policy_evaluates_weighted_average() -> None:
    """Weigh the scores and report the pass verdict."""
    policy = ScoringPolicy(
        scorers=[
            SourceScorerConfig(
                name="a", source=SourceRef(module="m", attribute="a"), weight=3
            ),
            SourceScorerConfig(
                name="b", source=SourceRef(module="m", attribute="b"), weight=1
            ),
        ],
        pass_threshold=0.5,
    )
    result = policy.evaluate({"a": 1.0, "b": 0.0})
    assert result.score == pytest.approx(0.75)
    assert result.passed is True
    assert result.scores == {"a": 1.0, "b": 0.0}


def test_scoring_policy_hard_fails_below_threshold() -> None:
    """Fail outright when a scorer lands at or below its floor."""
    policy = ScoringPolicy(
        scorers=[
            SourceScorerConfig(
                name="a", source=SourceRef(module="m", attribute="a"), fail_below=0.5
            )
        ],
        pass_threshold=0.1,
    )
    assert policy.evaluate({"a": 0.5}).passed is False
    assert policy.evaluate({"a": 0.6}).passed is True


def test_scoring_policy_rejects_zero_weight_and_missing_scores() -> None:
    """Reject evaluating without weight or without every score."""
    policy = ScoringPolicy(
        scorers=[
            SourceScorerConfig(
                name="a", source=SourceRef(module="m", attribute="a"), weight=0
            )
        ],
        pass_threshold=0.5,
    )
    with pytest.raises(
        InvalidReplayConfig, match="Scoring policy has a total scorer weight of 0"
    ):
        policy.evaluate({"a": 1.0})
    with pytest.raises(InvalidReplayConfig, match=r"Scorers \['a'\] have no score"):
        policy.evaluate({})


def test_replay_completion_is_never_stale() -> None:
    """Leave a completed replay job untouched by the staleness rule."""
    job = run_job()
    job.claim(WORKER_ID)
    job.start()
    job.link_result_session(uuid.uuid4())
    job.complete(None)
    stale = datetime.now(UTC) + timedelta(seconds=60)
    assert job.is_stale(stale) is False
    assert job.with_staleness(stale, 3) is job


def score_job(**overrides: object) -> Score:
    """Build a score job entity.

    Args:
        **overrides: Field overrides.

    Returns:
        Score entity.
    """
    values: dict[str, object] = {
        "parent_job_id": uuid.uuid4(),
        "input_session_id": uuid.uuid4(),
        "plugin_version_id": uuid.uuid4(),
        "scorer_config": RegistryScorerConfig(name="relevance", version=1),
        "execution_target": ExecutionTarget.POOL,
        **overrides,
    }
    return Score.model_validate(values)


def test_score_completes_with_a_score_result() -> None:
    """Complete a running score job with a score value."""
    job = score_job()
    job.claim(WORKER_ID)
    job.start()
    with pytest.raises(JobMissingScore, match=f"Job {job.id} has no score"):
        job.complete(None)
    with pytest.raises(JobMissingScore):
        job.complete(1.5)
    with pytest.raises(JobMissingScore):
        job.complete(True)
    job.complete(0.7)
    assert job.status is JobStatus.COMPLETED
    assert job.score == 0.7
    assert job.result == 0.7
    assert job.ended_at is not None


def test_score_arm_invariants() -> None:
    """Bind a score job to exactly one code reference."""
    with pytest.raises(InvalidJob, match="Registry scorers carry no agent version"):
        score_job(agent_version_id=uuid.uuid4())
    with pytest.raises(InvalidJob, match="Registry scorers require a plugin version"):
        score_job(plugin_version_id=None)
    with pytest.raises(InvalidJob, match="Source scorers require an agent version"):
        score_job(
            plugin_version_id=None,
            scorer_config=SourceScorerConfig(
                name="conciseness", source=SourceRef(module="m", attribute="a")
            ),
        )
    with pytest.raises(InvalidJob, match="Source scorers carry no plugin version"):
        score_job(
            agent_version_id=uuid.uuid4(),
            scorer_config=SourceScorerConfig(
                name="conciseness", source=SourceRef(module="m", attribute="a")
            ),
        )


def test_score_retry_clears_the_score() -> None:
    """Drop the recorded score when a score job goes back to pending."""
    job = score_job()
    job.claim(WORKER_ID)
    job.start()
    job.fail("scorer crashed")
    job.retry()
    assert job.status is JobStatus.PENDING
    assert job.score is None


async def test_fan_out_creates_one_child_per_scorer(
    service: JobService,
    replay_service: ReplayService,
    repository: FakeJobRepository,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Fan a scoring replay out to a result and a baseline score job."""
    session = await create_session(session_repository, agent.id)
    job, _ = await create_replay(replay_service, replay_create(session.id))
    result = await complete_replay(
        service, repository, session_repository, job.id, agent.id
    )
    children = await repository.list_children(job.id)
    assert len(children) == 2
    for child in children:
        assert isinstance(child, Score)
        assert child.status is JobStatus.PENDING
        assert child.scorer_config.name == "conciseness"
        assert child.agent_version_id == version.id
        assert child.plugin_version_id is None
        assert child.execution_target is ExecutionTarget.POOL
    assert {
        child.input_session_id for child in children if isinstance(child, Score)
    } == {result.id, session.id}


async def test_fan_out_skips_baselines_with_stored_scores(
    service: JobService,
    replay_service: ReplayService,
    repository: FakeJobRepository,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Skip the baseline score job for a scorer the session already has."""
    session = await create_session(session_repository, agent.id)
    session.merge_scores({"conciseness": 0.4})
    await session_repository.update(session)
    job, _ = await create_replay(replay_service, replay_create(session.id))
    result = await complete_replay(
        service, repository, session_repository, job.id, agent.id
    )
    children = await repository.list_children(job.id)
    assert [
        child.input_session_id for child in children if isinstance(child, Score)
    ] == [result.id]


async def test_fan_out_skips_baselines_when_the_run_opts_out(
    service: JobService,
    repository: FakeJobRepository,
    session_repository: FakeSessionRepository,
    cohort_repository: FakeCohortRepository,
    config_repository: FakeReplayConfigRepository,
    experiment_repository: FakeExperimentRepository,
    run_repository: FakeExperimentRunRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Skip baseline score jobs for a run created without baselines."""
    session = await create_session(session_repository, agent.id)
    _, jobs = await seed_run(
        session_repository,
        cohort_repository,
        config_repository,
        experiment_repository,
        run_repository,
        repository,
        agent,
        version,
        [session],
        score_baselines=False,
    )
    await repository.claim_pending(WORKER_ID, 1, WorkerScope())
    result = await complete_replay(
        service, repository, session_repository, jobs[0].id, agent.id
    )
    children = await repository.list_children(jobs[0].id)
    assert [
        child.input_session_id for child in children if isinstance(child, Score)
    ] == [result.id]


async def test_fan_out_pins_the_latest_registry_version(
    service: JobService,
    replay_service: ReplayService,
    repository: FakeJobRepository,
    session_repository: FakeSessionRepository,
    plugin_repository: FakePluginRepository,
    blob_repository: FakeBlobRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Resolve an unpinned registry scorer to its latest version."""
    plugin = await register_scorer(
        plugin_repository, blob_repository, "relevance", versions=2
    )
    latest = await plugin_repository.get_version(plugin.id, plugin.latest_version)
    session = await create_session(session_repository, agent.id)
    job, _ = await create_replay(
        replay_service, replay_create(session.id, scoring_policy=registry_policy())
    )
    await complete_replay(service, repository, session_repository, job.id, agent.id)
    children = await repository.list_children(job.id)
    for child in children:
        assert isinstance(child, Score)
        assert child.plugin_version_id == latest.id
        assert child.agent_version_id is None
        assert isinstance(child.scorer_config, RegistryScorerConfig)
        assert child.scorer_config.version == 2


async def test_fan_out_is_idempotent(
    service: JobService,
    replay_service: ReplayService,
    repository: FakeJobRepository,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Reject a second score job for the same parent, session, and scorer."""
    session = await create_session(session_repository, agent.id)
    job, _ = await create_replay(replay_service, replay_create(session.id))
    result = await complete_replay(
        service, repository, session_repository, job.id, agent.id
    )
    existing = (await repository.list_children(job.id))[0]
    assert isinstance(existing, Score)
    with pytest.raises(
        DuplicateScoreJob,
        match=(
            f"Session {existing.input_session_id} is already scored by "
            f"'conciseness' for job {job.id}"
        ),
    ):
        await repository.create_many(
            [
                Score(
                    parent_job_id=job.id,
                    input_session_id=existing.input_session_id,
                    agent_version_id=version.id,
                    scorer_config=existing.scorer_config,
                    execution_target=ExecutionTarget.POOL,
                )
            ]
        )
    assert len(await repository.list_children(job.id)) == 2
    assert result.id in {
        child.input_session_id
        for child in await repository.list_children(job.id)
        if isinstance(child, Score)
    }


async def test_aggregation_completes_the_replay(
    replay_repository: FakeReplayRepository,
    service: JobService,
    replay_service: ReplayService,
    repository: FakeJobRepository,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Merge the scores, evaluate the policy, and store the diff."""
    session = await create_session(session_repository, agent.id)
    job, _ = await create_replay(replay_service, replay_create(session.id))
    result = await complete_replay(
        service, repository, session_repository, job.id, agent.id
    )
    for child in await repository.list_children(job.id):
        assert isinstance(child, Score)
        await service.update_job(
            child.id, JobUpdate(status=JobStatus.RUNNING), actor=ACTOR
        )
        score = 0.8 if child.input_session_id == result.id else 0.4
        await service.update_job(
            child.id,
            JobUpdate(status=JobStatus.COMPLETED, result=score),
            actor=ACTOR,
        )
    settled = await replay_repository.get_by_job(job.id)
    assert (await repository.get(job.id)).status is JobStatus.COMPLETED
    assert settled.passed is True
    assert settled.score == pytest.approx(0.8)
    assert settled.scores == {"conciseness": 0.8}
    assert (await session_repository.get(session.id)).scores == {"conciseness": 0.4}
    assert (await session_repository.get(result.id)).scores == {"conciseness": 0.8}
    assert settled.diff is not None
    assert settled.diff["score_deltas"]["conciseness"] == pytest.approx(0.4)


async def test_aggregation_fails_the_replay_and_cancels_siblings(
    replay_repository: FakeReplayRepository,
    service: JobService,
    replay_service: ReplayService,
    repository: FakeJobRepository,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Fail the replay and cancel the remaining score jobs."""
    session = await create_session(session_repository, agent.id)
    job, _ = await create_replay(replay_service, replay_create(session.id))
    await complete_replay(service, repository, session_repository, job.id, agent.id)
    first, second = await repository.list_children(job.id)
    await service.update_job(first.id, JobUpdate(status=JobStatus.RUNNING), actor=ACTOR)
    await service.update_job(
        first.id,
        JobUpdate(status=JobStatus.FAILED, error="scorer crashed"),
        actor=ACTOR,
    )
    settled = await replay_repository.get_by_job(job.id)
    assert settled.error == "Scorer 'conciseness' did not complete"
    assert settled.passed is None
    # The replay job itself stays completed, its process did succeed.
    assert (await repository.get(job.id)).status is JobStatus.COMPLETED
    assert (await repository.get(second.id)).status is JobStatus.CANCELED


async def test_aggregation_fails_the_replay_on_a_timed_out_child(
    replay_repository: FakeReplayRepository,
    replay_service: ReplayService,
    repository: FakeJobRepository,
    config_repository: FakeReplayConfigRepository,
    session_repository: FakeSessionRepository,
    agent_repository: FakeAgentRepository,
    version_repository: FakeAgentVersionRepository,
    node_repository: FakeSessionNodeRepository,
    run_repository: FakeExperimentRunRepository,
    experiment_repository: FakeExperimentRepository,
    cohort_repository: FakeCohortRepository,
    secret_repository: FakeSecretRepository,
    worker_repository: FakeWorkerRepository,
    plugin_repository: FakePluginRepository,
    blob_repository: FakeBlobRepository,
    worker: Worker,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Fail the replay when a score job exhausts its attempts."""
    service = build_service(
        repository,
        replay_repository,
        config_repository,
        session_repository,
        agent_repository,
        version_repository,
        node_repository,
        run_repository,
        experiment_repository,
        cohort_repository,
        secret_repository,
        worker_repository,
        plugin_repository,
        blob_repository,
    )
    session = await create_session(session_repository, agent.id)
    job, _ = await create_replay(replay_service, replay_create(session.id))
    await complete_replay(service, repository, session_repository, job.id, agent.id)
    for child in await repository.list_children(job.id):
        claimed = await repository.get(child.id)
        claimed.attempt = 3
        claimed.claim(WORKER_ID)
        await repository.update(claimed)
    stale_service = build_service(
        repository,
        replay_repository,
        config_repository,
        session_repository,
        agent_repository,
        version_repository,
        node_repository,
        run_repository,
        experiment_repository,
        cohort_repository,
        secret_repository,
        worker_repository,
        plugin_repository,
        blob_repository,
        heartbeat_timeout_seconds=-60,
    )
    await stale_service.claim_jobs(
        worker_id=worker.id,
        max_jobs=5,
        scope=WorkerScope(),
        actor=ACTOR,
    )
    settled = await replay_repository.get_by_job(job.id)
    assert settled.error == "Scorer 'conciseness' did not complete"
    assert (await repository.get(job.id)).status is JobStatus.COMPLETED


async def test_update_score_job_requires_a_score_result(
    service: JobService,
    replay_service: ReplayService,
    repository: FakeJobRepository,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Reject completing a score job without a numeric result in 0..1."""
    session = await create_session(session_repository, agent.id)
    job, _ = await create_replay(replay_service, replay_create(session.id))
    await complete_replay(service, repository, session_repository, job.id, agent.id)
    child = (await repository.list_children(job.id))[0]
    await service.update_job(child.id, JobUpdate(status=JobStatus.RUNNING), actor=ACTOR)
    with pytest.raises(JobMissingScore, match=f"Job {child.id} has no score"):
        await service.update_job(
            child.id, JobUpdate(status=JobStatus.COMPLETED), actor=ACTOR
        )
    with pytest.raises(JobMissingScore):
        await service.update_job(
            child.id, JobUpdate(status=JobStatus.COMPLETED, result=1.5), actor=ACTOR
        )
    finished = await service.update_job(
        child.id, JobUpdate(status=JobStatus.COMPLETED, result=0.6), actor=ACTOR
    )
    assert isinstance(finished, Score)
    assert finished.status is JobStatus.COMPLETED
    assert finished.score == 0.6


async def test_update_job_requires_a_status(
    service: JobService,
    replay_service: ReplayService,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Reject an empty job update."""
    session = await create_session(session_repository, agent.id)
    job, _ = await create_replay(replay_service, replay_create(session.id))
    with pytest.raises(InvalidJob, match="Updating a job requires a status"):
        await service.update_job(job.id, JobUpdate(), actor=ACTOR)


async def test_cancel_replay_job_cancels_children(
    service: JobService,
    replay_service: ReplayService,
    repository: FakeJobRepository,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Cascade a replay job cancellation to its score jobs."""
    session = await create_session(session_repository, agent.id)
    job, _ = await create_replay(replay_service, replay_create(session.id))
    await complete_replay(service, repository, session_repository, job.id, agent.id)
    child = (await repository.list_children(job.id))[0]
    await service.update_job(child.id, JobUpdate(status=JobStatus.RUNNING), actor=ACTOR)
    await service.update_job(
        child.id, JobUpdate(status=JobStatus.CANCELED), actor=ACTOR
    )
    children = await repository.list_children(job.id)
    assert children
    assert all(entry.status is JobStatus.CANCELED for entry in children)


async def test_retry_replay_drops_its_children(
    service: JobService,
    replay_service: ReplayService,
    repository: FakeJobRepository,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Clear the score jobs of a retried replay job."""
    session = await create_session(session_repository, agent.id)
    job, _ = await create_replay(replay_service, replay_create(session.id))
    await service.update_job(job.id, JobUpdate(status=JobStatus.RUNNING), actor=ACTOR)
    await link_result_session(repository, session_repository, job.id, agent.id)
    await service.update_job(
        job.id,
        JobUpdate(status=JobStatus.FAILED, error="agent exited with code 1"),
        actor=ACTOR,
    )
    await repository.create_many(
        [
            Score(
                parent_job_id=job.id,
                input_session_id=session.id,
                agent_version_id=version.id,
                scorer_config=SCORING_POLICY.scorers[0],
                execution_target=ExecutionTarget.POOL,
            )
        ]
    )
    await service.retry_job(job.id, actor=ACTOR)
    assert await repository.list_children(job.id) == []


async def test_claim_jobs_parent_scope(
    service: JobService,
    replay_service: ReplayService,
    repository: FakeJobRepository,
    session_repository: FakeSessionRepository,
    worker: Worker,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Claim the score jobs of one replay through the parent scope."""
    session = await create_session(session_repository, agent.id)
    job, _ = await create_replay(replay_service, replay_create(session.id))
    await complete_replay(service, repository, session_repository, job.id, agent.id)
    other = await create_session(session_repository, agent.id)
    other_job, _ = await create_replay(replay_service, replay_create(other.id))
    await complete_replay(
        service, repository, session_repository, other_job.id, agent.id
    )
    claimed = await service.claim_jobs(
        worker_id=worker.id,
        max_jobs=10,
        scope=WorkerScope(job_id=job.id),
        actor=ACTOR,
    )
    children = {child.id for child in await repository.list_children(job.id)}
    assert {claimed_job.id for claimed_job, _ in claimed} == children


async def test_claim_jobs_run_scope_picks_up_children(
    service: JobService,
    repository: FakeJobRepository,
    session_repository: FakeSessionRepository,
    cohort_repository: FakeCohortRepository,
    config_repository: FakeReplayConfigRepository,
    experiment_repository: FakeExperimentRepository,
    run_repository: FakeExperimentRunRepository,
    worker: Worker,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Claim the score jobs of a run's replay under the run scope."""
    session = await create_session(session_repository, agent.id)
    run, jobs = await seed_run(
        session_repository,
        cohort_repository,
        config_repository,
        experiment_repository,
        run_repository,
        repository,
        agent,
        version,
        [session],
    )
    await repository.claim_pending(WORKER_ID, 1, WorkerScope(experiment_run_id=run.id))
    await complete_replay(service, repository, session_repository, jobs[0].id, agent.id)
    claimed = await service.claim_jobs(
        worker_id=worker.id,
        max_jobs=10,
        scope=WorkerScope(experiment_run_id=run.id),
        actor=ACTOR,
    )
    children = {child.id for child in await repository.list_children(jobs[0].id)}
    assert {claimed_job.id for claimed_job, _ in claimed} == children


async def test_claim_jobs_agent_scope_matches_registry_score_jobs(
    service: JobService,
    replay_service: ReplayService,
    repository: FakeJobRepository,
    session_repository: FakeSessionRepository,
    plugin_repository: FakePluginRepository,
    blob_repository: FakeBlobRepository,
    agent_repository: FakeAgentRepository,
    version_repository: FakeAgentVersionRepository,
    worker: Worker,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Match registry score jobs and skip score jobs of other agents."""
    await register_scorer(plugin_repository, blob_repository, "relevance")
    session = await create_session(session_repository, agent.id)
    registry_job, _ = await create_replay(
        replay_service, replay_create(session.id, scoring_policy=registry_policy())
    )
    await complete_replay(
        service, repository, session_repository, registry_job.id, agent.id
    )
    other_agent = await agent_repository.create(
        Agent(owner_id=ACTOR.account.id, name="other-bot")
    )
    other_version = await version_repository.create(
        AgentVersion(
            owner_id=ACTOR.account.id,
            agent_id=other_agent.id,
            version="v1",
            run_spec=RunSpec(command="python agent.py", timeout_seconds=600),
        )
    )
    other_session = await create_session(session_repository, other_agent.id)
    other_job, _ = await create_replay(replay_service, replay_create(other_session.id))
    await complete_replay(
        service, repository, session_repository, other_job.id, other_agent.id
    )
    claimed = await service.claim_jobs(
        worker_id=worker.id,
        max_jobs=10,
        scope=WorkerScope(agent_version_ids=[version.id]),
        actor=ACTOR,
    )
    claimed_ids = {claimed_job.id for claimed_job, _ in claimed}
    assert claimed_ids == {
        child.id for child in await repository.list_children(registry_job.id)
    }
    assert other_version.id not in {
        claimed_job.agent_version_id for claimed_job, _ in claimed
    }


async def test_get_spec_registry_score_job(
    service: JobService,
    replay_service: ReplayService,
    repository: FakeJobRepository,
    session_repository: FakeSessionRepository,
    plugin_repository: FakePluginRepository,
    blob_repository: FakeBlobRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Resolve the plugin code of a registry score job."""
    plugin = await register_scorer(plugin_repository, blob_repository, "relevance")
    plugin_version = await plugin_repository.get_version(plugin.id, 1)
    blob = await blob_repository.get(plugin_version.blob_id)
    session = await create_session(session_repository, agent.id)
    job, _ = await create_replay(
        replay_service, replay_create(session.id, scoring_policy=registry_policy())
    )
    result = await complete_replay(
        service, repository, session_repository, job.id, agent.id
    )
    child = next(
        candidate
        for candidate in await repository.list_children(job.id)
        if isinstance(candidate, Score) and candidate.input_session_id == result.id
    )
    spec = await service.get_spec(child.id, actor=ACTOR)
    assert spec.kind is JobKind.SCORE
    assert spec.run_spec is None
    assert spec.secret_env == {}
    assert spec.input_session_id == result.id
    assert spec.scorer is not None
    assert spec.scorer.input_session_id == result.id
    assert spec.scorer.config == child.scorer_config
    assert spec.scorer.plugin is not None
    assert spec.scorer.plugin.format is PluginFormat.INLINE
    assert spec.scorer.plugin.entrypoint == "score"
    assert spec.scorer.plugin.blob_id == blob.id
    assert spec.scorer.plugin.sha256 == blob.sha256


async def test_get_spec_source_score_job(
    service: JobService,
    replay_service: ReplayService,
    repository: FakeJobRepository,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Resolve the agent run environment of a source score job."""
    session = await create_session(session_repository, agent.id)
    job, _ = await create_replay(replay_service, replay_create(session.id))
    result = await complete_replay(
        service, repository, session_repository, job.id, agent.id
    )
    child = next(
        candidate
        for candidate in await repository.list_children(job.id)
        if isinstance(candidate, Score) and candidate.input_session_id == result.id
    )
    spec = await service.get_spec(child.id, actor=ACTOR)
    assert spec.kind is JobKind.SCORE
    assert spec.run_spec == version.run_spec
    assert spec.scorer is not None
    assert spec.scorer.plugin is None
    assert spec.scorer.config == child.scorer_config
    assert spec.scorer.input_session_id == result.id


async def register_importer(
    plugin_repository: FakePluginRepository,
    blob_repository: FakeBlobRepository,
    name: str,
    versions: int = 1,
    provider: str | None = "langfuse",
) -> Plugin:
    """Register an importer plugin with code versions.

    Args:
        plugin_repository: Fake plugin repository.
        blob_repository: Fake blob repository.
        name: Importer name.
        versions: Number of versions to register.
        provider: Provider the importer reads from.

    Returns:
        Stored plugin with the latest version counter set.
    """
    plugin = await plugin_repository.create(
        Plugin(
            owner_id=ACTOR.account.id,
            kind=PluginKind.IMPORTER,
            name=name,
            provider=provider,
        )
    )
    for index in range(versions):
        blob = await blob_repository.create(
            Blob(
                owner_id=ACTOR.account.id,
                sha256=f"importer{name}{index}".ljust(64, "0"),
                size=4,
                media_type="text/x-python",
                data=b"code",
            )
        )
        await plugin_repository.create_version(
            PluginVersion(
                plugin_id=plugin.id,
                format=PluginFormat.INLINE,
                blob_id=blob.id,
                entrypoint="parse",
            )
        )
    return await plugin_repository.get(plugin.id)


async def create_payload(blob_repository: FakeBlobRepository) -> Blob:
    """Store a payload blob for import tests.

    Args:
        blob_repository: Fake blob repository.

    Returns:
        Stored blob.
    """
    return await blob_repository.create(
        Blob(
            owner_id=ACTOR.account.id,
            sha256="payload".ljust(64, "0"),
            size=7,
            media_type="application/jsonl",
            data=b"payload",
        )
    )


def test_import_rejects_agent_version() -> None:
    """Reject an import bound to an agent version."""
    with pytest.raises(InvalidJob, match="Imports carry no agent version"):
        Import(
            plugin_version_id=uuid.uuid4(),
            payload_blob_id=uuid.uuid4(),
            agent_id=uuid.uuid4(),
            agent_version_id=uuid.uuid4(),
            execution_target=ExecutionTarget.POOL,
        )


def test_import_completes_with_a_result() -> None:
    """Complete a running import with its stats result."""
    job = Import(
        plugin_version_id=uuid.uuid4(),
        payload_blob_id=uuid.uuid4(),
        agent_id=uuid.uuid4(),
        execution_target=ExecutionTarget.POOL,
    )
    job.claim(WORKER_ID)
    job.start()
    with pytest.raises(JobMissingResult, match=f"Job {job.id} has no result"):
        job.complete(None)
    job.complete({"created": 2, "skipped": 1, "failed": 0})
    assert job.status is JobStatus.COMPLETED
    assert job.result == {"created": 2, "skipped": 1, "failed": 0}
    assert job.ended_at is not None


def test_import_retry_clears_the_result() -> None:
    """Clear the recorded result when an import is retried."""
    job = Import(
        plugin_version_id=uuid.uuid4(),
        payload_blob_id=uuid.uuid4(),
        agent_id=uuid.uuid4(),
        execution_target=ExecutionTarget.POOL,
    )
    job.claim(WORKER_ID)
    job.start()
    job.complete({"created": 1, "skipped": 0, "failed": 0})
    job.status = JobStatus.FAILED
    job.retry()
    assert job.result is None
    assert job.status is JobStatus.PENDING
    assert job.attempt == 2


async def test_create_import_pins_latest_version(
    service: JobService,
    plugin_repository: FakePluginRepository,
    blob_repository: FakeBlobRepository,
    agent: Agent,
) -> None:
    """Pin the latest importer version and store the params as inputs."""
    plugin = await register_importer(
        plugin_repository, blob_repository, "langfuse", versions=2
    )
    payload = await create_payload(blob_repository)
    latest = await plugin_repository.get_version(plugin.id, 2)
    job = await service.create_import(
        ImportCreate(
            importer="langfuse",
            agent_id=agent.id,
            payload_blob_id=payload.id,
            params={"project": "demo"},
        ),
        actor=ACTOR,
    )
    assert job.kind is JobKind.IMPORT
    assert job.plugin_version_id == latest.id
    assert job.payload_blob_id == payload.id
    assert job.inputs == {"project": "demo"}
    assert job.status is JobStatus.PENDING
    assert job.execution_target is ExecutionTarget.POOL
    assert job.agent_version_id is None


async def test_create_import_pins_explicit_version(
    service: JobService,
    plugin_repository: FakePluginRepository,
    blob_repository: FakeBlobRepository,
    agent: Agent,
) -> None:
    """Pin the requested importer version."""
    plugin = await register_importer(
        plugin_repository, blob_repository, "langfuse", versions=2
    )
    payload = await create_payload(blob_repository)
    first = await plugin_repository.get_version(plugin.id, 1)
    job = await service.create_import(
        ImportCreate(
            importer="langfuse",
            agent_id=agent.id,
            version=1,
            payload_blob_id=payload.id,
        ),
        actor=ACTOR,
    )
    assert job.plugin_version_id == first.id
    assert job.inputs == {}


async def test_create_import_unknown_importer(
    service: JobService, blob_repository: FakeBlobRepository, agent: Agent
) -> None:
    """Reject an import naming an unregistered importer."""
    payload = await create_payload(blob_repository)
    with pytest.raises(
        PluginNameNotFound, match="Plugin 'langfuse' of kind 'importer' was not found"
    ):
        await service.create_import(
            ImportCreate(
                importer="langfuse", agent_id=agent.id, payload_blob_id=payload.id
            ),
            actor=ACTOR,
        )


async def test_create_import_unknown_version(
    service: JobService,
    plugin_repository: FakePluginRepository,
    blob_repository: FakeBlobRepository,
    agent: Agent,
) -> None:
    """Reject an import naming a version the importer does not have."""
    plugin = await register_importer(plugin_repository, blob_repository, "langfuse")
    payload = await create_payload(blob_repository)
    with pytest.raises(
        PluginVersionNotFound, match=f"Plugin {plugin.id} has no version 4"
    ):
        await service.create_import(
            ImportCreate(
                importer="langfuse",
                agent_id=agent.id,
                version=4,
                payload_blob_id=payload.id,
            ),
            actor=ACTOR,
        )


async def test_create_import_unknown_payload(
    service: JobService,
    plugin_repository: FakePluginRepository,
    blob_repository: FakeBlobRepository,
    agent: Agent,
) -> None:
    """Reject an import referencing a payload blob that does not exist."""
    await register_importer(plugin_repository, blob_repository, "langfuse")
    missing = uuid.uuid4()
    with pytest.raises(BlobNotFound, match=f"Blob {missing} was not found"):
        await service.create_import(
            ImportCreate(
                importer="langfuse", agent_id=agent.id, payload_blob_id=missing
            ),
            actor=ACTOR,
        )


async def test_create_import_unknown_agent(
    service: JobService,
    plugin_repository: FakePluginRepository,
    blob_repository: FakeBlobRepository,
) -> None:
    """Reject an import naming an agent that does not exist."""
    await register_importer(plugin_repository, blob_repository, "langfuse")
    payload = await create_payload(blob_repository)
    missing = uuid.uuid4()
    with pytest.raises(AgentNotFound, match=f"Agent {missing} was not found"):
        await service.create_import(
            ImportCreate(
                importer="langfuse", agent_id=missing, payload_blob_id=payload.id
            ),
            actor=ACTOR,
        )


async def test_create_import_importer_without_provider(
    service: JobService,
    plugin_repository: FakePluginRepository,
    blob_repository: FakeBlobRepository,
    agent: Agent,
) -> None:
    """Reject an import whose importer carries no provider."""
    await register_importer(
        plugin_repository, blob_repository, "langfuse", provider=None
    )
    payload = await create_payload(blob_repository)
    with pytest.raises(InvalidJob, match="Importer 'langfuse' carries no provider"):
        await service.create_import(
            ImportCreate(
                importer="langfuse", agent_id=agent.id, payload_blob_id=payload.id
            ),
            actor=ACTOR,
        )


async def test_create_import_importer_with_unknown_provider(
    service: JobService,
    plugin_repository: FakePluginRepository,
    blob_repository: FakeBlobRepository,
    agent: Agent,
) -> None:
    """Reject an import whose importer reads from no session provider."""
    await register_importer(
        plugin_repository, blob_repository, "langfuse", provider="phoenix"
    )
    payload = await create_payload(blob_repository)
    with pytest.raises(
        InvalidJob, match="Importer 'langfuse' reads from unknown provider 'phoenix'"
    ):
        await service.create_import(
            ImportCreate(
                importer="langfuse", agent_id=agent.id, payload_blob_id=payload.id
            ),
            actor=ACTOR,
        )


async def test_get_spec_import_job(
    service: JobService,
    plugin_repository: FakePluginRepository,
    blob_repository: FakeBlobRepository,
    agent: Agent,
) -> None:
    """Resolve the importer code and payload of an import job."""
    plugin = await register_importer(plugin_repository, blob_repository, "langfuse")
    plugin_version = await plugin_repository.get_version(plugin.id, 1)
    code = await blob_repository.get(plugin_version.blob_id)
    payload = await create_payload(blob_repository)
    job = await service.create_import(
        ImportCreate(
            importer="langfuse",
            agent_id=agent.id,
            payload_blob_id=payload.id,
            params={"project": "demo"},
        ),
        actor=ACTOR,
    )
    spec = await service.get_spec(job.id, actor=ACTOR)
    assert spec.kind is JobKind.IMPORT
    assert spec.run_spec is None
    assert spec.secret_env == {}
    assert spec.scorer is None
    assert spec.importer is not None
    assert spec.importer.plugin.format is PluginFormat.INLINE
    assert spec.importer.plugin.entrypoint == "parse"
    assert spec.importer.plugin.blob_id == code.id
    assert spec.importer.plugin.sha256 == code.sha256
    assert spec.importer.payload.blob_id == payload.id
    assert spec.importer.payload.sha256 == payload.sha256
    assert spec.importer.provider is SessionProvider.LANGFUSE
    assert spec.importer.agent_id == agent.id
    assert spec.importer.params == {"project": "demo"}


async def test_update_import_completes_with_a_result(
    service: JobService,
    plugin_repository: FakePluginRepository,
    blob_repository: FakeBlobRepository,
    worker: Worker,
    agent: Agent,
) -> None:
    """Complete an import with its stats result."""
    await register_importer(plugin_repository, blob_repository, "langfuse")
    payload = await create_payload(blob_repository)
    job = await service.create_import(
        ImportCreate(
            importer="langfuse", agent_id=agent.id, payload_blob_id=payload.id
        ),
        actor=ACTOR,
    )
    await claim_one(service, worker, job.id)
    await service.update_job(job.id, JobUpdate(status=JobStatus.RUNNING), actor=ACTOR)
    stats = {
        "created": 3,
        "skipped": 1,
        "failed": 1,
        "failures": [{"line": 7, "external_id": "ext-7", "error": "bad line"}],
    }
    finished = await service.update_job(
        job.id, JobUpdate(status=JobStatus.COMPLETED, result=stats), actor=ACTOR
    )
    assert isinstance(finished, Import)
    assert finished.status is JobStatus.COMPLETED
    assert finished.result == stats


async def test_update_import_complete_requires_a_result(
    service: JobService,
    plugin_repository: FakePluginRepository,
    blob_repository: FakeBlobRepository,
    worker: Worker,
    agent: Agent,
) -> None:
    """Reject completing an import that reported no result."""
    await register_importer(plugin_repository, blob_repository, "langfuse")
    payload = await create_payload(blob_repository)
    job = await service.create_import(
        ImportCreate(
            importer="langfuse", agent_id=agent.id, payload_blob_id=payload.id
        ),
        actor=ACTOR,
    )
    await claim_one(service, worker, job.id)
    await service.update_job(job.id, JobUpdate(status=JobStatus.RUNNING), actor=ACTOR)
    with pytest.raises(JobMissingResult, match=f"Job {job.id} has no result"):
        await service.update_job(
            job.id, JobUpdate(status=JobStatus.COMPLETED), actor=ACTOR
        )


async def test_claim_jobs_matches_unbound_import_jobs(
    version: AgentVersion,
    service: JobService,
    plugin_repository: FakePluginRepository,
    blob_repository: FakeBlobRepository,
    worker: Worker,
    agent: Agent,
) -> None:
    """Claim pool imports whether or not the claim scopes to agents."""
    await register_importer(plugin_repository, blob_repository, "langfuse")
    payload = await create_payload(blob_repository)
    job = await service.create_import(
        ImportCreate(
            importer="langfuse", agent_id=agent.id, payload_blob_id=payload.id
        ),
        actor=ACTOR,
    )
    claimed = await service.claim_jobs(
        worker_id=worker.id,
        max_jobs=10,
        scope=WorkerScope(agent_version_ids=[version.id]),
        actor=ACTOR,
    )
    assert [claimed_job.id for claimed_job, _ in claimed] == [job.id]


async def test_claim_jobs_ship_specs_for_every_kind(
    service: JobService,
    replay_service: ReplayService,
    session_repository: FakeSessionRepository,
    plugin_repository: FakePluginRepository,
    blob_repository: FakeBlobRepository,
    worker: Worker,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Assemble one spec per claimed job across the kinds in a batch."""
    await register_importer(plugin_repository, blob_repository, "langfuse")
    payload = await create_payload(blob_repository)
    session = await create_session(session_repository, agent.id)
    replay, _ = await create_replay(replay_service, replay_create(session.id))
    session_run = await service.create_session_run(
        session_run_create(agent_id=agent.id), actor=ACTOR
    )
    import_job = await service.create_import(
        ImportCreate(
            importer="langfuse", agent_id=agent.id, payload_blob_id=payload.id
        ),
        actor=ACTOR,
    )

    claimed = await service.claim_jobs(
        worker_id=worker.id,
        max_jobs=10,
        scope=WorkerScope(),
        actor=ACTOR,
    )

    specs = {job.id: spec for job, spec in claimed}
    assert set(specs) == {replay.id, session_run.id, import_job.id}
    assert specs[replay.id].kind is JobKind.REPLAY
    assert specs[replay.id].input_session_id == session.id
    assert specs[replay.id].run_spec is not None
    assert specs[session_run.id].kind is JobKind.SESSION_RUN
    assert specs[session_run.id].run_spec is not None
    import_spec = specs[import_job.id]
    assert import_spec.kind is JobKind.IMPORT
    assert import_spec.importer is not None
    assert import_spec.importer.payload.blob_id == payload.id


async def test_claim_jobs_ship_score_specs_from_one_batch(
    service: JobService,
    replay_service: ReplayService,
    repository: FakeJobRepository,
    session_repository: FakeSessionRepository,
    plugin_repository: FakePluginRepository,
    blob_repository: FakeBlobRepository,
    worker: Worker,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Assemble the plugin code reference of every claimed score job."""
    await register_scorer(plugin_repository, blob_repository, "relevance")
    session = await create_session(session_repository, agent.id)
    job, _ = await create_replay(
        replay_service, replay_create(session.id, scoring_policy=registry_policy())
    )
    await complete_replay(service, repository, session_repository, job.id, agent.id)

    claimed = await service.claim_jobs(
        worker_id=worker.id,
        max_jobs=10,
        scope=WorkerScope(job_id=job.id),
        actor=ACTOR,
    )

    assert len(claimed) == 2
    for _, spec in claimed:
        assert spec.kind is JobKind.SCORE
        assert spec.scorer is not None
        assert spec.scorer.plugin is not None
        assert len(spec.scorer.plugin.sha256) == 64


async def test_claim_jobs_fail_jobs_whose_referents_are_gone(
    service: JobService,
    repository: FakeJobRepository,
    session_repository: FakeSessionRepository,
    version_repository: FakeAgentVersionRepository,
    worker: Worker,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Fail a claimed job with an unresolvable spec and keep the batch."""
    broken_version = await version_repository.create(
        AgentVersion(
            owner_id=ACTOR.account.id,
            agent_id=agent.id,
            version="v2",
            run_spec=RunSpec(
                command="python agent.py",
                secret_ids=[uuid.uuid4()],
                timeout_seconds=600,
            ),
        )
    )
    broken = await service.create_session_run(
        session_run_create(agent_version_id=broken_version.id), actor=ACTOR
    )
    healthy = await service.create_session_run(
        session_run_create(agent_version_id=version.id), actor=ACTOR
    )

    claimed = await service.claim_jobs(
        worker_id=worker.id,
        max_jobs=10,
        scope=WorkerScope(),
        actor=ACTOR,
    )

    assert [job.id for job, _ in claimed] == [healthy.id]
    failed = await repository.get(broken.id)
    assert failed.status is JobStatus.FAILED
    assert failed.error is not None
    assert "Failed to resolve the job spec" in failed.error
