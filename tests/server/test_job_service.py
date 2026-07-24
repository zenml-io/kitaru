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
    FakeCohortRepository,
    FakeExperimentRepository,
    FakeExperimentRunRepository,
    FakeJobRepository,
    FakeReplayConfigRepository,
    FakeSecretRepository,
    FakeSessionNodeRepository,
    FakeSessionRepository,
)
from kitaru.hashing import tool_call_cache_key
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.jobs import (
    JobFilter,
    JobUpdate,
    ReplayCreate,
)
from kitaru.server.application.services.job_service import JobService
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.agent_version import (
    AgentVersion,
    AgentVersionNotRunnable,
    NoRunnableAgentVersion,
    RunSpec,
)
from kitaru.server.domain.cohort import Cohort
from kitaru.server.domain.experiment import Experiment
from kitaru.server.domain.experiment_run import (
    ExperimentRun,
    ExperimentRunStatus,
)
from kitaru.server.domain.job import (
    HEARTBEAT_TIMEOUT_ERROR,
    InvalidJob,
    InvalidJobTransition,
    InvalidToolLookup,
    Job,
    JobActive,
    JobAlreadyLinked,
    JobMissingResultSession,
    JobNotActive,
    JobNotFound,
    JobNotStandalone,
    JobStatus,
)
from kitaru.server.domain.replay_config import (
    HistoryPolicy,
    HistoryScope,
    InvalidReplayConfig,
    PassthroughPolicy,
    ReplayConfig,
    ReplayConfigNotFound,
    ReplayOverride,
    ScorerConfig,
    ScoringPolicy,
    ScoringResult,
    SourceRef,
    ToolPolicyConfig,
)
from kitaru.server.domain.secret import Secret, SecretNotFound
from kitaru.server.domain.session import (
    Session,
    SessionNotFound,
    SessionOrigin,
    SessionStatus,
)
from kitaru.server.domain.session_node import (
    NodeStatus,
    NodeType,
    SessionNode,
)

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
) -> FakeJobRepository:
    """Provide a fake job repository."""
    return FakeJobRepository(session_repository, version_repository, config_repository)


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
def run_repository(
    experiment_repository: FakeExperimentRepository,
    repository: FakeJobRepository,
) -> FakeExperimentRunRepository:
    """Provide a fake experiment run repository."""
    return FakeExperimentRunRepository(experiment_repository, repository)


@pytest.fixture
def secret_repository() -> FakeSecretRepository:
    """Provide a fake secret repository."""
    return FakeSecretRepository()


@pytest.fixture
def service(
    repository: FakeJobRepository,
    config_repository: FakeReplayConfigRepository,
    session_repository: FakeSessionRepository,
    version_repository: FakeAgentVersionRepository,
    node_repository: FakeSessionNodeRepository,
    run_repository: FakeExperimentRunRepository,
    experiment_repository: FakeExperimentRepository,
    cohort_repository: FakeCohortRepository,
    secret_repository: FakeSecretRepository,
) -> JobService:
    """Provide a job service backed by the fake repositories."""
    return JobService(
        repository=repository,
        replay_config_repository=config_repository,
        session_repository=session_repository,
        agent_version_repository=version_repository,
        session_node_repository=node_repository,
        experiment_run_repository=run_repository,
        experiment_repository=experiment_repository,
        cohort_repository=cohort_repository,
        secret_repository=secret_repository,
        heartbeat_timeout_seconds=60,
        max_attempts=3,
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
        "original_session_id": session_id,
        "scoring_policy": SCORING_POLICY,
        **overrides,
    }
    return ReplayCreate.model_validate(values)


async def test_create_replay_defaults(
    service: JobService,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Create a standalone job defaulting the tool policy and version."""
    session = await create_session(session_repository, agent.id)
    job, config = await service.create_replay(replay_create(session.id), actor=ACTOR)
    assert job.experiment_run_id is None
    assert job.original_session_id == session.id
    assert job.result_session_id is None
    assert job.agent_version_id == version.id
    assert job.replay_config_id == config.id
    assert job.status is JobStatus.PENDING
    assert job.attempt == 1
    assert job.created is not None
    assert job.updated is not None
    assert config.override is None
    assert config.tool_policy == ToolPolicyConfig(default=HistoryPolicy())
    assert config.scoring_policy == SCORING_POLICY


async def test_create_replay_with_config(
    service: JobService,
    session_repository: FakeSessionRepository,
    config_repository: FakeReplayConfigRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Normalize the inline config into an own replay config row."""
    session = await create_session(session_repository, agent.id)
    override = ReplayOverride(model="claude-sonnet-5")
    tool_policy = ToolPolicyConfig(default=HistoryPolicy(scope=HistoryScope.AGENT))
    job, config = await service.create_replay(
        replay_create(session.id, override=override, tool_policy=tool_policy),
        actor=ACTOR,
    )
    stored = await config_repository.get(job.replay_config_id)
    assert stored.override == override
    assert stored.tool_policy == tool_policy
    assert stored == config


async def test_create_replay_rejects_cohort_scope(
    service: JobService,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Reject a history policy scoped to a cohort."""
    session = await create_session(session_repository, agent.id)
    tool_policy = ToolPolicyConfig(default=HistoryPolicy(scope=HistoryScope.COHORT))
    with pytest.raises(
        InvalidReplayConfig,
        match="Standalone replays cannot use history scope 'cohort'",
    ):
        await service.create_replay(
            replay_create(session.id, tool_policy=tool_policy), actor=ACTOR
        )
    per_tool = ToolPolicyConfig(
        default=HistoryPolicy(),
        tools={"search": HistoryPolicy(scope=HistoryScope.COHORT)},
    )
    with pytest.raises(InvalidReplayConfig):
        await service.create_replay(
            replay_create(session.id, tool_policy=per_tool), actor=ACTOR
        )


async def test_create_replay_in_progress_session(
    service: JobService,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Reject jobing an in-progress session."""
    session = await create_session(
        session_repository, agent.id, status=SessionStatus.IN_PROGRESS
    )
    with pytest.raises(InvalidJob, match=f"Session {session.id} is in progress"):
        await service.create_replay(replay_create(session.id), actor=ACTOR)


async def test_create_replay_unknown_session(service: JobService) -> None:
    """Raise for an unknown session id."""
    missing_id = uuid.uuid4()
    with pytest.raises(SessionNotFound, match=f"Session {missing_id} was not found"):
        await service.create_replay(replay_create(missing_id), actor=ACTOR)


async def test_create_replay_no_runnable_version(
    service: JobService,
    session_repository: FakeSessionRepository,
    agent: Agent,
) -> None:
    """Raise when the session's agent has no runnable version."""
    session = await create_session(session_repository, agent.id)
    with pytest.raises(
        NoRunnableAgentVersion, match=f"Agent {agent.id} has no runnable version"
    ):
        await service.create_replay(replay_create(session.id), actor=ACTOR)


async def test_create_replay_cross_agent_version(
    service: JobService,
    session_repository: FakeSessionRepository,
    version_repository: FakeAgentVersionRepository,
    agent_repository: FakeAgentRepository,
    agent: Agent,
) -> None:
    """Reject a version that belongs to another agent."""
    session = await create_session(session_repository, agent.id)
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
        await service.create_replay(
            replay_create(session.id, agent_version_id=other_version.id), actor=ACTOR
        )


async def test_create_replay_version_without_run_spec(
    service: JobService,
    session_repository: FakeSessionRepository,
    version_repository: FakeAgentVersionRepository,
    agent: Agent,
) -> None:
    """Reject an explicit version without a run spec."""
    session = await create_session(session_repository, agent.id)
    bare = await version_repository.create(
        AgentVersion(owner_id=ACTOR.account.id, agent_id=agent.id, version="v1")
    )
    with pytest.raises(
        AgentVersionNotRunnable, match=f"Agent version {bare.id} has no run spec"
    ):
        await service.create_replay(
            replay_create(session.id, agent_version_id=bare.id), actor=ACTOR
        )


async def test_get_job(
    service: JobService,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Load a stored job with its config."""
    session = await create_session(session_repository, agent.id)
    created, created_config = await service.create_replay(
        replay_create(session.id), actor=ACTOR
    )
    job, config = await service.get_job(created.id, actor=ACTOR)
    assert job == created
    assert config == created_config


async def test_get_job_not_found(service: JobService) -> None:
    """Raise for an unknown job id."""
    missing_id = uuid.uuid4()
    with pytest.raises(JobNotFound, match=f"Job {missing_id} was not found"):
        await service.get_job(missing_id, actor=ACTOR)


async def test_list_jobs_filters(
    service: JobService,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """List jobs filtered by session, status, and standalone."""
    first = await create_session(session_repository, agent.id)
    second = await create_session(session_repository, agent.id)
    job_one, _ = await service.create_replay(replay_create(first.id), actor=ACTOR)
    await service.create_replay(replay_create(second.id), actor=ACTOR)

    jobs, total = await service.list_jobs(JobFilter(), actor=ACTOR)
    assert total == 2

    jobs, total = await service.list_jobs(
        JobFilter(original_session_id=first.id), actor=ACTOR
    )
    assert total == 1
    assert jobs[0][0].id == job_one.id

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


def run_job(**overrides: object) -> Job:
    """Build a run-created job entity.

    Args:
        **overrides: Field overrides.

    Returns:
        Job entity.
    """
    values: dict[str, object] = {
        "experiment_run_id": uuid.uuid4(),
        "replay_config_id": uuid.uuid4(),
        "agent_version_id": uuid.uuid4(),
        "original_session_id": uuid.uuid4(),
        **overrides,
    }
    return Job.model_validate(values)


def test_job_claim_and_start() -> None:
    """Walk a run-created job from pending through running."""
    job = run_job()
    job.claim("worker-1")
    assert job.status is JobStatus.CLAIMED
    assert job.worker_id == "worker-1"
    assert job.claimed_at is not None
    assert job.heartbeat_at is not None
    job.start()
    assert job.status is JobStatus.RUNNING
    assert job.started_at is not None


def test_job_claim_requires_pending() -> None:
    """Reject claiming a job that is not pending."""
    job = run_job()
    job.claim("worker-1")
    with pytest.raises(
        InvalidJobTransition,
        match=f"Job {job.id} cannot transition from 'claimed' to 'claimed'",
    ):
        job.claim("worker-2")


def test_job_standalone_claim_and_start() -> None:
    """Walk a standalone job from pending through running."""
    job = run_job(experiment_run_id=None)
    job.claim("worker-1")
    assert job.status is JobStatus.CLAIMED
    assert job.worker_id == "worker-1"
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
    job.claim("worker-1")
    job.requeue()
    assert job.status is JobStatus.PENDING
    assert job.attempt == 2
    assert job.worker_id is None
    assert job.claimed_at is None
    assert job.heartbeat_at is None
    assert job.started_at is None
    with pytest.raises(InvalidJobTransition):
        job.requeue()


def finished_job(status: JobStatus) -> Job:
    """Build a standalone job finished in a status.

    Args:
        status: Failed, timed out, or canceled.

    Returns:
        Job entity.
    """
    job = run_job(experiment_run_id=None)
    job.claim("worker-1")
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
    completed.complete(ScoringResult(passed=True, score=1.0, scores={}), diff=None)
    with pytest.raises(InvalidJobTransition):
        completed.retry()


def test_job_complete_records_result() -> None:
    """Complete a running job with its scoring result."""
    job = run_job()
    job.claim("worker-1")
    job.start()
    job.link_result_session(uuid.uuid4())
    job.complete(
        ScoringResult(passed=True, score=0.8, scores={"conciseness": 0.8}),
        diff={"cost_delta": "-0.1"},
    )
    assert job.status is JobStatus.COMPLETED
    assert job.passed is True
    assert job.score == 0.8
    assert job.scores == {"conciseness": 0.8}
    assert job.diff == {"cost_delta": "-0.1"}
    assert job.ended_at is not None


def test_job_complete_requires_running() -> None:
    """Reject completing a job that is not running."""
    job = run_job()
    with pytest.raises(InvalidJobTransition):
        job.complete(ScoringResult(passed=True, score=1.0, scores={}), diff=None)


def test_job_fail_and_time_out() -> None:
    """Fail or time out a claimed or running job."""
    job = run_job()
    job.claim("worker-1")
    job.fail("agent exited with code 1")
    assert job.status is JobStatus.FAILED
    assert job.error == "agent exited with code 1"

    other = run_job()
    other.claim("worker-1")
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


def test_job_heartbeat_and_link() -> None:
    """Record heartbeats and link the result session while active."""
    job = run_job()
    with pytest.raises(JobNotActive):
        job.heartbeat()
    job.claim("worker-1")
    before = job.heartbeat_at
    job.heartbeat()
    assert job.heartbeat_at is not None
    assert before is not None
    assert job.heartbeat_at >= before

    session_id = uuid.uuid4()
    job.link_result_session(session_id)
    assert job.result_session_id == session_id
    with pytest.raises(
        JobAlreadyLinked,
        match=f"Job {job.id} already has a result session",
    ):
        job.link_result_session(uuid.uuid4())

    idle = run_job()
    with pytest.raises(JobNotActive, match=f"Job {idle.id} is not claimed or running"):
        idle.link_result_session(uuid.uuid4())


def test_job_with_staleness() -> None:
    """Report stale claims as pending or timed out without mutating."""
    job = run_job()
    job.claim("worker-1")
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
    exhausted.claim("worker-1")
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
) -> tuple[ExperimentRun, list[Job]]:
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
        Job(
            experiment_run_id=run.id,
            replay_config_id=config.id,
            agent_version_id=version.id,
            original_session_id=session.id,
        )
        for session in sessions
    ]
    run = await run_repository.create(run, jobs)
    stored, _ = await job_repository.query(JobFilter(experiment_run_id=run.id))
    return run, stored


async def test_get_spec_standalone(
    service: JobService,
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
    job, config = await service.create_replay(
        replay_create(session.id, override=override), actor=ACTOR
    )
    spec = await service.get_spec(job.id, actor=ACTOR)
    assert spec.job_id == job.id
    assert spec.inputs == {"prompt": "hi"}
    assert spec.override == override
    assert spec.tool_policy == config.tool_policy
    assert spec.scoring_policy == SCORING_POLICY
    assert spec.score_baselines is True
    assert spec.run_spec == version.run_spec
    assert spec.original_session_id == session.id
    assert {
        name: value.get_secret_value() for name, value in spec.secret_env.items()
    } == {"OPENAI_API_KEY": "sk-1", "SHARED": "second"}


async def test_get_spec_applies_prompt_override(
    service: JobService,
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
    job, _ = await service.create_replay(
        replay_create(session.id, override=override), actor=ACTOR
    )
    spec = await service.get_spec(job.id, actor=ACTOR)
    assert spec.inputs == "rewritten task"
    assert spec.run_spec == version.run_spec


async def test_get_spec_run_job_carries_score_baselines(
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
    """Resolve a run job spec with the run's baseline scoring flag."""
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
    spec = await service.get_spec(jobs[0].id, actor=ACTOR)
    assert spec.score_baselines is False


async def test_get_spec_version_without_run_spec(
    service: JobService,
    session_repository: FakeSessionRepository,
    version_repository: FakeAgentVersionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Raise when the stamped version lost its run spec."""
    session = await create_session(session_repository, agent.id)
    job, _ = await service.create_replay(replay_create(session.id), actor=ACTOR)
    await version_repository.update(version.model_copy(update={"run_spec": None}))
    with pytest.raises(
        AgentVersionNotRunnable, match=f"Agent version {version.id} has no run spec"
    ):
        await service.get_spec(job.id, actor=ACTOR)


async def test_get_spec_deleted_secret(
    service: JobService,
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
    job, _ = await service.create_replay(replay_create(session.id), actor=ACTOR)
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


async def test_update_job_standalone_lifecycle(
    service: JobService,
    repository: FakeJobRepository,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Run a standalone job from pending to completed with a diff."""
    session = await create_session(session_repository, agent.id)
    job, _ = await service.create_replay(replay_create(session.id), actor=ACTOR)
    updated, _ = await service.update_job(
        job.id, JobUpdate(status=JobStatus.RUNNING), actor=ACTOR
    )
    assert updated.status is JobStatus.RUNNING
    assert updated.started_at is not None

    result = await link_result_session(repository, session_repository, job.id, agent.id)
    completed, _ = await service.update_job(
        job.id,
        JobUpdate(
            status=JobStatus.COMPLETED,
            passed=True,
            score=0.8,
            scores={"conciseness": 0.8},
        ),
        actor=ACTOR,
    )
    assert completed.status is JobStatus.COMPLETED
    assert completed.result_session_id == result.id
    assert completed.passed is True
    assert completed.score == 0.8
    assert completed.scores == {"conciseness": 0.8}
    assert completed.ended_at is not None
    assert completed.diff is not None
    assert completed.diff["status_changed"] is False
    assert completed.diff["tool_calls"] == {
        "matched": 0,
        "mocked": 0,
        "added": 0,
        "removed": 0,
    }


async def test_update_job_completed_requires_scoring_result(
    service: JobService,
    repository: FakeJobRepository,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Reject completing without the full scoring result."""
    session = await create_session(session_repository, agent.id)
    job, _ = await service.create_replay(replay_create(session.id), actor=ACTOR)
    await service.update_job(job.id, JobUpdate(status=JobStatus.RUNNING), actor=ACTOR)
    running = await repository.get(job.id)
    running.link_result_session(session.id)
    await repository.update(running)
    with pytest.raises(
        InvalidJob, match="Completing a job requires passed, score, and scores"
    ):
        await service.update_job(
            job.id,
            JobUpdate(status=JobStatus.COMPLETED, passed=True),
            actor=ACTOR,
        )


async def test_update_job_completed_requires_result_session(
    service: JobService,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Reject completing an unlinked job."""
    session = await create_session(session_repository, agent.id)
    job, _ = await service.create_replay(replay_create(session.id), actor=ACTOR)
    await service.update_job(job.id, JobUpdate(status=JobStatus.RUNNING), actor=ACTOR)
    with pytest.raises(
        JobMissingResultSession,
        match=f"Job {job.id} has no result session",
    ):
        await service.update_job(
            job.id,
            JobUpdate(status=JobStatus.COMPLETED, passed=True, score=1.0, scores={}),
            actor=ACTOR,
        )


async def test_update_job_illegal_transitions(
    service: JobService,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Reject illegal runner transitions."""
    session = await create_session(session_repository, agent.id)
    job, _ = await service.create_replay(replay_create(session.id), actor=ACTOR)
    with pytest.raises(
        InvalidJobTransition,
        match=f"Job {job.id} cannot transition from 'pending' to 'completed'",
    ):
        await service.update_job(
            job.id,
            JobUpdate(status=JobStatus.COMPLETED, passed=True, score=1.0, scores={}),
            actor=ACTOR,
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
    await repository.claim_pending(run.id, "worker-1", 2)
    failed, _ = await service.update_job(
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
        second.id,
        JobUpdate(
            status=JobStatus.COMPLETED,
            passed=True,
            score=0.9,
            scores={"conciseness": 0.9},
        ),
        actor=ACTOR,
    )
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


async def test_heartbeat_job(
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
    """Record heartbeats and report cancellation."""
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
    with pytest.raises(JobNotActive, match=f"Job {job.id} is not claimed or running"):
        await service.heartbeat_job(job.id, actor=ACTOR)

    await repository.claim_pending(run.id, "worker-1", 1)
    assert await service.heartbeat_job(job.id, actor=ACTOR) == (
        JobStatus.CLAIMED,
        False,
    )
    heartbeat_at = (await repository.get(job.id)).heartbeat_at
    assert heartbeat_at is not None

    canceling = await run_repository.get(run.id)
    canceling.cancel()
    await run_repository.update(canceling)
    assert await service.heartbeat_job(job.id, actor=ACTOR) == (
        JobStatus.CLAIMED,
        True,
    )

    canceled = await repository.get(job.id)
    canceled.cancel()
    await repository.update(canceled)
    assert await service.heartbeat_job(job.id, actor=ACTOR) == (
        JobStatus.CANCELED,
        True,
    )


async def test_heartbeat_job_stops_on_terminal_statuses(
    service: JobService,
    repository: FakeJobRepository,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Report the stop flag for every terminal status without recording."""
    for status in (
        JobStatus.COMPLETED,
        JobStatus.FAILED,
        JobStatus.TIMED_OUT,
        JobStatus.CANCELED,
    ):
        session = await create_session(session_repository, agent.id)
        job, _ = await service.create_replay(replay_create(session.id), actor=ACTOR)
        stored = await repository.get(job.id)
        stored = stored.model_copy(update={"status": status})
        stored = await repository.update(stored)
        assert await service.heartbeat_job(job.id, actor=ACTOR) == (status, True)
        assert (await repository.get(job.id)).heartbeat_at is None


def build_service(
    repository: FakeJobRepository,
    config_repository: FakeReplayConfigRepository,
    session_repository: FakeSessionRepository,
    version_repository: FakeAgentVersionRepository,
    node_repository: FakeSessionNodeRepository,
    run_repository: FakeExperimentRunRepository,
    experiment_repository: FakeExperimentRepository,
    cohort_repository: FakeCohortRepository,
    secret_repository: FakeSecretRepository,
    heartbeat_timeout_seconds: int = 60,
    max_attempts: int = 3,
) -> JobService:
    """Build a job service with explicit staleness settings.

    Args:
        repository: Fake job repository.
        config_repository: Fake replay config repository.
        session_repository: Fake session repository.
        version_repository: Fake agent version repository.
        node_repository: Fake session node repository.
        run_repository: Fake experiment run repository.
        experiment_repository: Fake experiment repository.
        cohort_repository: Fake cohort repository.
        secret_repository: Fake secret repository.
        heartbeat_timeout_seconds: Heartbeat timeout, negative values mark
            every claim stale immediately.
        max_attempts: Attempt count at which a stale job times out.

    Returns:
        Job service.
    """
    return JobService(
        repository=repository,
        replay_config_repository=config_repository,
        session_repository=session_repository,
        agent_version_repository=version_repository,
        session_node_repository=node_repository,
        experiment_run_repository=run_repository,
        experiment_repository=experiment_repository,
        cohort_repository=cohort_repository,
        secret_repository=secret_repository,
        heartbeat_timeout_seconds=heartbeat_timeout_seconds,
        max_attempts=max_attempts,
    )


async def test_claim_job(
    service: JobService,
    repository: FakeJobRepository,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Claim a pending standalone job for a worker."""
    session = await create_session(session_repository, agent.id)
    job, config = await service.create_replay(replay_create(session.id), actor=ACTOR)
    claimed, claimed_config = await service.claim_job(
        job.id, worker_id="worker-1", actor=ACTOR
    )
    assert claimed.status is JobStatus.CLAIMED
    assert claimed.worker_id == "worker-1"
    assert claimed.claimed_at is not None
    assert claimed.heartbeat_at is not None
    assert claimed_config.id == config.id
    assert (await repository.get(job.id)).status is JobStatus.CLAIMED

    with pytest.raises(
        InvalidJobTransition,
        match=f"Job {job.id} cannot transition from 'claimed' to 'claimed'",
    ):
        await service.claim_job(job.id, worker_id="worker-2", actor=ACTOR)


async def test_claim_job_not_found(service: JobService) -> None:
    """Raise for an unknown job id."""
    missing_id = uuid.uuid4()
    with pytest.raises(JobNotFound, match=f"Job {missing_id} was not found"):
        await service.claim_job(missing_id, worker_id="worker-1", actor=ACTOR)


async def test_claim_job_rejects_run_job(
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
    """Reject claiming a run job through the standalone endpoint."""
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
    with pytest.raises(
        JobNotStandalone,
        match=f"Job {job.id} belongs to an experiment run",
    ):
        await service.claim_job(job.id, worker_id="worker-1", actor=ACTOR)


async def test_claim_job_resolves_stale_claim(
    repository: FakeJobRepository,
    config_repository: FakeReplayConfigRepository,
    session_repository: FakeSessionRepository,
    version_repository: FakeAgentVersionRepository,
    node_repository: FakeSessionNodeRepository,
    run_repository: FakeExperimentRunRepository,
    experiment_repository: FakeExperimentRepository,
    cohort_repository: FakeCohortRepository,
    secret_repository: FakeSecretRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Reclaim a standalone job whose worker lost its heartbeat."""
    stale_service = build_service(
        repository,
        config_repository,
        session_repository,
        version_repository,
        node_repository,
        run_repository,
        experiment_repository,
        cohort_repository,
        secret_repository,
        heartbeat_timeout_seconds=-60,
    )
    session = await create_session(session_repository, agent.id)
    job, _ = await stale_service.create_replay(replay_create(session.id), actor=ACTOR)
    await stale_service.claim_job(job.id, worker_id="worker-1", actor=ACTOR)

    claimed, _ = await stale_service.claim_job(
        job.id, worker_id="worker-2", actor=ACTOR
    )
    assert claimed.status is JobStatus.CLAIMED
    assert claimed.worker_id == "worker-2"
    assert claimed.attempt == 2


async def test_claim_job_times_out_exhausted_stale_claim(
    repository: FakeJobRepository,
    config_repository: FakeReplayConfigRepository,
    session_repository: FakeSessionRepository,
    version_repository: FakeAgentVersionRepository,
    node_repository: FakeSessionNodeRepository,
    run_repository: FakeExperimentRunRepository,
    experiment_repository: FakeExperimentRepository,
    cohort_repository: FakeCohortRepository,
    secret_repository: FakeSecretRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Persist the time-out of a stale claim out of attempts and reject."""
    stale_service = build_service(
        repository,
        config_repository,
        session_repository,
        version_repository,
        node_repository,
        run_repository,
        experiment_repository,
        cohort_repository,
        secret_repository,
        heartbeat_timeout_seconds=-60,
        max_attempts=1,
    )
    session = await create_session(session_repository, agent.id)
    job, _ = await stale_service.create_replay(replay_create(session.id), actor=ACTOR)
    await stale_service.claim_job(job.id, worker_id="worker-1", actor=ACTOR)

    with pytest.raises(
        InvalidJobTransition,
        match=f"Job {job.id} cannot transition from 'timed_out' to 'claimed'",
    ):
        await stale_service.claim_job(job.id, worker_id="worker-2", actor=ACTOR)
    stored = await repository.get(job.id)
    assert stored.status is JobStatus.TIMED_OUT
    assert stored.error == HEARTBEAT_TIMEOUT_ERROR


async def test_release_job(
    service: JobService,
    repository: FakeJobRepository,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Requeue a claimed or running job and reject other statuses."""
    session = await create_session(session_repository, agent.id)
    job, _ = await service.create_replay(replay_create(session.id), actor=ACTOR)
    with pytest.raises(
        InvalidJobTransition,
        match=f"Job {job.id} cannot transition from 'pending' to 'pending'",
    ):
        await service.release_job(job.id, actor=ACTOR)

    await service.claim_job(job.id, worker_id="worker-1", actor=ACTOR)
    released, _ = await service.release_job(job.id, actor=ACTOR)
    assert released.status is JobStatus.PENDING
    assert released.attempt == 2
    assert released.worker_id is None
    assert released.claimed_at is None
    assert released.heartbeat_at is None

    await service.update_job(job.id, JobUpdate(status=JobStatus.RUNNING), actor=ACTOR)
    released, _ = await service.release_job(job.id, actor=ACTOR)
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
    await repository.claim_pending(run.id, "worker-1", 1)
    released, _ = await service.release_job(jobs[0].id, actor=ACTOR)
    assert released.status is JobStatus.PENDING
    assert released.attempt == 2
    assert released.worker_id is None


async def test_retry_job(
    service: JobService,
    repository: FakeJobRepository,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Requeue a failed standalone job and clear its attempt state."""
    session = await create_session(session_repository, agent.id)
    job, _ = await service.create_replay(replay_create(session.id), actor=ACTOR)
    await service.update_job(job.id, JobUpdate(status=JobStatus.RUNNING), actor=ACTOR)
    await link_result_session(repository, session_repository, job.id, agent.id)
    await service.update_job(
        job.id,
        JobUpdate(status=JobStatus.FAILED, error="agent exited with code 1"),
        actor=ACTOR,
    )

    retried, _ = await service.retry_job(job.id, actor=ACTOR)
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
    await repository.claim_pending(run.id, "worker-1", 1)
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
    repository: FakeJobRepository,
    config_repository: FakeReplayConfigRepository,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Delete a pending standalone job and its unreferenced config."""
    session = await create_session(session_repository, agent.id)
    job, config = await service.create_replay(replay_create(session.id), actor=ACTOR)
    await service.delete_job(job.id, actor=ACTOR)
    with pytest.raises(JobNotFound):
        await repository.get(job.id)
    with pytest.raises(ReplayConfigNotFound):
        await config_repository.get(config.id)


async def test_delete_job_conflicts(
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
    """Reject deleting a claimed or running job or a run job."""
    session = await create_session(session_repository, agent.id)
    job, _ = await service.create_replay(replay_create(session.id), actor=ACTOR)
    await service.claim_job(job.id, worker_id="worker-1", actor=ACTOR)
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
    job, _ = await service.create_replay(replay_create(session.id), actor=ACTOR)
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
    job, _ = await service.create_replay(
        replay_create(
            session.id,
            tool_policy=ToolPolicyConfig(
                default=HistoryPolicy(scope=HistoryScope.AGENT)
            ),
        ),
        actor=ACTOR,
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
    job = next(entry for entry in jobs if entry.original_session_id == sessions[0].id)
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
        Job(
            replay_config_id=config.id,
            agent_version_id=version.id,
            original_session_id=session.id,
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
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Reject cache key mismatches and tools without a history policy."""
    session = await create_session(session_repository, agent.id)
    job, _ = await service.create_replay(
        replay_create(
            session.id,
            tool_policy=ToolPolicyConfig(
                default=HistoryPolicy(),
                tools={"search": PassthroughPolicy()},
            ),
        ),
        actor=ACTOR,
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


async def test_compute_diff_requires_result_session(
    service: JobService,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Raise when the job has no result session yet."""
    session = await create_session(session_repository, agent.id)
    job, _ = await service.create_replay(replay_create(session.id), actor=ACTOR)
    with pytest.raises(
        JobMissingResultSession, match=f"Job {job.id} has no result session"
    ):
        await service.compute_diff(job.id, actor=ACTOR)


async def test_compute_diff(
    service: JobService,
    repository: FakeJobRepository,
    session_repository: FakeSessionRepository,
    node_repository: FakeSessionNodeRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Compute the full diff between original and result session."""
    session = await create_session(session_repository, agent.id)
    await store_tool_result(
        node_repository,
        session.id,
        inputs={"city": "Berlin"},
        outputs={"temp": 21},
    )
    job, _ = await service.create_replay(
        replay_create(session.id, override=ReplayOverride(model="claude-sonnet-5")),
        actor=ACTOR,
    )
    await service.update_job(job.id, JobUpdate(status=JobStatus.RUNNING), actor=ACTOR)
    result = await link_result_session(repository, session_repository, job.id, agent.id)
    await store_tool_result(
        node_repository,
        result.id,
        inputs={"city": "Berlin"},
        outputs={"temp": 21},
        mocked=True,
    )
    diff = await service.compute_diff(job.id, actor=ACTOR)
    assert diff.replay_id == job.id
    assert diff.original_session_id == session.id
    assert diff.result_session_id == result.id
    assert len(diff.node_pairs) == 1
    pair = diff.node_pairs[0]
    assert pair.node_type is NodeType.TOOL_CALL
    assert pair.mocked is True
    assert pair.cache_key_changed is False
    assert pair.outputs_equal is True
    assert diff.added_nodes == []
    assert diff.removed_nodes == []
    assert diff.input_diff.model.effective == []


async def test_get_job_reports_staleness(
    repository: FakeJobRepository,
    config_repository: FakeReplayConfigRepository,
    session_repository: FakeSessionRepository,
    version_repository: FakeAgentVersionRepository,
    node_repository: FakeSessionNodeRepository,
    run_repository: FakeExperimentRunRepository,
    experiment_repository: FakeExperimentRepository,
    cohort_repository: FakeCohortRepository,
    secret_repository: FakeSecretRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Report stale claims as pending on reads without writing."""
    stale_service = JobService(
        repository=repository,
        replay_config_repository=config_repository,
        session_repository=session_repository,
        agent_version_repository=version_repository,
        session_node_repository=node_repository,
        experiment_run_repository=run_repository,
        experiment_repository=experiment_repository,
        cohort_repository=cohort_repository,
        secret_repository=secret_repository,
        heartbeat_timeout_seconds=-60,
        max_attempts=3,
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
    await repository.claim_pending(job.experiment_run_id, "worker-1", 1)
    reported, _ = await stale_service.get_job(job.id, actor=ACTOR)
    assert reported.status is JobStatus.PENDING
    assert reported.attempt == 2
    assert reported.worker_id is None
    # Reporting never writes.
    stored = await repository.get(job.id)
    assert stored.status is JobStatus.CLAIMED
