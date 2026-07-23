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
"""Tests for replay use cases and the replay state machine."""

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
    FakeReplayConfigRepository,
    FakeReplayRepository,
    FakeSecretRepository,
    FakeSessionNodeRepository,
    FakeSessionRepository,
)
from kitaru.hashing import tool_call_cache_key
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.replays import (
    ReplayCreate,
    ReplayFilter,
    ReplayUpdate,
)
from kitaru.server.application.services.replay_service import ReplayService
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
from kitaru.server.domain.replay import (
    HEARTBEAT_TIMEOUT_ERROR,
    InvalidReplay,
    InvalidReplayTransition,
    InvalidToolLookup,
    Replay,
    ReplayAlreadyLinked,
    ReplayMissingResultSession,
    ReplayNotActive,
    ReplayNotFound,
    ReplayStatus,
)
from kitaru.server.domain.replay_config import (
    HistoryPolicy,
    HistoryScope,
    InvalidReplayConfig,
    PassthroughPolicy,
    ReplayConfig,
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
) -> FakeReplayRepository:
    """Provide a fake replay repository."""
    return FakeReplayRepository(
        session_repository, version_repository, config_repository
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
def run_repository(
    experiment_repository: FakeExperimentRepository,
    repository: FakeReplayRepository,
) -> FakeExperimentRunRepository:
    """Provide a fake experiment run repository."""
    return FakeExperimentRunRepository(experiment_repository, repository)


@pytest.fixture
def secret_repository() -> FakeSecretRepository:
    """Provide a fake secret repository."""
    return FakeSecretRepository()


@pytest.fixture
def service(
    repository: FakeReplayRepository,
    config_repository: FakeReplayConfigRepository,
    session_repository: FakeSessionRepository,
    version_repository: FakeAgentVersionRepository,
    node_repository: FakeSessionNodeRepository,
    run_repository: FakeExperimentRunRepository,
    experiment_repository: FakeExperimentRepository,
    cohort_repository: FakeCohortRepository,
    secret_repository: FakeSecretRepository,
) -> ReplayService:
    """Provide a replay service backed by the fake repositories."""
    return ReplayService(
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
    """Store a recorded session for replay tests.

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
    service: ReplayService,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Create a standalone replay defaulting the tool policy and version."""
    session = await create_session(session_repository, agent.id)
    replay, config = await service.create_replay(replay_create(session.id), actor=ACTOR)
    assert replay.experiment_run_id is None
    assert replay.original_session_id == session.id
    assert replay.result_session_id is None
    assert replay.agent_version_id == version.id
    assert replay.replay_config_id == config.id
    assert replay.status is ReplayStatus.PENDING
    assert replay.attempt == 1
    assert replay.created is not None
    assert replay.updated is not None
    assert config.override is None
    assert config.tool_policy == ToolPolicyConfig(default=HistoryPolicy())
    assert config.scoring_policy == SCORING_POLICY


async def test_create_replay_with_config(
    service: ReplayService,
    session_repository: FakeSessionRepository,
    config_repository: FakeReplayConfigRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Normalize the inline config into an own replay config row."""
    session = await create_session(session_repository, agent.id)
    override = ReplayOverride(model="claude-sonnet-5")
    tool_policy = ToolPolicyConfig(default=HistoryPolicy(scope=HistoryScope.AGENT))
    replay, config = await service.create_replay(
        replay_create(session.id, override=override, tool_policy=tool_policy),
        actor=ACTOR,
    )
    stored = await config_repository.get(replay.replay_config_id)
    assert stored.override == override
    assert stored.tool_policy == tool_policy
    assert stored == config


async def test_create_replay_rejects_cohort_scope(
    service: ReplayService,
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
    service: ReplayService,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Reject replaying an in-progress session."""
    session = await create_session(
        session_repository, agent.id, status=SessionStatus.IN_PROGRESS
    )
    with pytest.raises(InvalidReplay, match=f"Session {session.id} is in progress"):
        await service.create_replay(replay_create(session.id), actor=ACTOR)


async def test_create_replay_unknown_session(service: ReplayService) -> None:
    """Raise for an unknown session id."""
    missing_id = uuid.uuid4()
    with pytest.raises(SessionNotFound, match=f"Session {missing_id} was not found"):
        await service.create_replay(replay_create(missing_id), actor=ACTOR)


async def test_create_replay_no_runnable_version(
    service: ReplayService,
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
    service: ReplayService,
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
        InvalidReplay,
        match=f"Agent version {other_version.id} does not belong to agent {agent.id}",
    ):
        await service.create_replay(
            replay_create(session.id, agent_version_id=other_version.id), actor=ACTOR
        )


async def test_create_replay_version_without_run_spec(
    service: ReplayService,
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


async def test_get_replay(
    service: ReplayService,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Load a stored replay with its config."""
    session = await create_session(session_repository, agent.id)
    created, created_config = await service.create_replay(
        replay_create(session.id), actor=ACTOR
    )
    replay, config = await service.get_replay(created.id, actor=ACTOR)
    assert replay == created
    assert config == created_config


async def test_get_replay_not_found(service: ReplayService) -> None:
    """Raise for an unknown replay id."""
    missing_id = uuid.uuid4()
    with pytest.raises(ReplayNotFound, match=f"Replay {missing_id} was not found"):
        await service.get_replay(missing_id, actor=ACTOR)


async def test_list_replays_filters(
    service: ReplayService,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """List replays filtered by session, status, and standalone."""
    first = await create_session(session_repository, agent.id)
    second = await create_session(session_repository, agent.id)
    replay_one, _ = await service.create_replay(replay_create(first.id), actor=ACTOR)
    await service.create_replay(replay_create(second.id), actor=ACTOR)

    replays, total = await service.list_replays(ReplayFilter(), actor=ACTOR)
    assert total == 2

    replays, total = await service.list_replays(
        ReplayFilter(original_session_id=first.id), actor=ACTOR
    )
    assert total == 1
    assert replays[0][0].id == replay_one.id

    replays, total = await service.list_replays(
        ReplayFilter(status=ReplayStatus.PENDING), actor=ACTOR
    )
    assert total == 2
    replays, total = await service.list_replays(
        ReplayFilter(status=ReplayStatus.RUNNING), actor=ACTOR
    )
    assert total == 0

    replays, total = await service.list_replays(
        ReplayFilter(standalone=True), actor=ACTOR
    )
    assert total == 2
    replays, total = await service.list_replays(
        ReplayFilter(standalone=False), actor=ACTOR
    )
    assert total == 0

    replays, total = await service.list_replays(
        ReplayFilter(page=2, page_size=1), actor=ACTOR
    )
    assert total == 2
    assert len(replays) == 1


def run_replay(**overrides: object) -> Replay:
    """Build a run-created replay entity.

    Args:
        **overrides: Field overrides.

    Returns:
        Replay entity.
    """
    values: dict[str, object] = {
        "experiment_run_id": uuid.uuid4(),
        "replay_config_id": uuid.uuid4(),
        "agent_version_id": uuid.uuid4(),
        "original_session_id": uuid.uuid4(),
        **overrides,
    }
    return Replay.model_validate(values)


def test_replay_claim_and_start() -> None:
    """Walk a run-created replay from pending through running."""
    replay = run_replay()
    replay.claim("worker-1")
    assert replay.status is ReplayStatus.CLAIMED
    assert replay.worker_id == "worker-1"
    assert replay.claimed_at is not None
    assert replay.heartbeat_at is not None
    replay.start()
    assert replay.status is ReplayStatus.RUNNING
    assert replay.started_at is not None


def test_replay_claim_requires_pending() -> None:
    """Reject claiming a replay that is not pending."""
    replay = run_replay()
    replay.claim("worker-1")
    with pytest.raises(
        InvalidReplayTransition,
        match=f"Replay {replay.id} cannot transition from 'claimed' to 'claimed'",
    ):
        replay.claim("worker-2")


def test_replay_claim_rejects_standalone() -> None:
    """Reject claiming a standalone replay."""
    replay = run_replay(experiment_run_id=None)
    with pytest.raises(InvalidReplayTransition):
        replay.claim("worker-1")


def test_replay_standalone_starts_from_pending() -> None:
    """Skip the claim for standalone replays."""
    replay = run_replay(experiment_run_id=None)
    replay.start()
    assert replay.status is ReplayStatus.RUNNING


def test_replay_run_created_start_requires_claim() -> None:
    """Reject starting a run-created replay that was not claimed."""
    replay = run_replay()
    with pytest.raises(InvalidReplayTransition):
        replay.start()


def test_replay_requeue_increments_attempt() -> None:
    """Requeue a claimed replay and clear the claim state."""
    replay = run_replay()
    replay.claim("worker-1")
    replay.requeue()
    assert replay.status is ReplayStatus.PENDING
    assert replay.attempt == 2
    assert replay.worker_id is None
    assert replay.claimed_at is None
    assert replay.heartbeat_at is None
    assert replay.started_at is None
    with pytest.raises(InvalidReplayTransition):
        replay.requeue()


def test_replay_complete_records_result() -> None:
    """Complete a running replay with its scoring result."""
    replay = run_replay()
    replay.claim("worker-1")
    replay.start()
    replay.link_result_session(uuid.uuid4())
    replay.complete(
        ScoringResult(passed=True, score=0.8, scores={"conciseness": 0.8}),
        diff={"cost_delta": "-0.1"},
    )
    assert replay.status is ReplayStatus.COMPLETED
    assert replay.passed is True
    assert replay.score == 0.8
    assert replay.scores == {"conciseness": 0.8}
    assert replay.diff == {"cost_delta": "-0.1"}
    assert replay.ended_at is not None


def test_replay_complete_requires_running() -> None:
    """Reject completing a replay that is not running."""
    replay = run_replay()
    with pytest.raises(InvalidReplayTransition):
        replay.complete(ScoringResult(passed=True, score=1.0, scores={}), diff=None)


def test_replay_fail_and_time_out() -> None:
    """Fail or time out a claimed or running replay."""
    replay = run_replay()
    replay.claim("worker-1")
    replay.fail("agent exited with code 1")
    assert replay.status is ReplayStatus.FAILED
    assert replay.error == "agent exited with code 1"

    other = run_replay()
    other.claim("worker-1")
    other.start()
    other.time_out("wall clock limit exceeded")
    assert other.status is ReplayStatus.TIMED_OUT
    with pytest.raises(InvalidReplayTransition):
        other.fail("late failure")


def test_replay_cancel() -> None:
    """Cancel a replay in any non-terminal status."""
    replay = run_replay()
    replay.cancel()
    assert replay.status is ReplayStatus.CANCELED
    with pytest.raises(InvalidReplayTransition):
        replay.cancel()


def test_replay_heartbeat_and_link() -> None:
    """Record heartbeats and link the result session while active."""
    replay = run_replay()
    with pytest.raises(ReplayNotActive):
        replay.heartbeat()
    replay.claim("worker-1")
    before = replay.heartbeat_at
    replay.heartbeat()
    assert replay.heartbeat_at is not None
    assert before is not None
    assert replay.heartbeat_at >= before

    session_id = uuid.uuid4()
    replay.link_result_session(session_id)
    assert replay.result_session_id == session_id
    with pytest.raises(
        ReplayAlreadyLinked,
        match=f"Replay {replay.id} already has a result session",
    ):
        replay.link_result_session(uuid.uuid4())

    idle = run_replay()
    with pytest.raises(
        ReplayNotActive, match=f"Replay {idle.id} is not claimed or running"
    ):
        idle.link_result_session(uuid.uuid4())


def test_replay_with_staleness() -> None:
    """Report stale claims as pending or timed out without mutating."""
    replay = run_replay()
    replay.claim("worker-1")
    fresh = datetime.now(UTC) - timedelta(seconds=60)
    assert replay.with_staleness(fresh, 3) is replay

    stale = datetime.now(UTC) + timedelta(seconds=60)
    reported = replay.with_staleness(stale, 3)
    assert reported is not replay
    assert reported.status is ReplayStatus.PENDING
    assert reported.attempt == 2
    assert reported.worker_id is None
    assert replay.status is ReplayStatus.CLAIMED

    exhausted = run_replay(attempt=3)
    exhausted.claim("worker-1")
    reported = exhausted.with_staleness(stale, 3)
    assert reported.status is ReplayStatus.TIMED_OUT
    assert reported.error == HEARTBEAT_TIMEOUT_ERROR

    terminal = run_replay()
    terminal.cancel()
    assert terminal.with_staleness(stale, 3) is terminal


async def seed_run(
    session_repository: FakeSessionRepository,
    cohort_repository: FakeCohortRepository,
    config_repository: FakeReplayConfigRepository,
    experiment_repository: FakeExperimentRepository,
    run_repository: FakeExperimentRunRepository,
    replay_repository: FakeReplayRepository,
    agent: Agent,
    version: AgentVersion,
    sessions: list[Session],
    tool_policy: ToolPolicyConfig | None = None,
    score_baselines: bool = False,
    name: str = "swap-model",
) -> tuple[ExperimentRun, list[Replay]]:
    """Store a run with one pending replay per session.

    Args:
        session_repository: Fake session repository.
        cohort_repository: Fake cohort repository.
        config_repository: Fake replay config repository.
        experiment_repository: Fake experiment repository.
        run_repository: Fake experiment run repository.
        replay_repository: Fake replay repository.
        agent: Agent of the sessions.
        version: Agent version to execute.
        sessions: Original sessions.
        tool_policy: Tool policy of the config.
        score_baselines: Baseline scoring flag of the run.
        name: Experiment and cohort name.

    Returns:
        Stored run and its replays.
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
    replays = [
        Replay(
            experiment_run_id=run.id,
            replay_config_id=config.id,
            agent_version_id=version.id,
            original_session_id=session.id,
        )
        for session in sessions
    ]
    run = await run_repository.create(run, replays)
    stored, _ = await replay_repository.query(ReplayFilter(experiment_run_id=run.id))
    return run, stored


async def test_get_spec_standalone(
    service: ReplayService,
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
    replay, config = await service.create_replay(
        replay_create(session.id, override=override), actor=ACTOR
    )
    spec = await service.get_spec(replay.id, actor=ACTOR)
    assert spec.replay_id == replay.id
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


async def test_get_spec_run_replay_carries_score_baselines(
    service: ReplayService,
    repository: FakeReplayRepository,
    session_repository: FakeSessionRepository,
    cohort_repository: FakeCohortRepository,
    config_repository: FakeReplayConfigRepository,
    experiment_repository: FakeExperimentRepository,
    run_repository: FakeExperimentRunRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Resolve a run replay spec with the run's baseline scoring flag."""
    session = await create_session(session_repository, agent.id)
    _, replays = await seed_run(
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
    spec = await service.get_spec(replays[0].id, actor=ACTOR)
    assert spec.score_baselines is False


async def test_get_spec_version_without_run_spec(
    service: ReplayService,
    session_repository: FakeSessionRepository,
    version_repository: FakeAgentVersionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Raise when the stamped version lost its run spec."""
    session = await create_session(session_repository, agent.id)
    replay, _ = await service.create_replay(replay_create(session.id), actor=ACTOR)
    await version_repository.update(version.model_copy(update={"run_spec": None}))
    with pytest.raises(
        AgentVersionNotRunnable, match=f"Agent version {version.id} has no run spec"
    ):
        await service.get_spec(replay.id, actor=ACTOR)


async def test_get_spec_deleted_secret(
    service: ReplayService,
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
    replay, _ = await service.create_replay(replay_create(session.id), actor=ACTOR)
    with pytest.raises(SecretNotFound, match=f"Secret {missing_id} was not found"):
        await service.get_spec(replay.id, actor=ACTOR)


async def link_result_session(
    repository: FakeReplayRepository,
    session_repository: FakeSessionRepository,
    replay_id: uuid.UUID,
    agent_id: uuid.UUID,
) -> Session:
    """Link a completed result session to a running replay.

    Args:
        repository: Fake replay repository.
        session_repository: Fake session repository.
        replay_id: Id of the replay.
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
    running = await repository.get(replay_id)
    running.link_result_session(result.id)
    await repository.update(running)
    return result


async def test_update_replay_standalone_lifecycle(
    service: ReplayService,
    repository: FakeReplayRepository,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Run a standalone replay from pending to completed with a diff."""
    session = await create_session(session_repository, agent.id)
    replay, _ = await service.create_replay(replay_create(session.id), actor=ACTOR)
    updated, _ = await service.update_replay(
        replay.id, ReplayUpdate(status=ReplayStatus.RUNNING), actor=ACTOR
    )
    assert updated.status is ReplayStatus.RUNNING
    assert updated.started_at is not None

    result = await link_result_session(
        repository, session_repository, replay.id, agent.id
    )
    completed, _ = await service.update_replay(
        replay.id,
        ReplayUpdate(
            status=ReplayStatus.COMPLETED,
            passed=True,
            score=0.8,
            scores={"conciseness": 0.8},
        ),
        actor=ACTOR,
    )
    assert completed.status is ReplayStatus.COMPLETED
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


async def test_update_replay_completed_requires_scoring_result(
    service: ReplayService,
    repository: FakeReplayRepository,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Reject completing without the full scoring result."""
    session = await create_session(session_repository, agent.id)
    replay, _ = await service.create_replay(replay_create(session.id), actor=ACTOR)
    await service.update_replay(
        replay.id, ReplayUpdate(status=ReplayStatus.RUNNING), actor=ACTOR
    )
    running = await repository.get(replay.id)
    running.link_result_session(session.id)
    await repository.update(running)
    with pytest.raises(
        InvalidReplay, match="Completing a replay requires passed, score, and scores"
    ):
        await service.update_replay(
            replay.id,
            ReplayUpdate(status=ReplayStatus.COMPLETED, passed=True),
            actor=ACTOR,
        )


async def test_update_replay_completed_requires_result_session(
    service: ReplayService,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Reject completing an unlinked replay."""
    session = await create_session(session_repository, agent.id)
    replay, _ = await service.create_replay(replay_create(session.id), actor=ACTOR)
    await service.update_replay(
        replay.id, ReplayUpdate(status=ReplayStatus.RUNNING), actor=ACTOR
    )
    with pytest.raises(
        ReplayMissingResultSession,
        match=f"Replay {replay.id} has no result session",
    ):
        await service.update_replay(
            replay.id,
            ReplayUpdate(
                status=ReplayStatus.COMPLETED, passed=True, score=1.0, scores={}
            ),
            actor=ACTOR,
        )


async def test_update_replay_illegal_transitions(
    service: ReplayService,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Reject illegal runner transitions."""
    session = await create_session(session_repository, agent.id)
    replay, _ = await service.create_replay(replay_create(session.id), actor=ACTOR)
    with pytest.raises(
        InvalidReplayTransition,
        match=f"Replay {replay.id} cannot transition from 'pending' to 'completed'",
    ):
        await service.update_replay(
            replay.id,
            ReplayUpdate(
                status=ReplayStatus.COMPLETED, passed=True, score=1.0, scores={}
            ),
            actor=ACTOR,
        )
    with pytest.raises(InvalidReplayTransition):
        await service.update_replay(
            replay.id, ReplayUpdate(status=ReplayStatus.PENDING), actor=ACTOR
        )
    with pytest.raises(InvalidReplay, match="Failing a replay requires an error"):
        await service.update_replay(
            replay.id, ReplayUpdate(status=ReplayStatus.FAILED), actor=ACTOR
        )
    with pytest.raises(InvalidReplay, match="Timing out a replay requires an error"):
        await service.update_replay(
            replay.id, ReplayUpdate(status=ReplayStatus.TIMED_OUT), actor=ACTOR
        )


async def test_update_replay_finalizes_run(
    service: ReplayService,
    repository: FakeReplayRepository,
    session_repository: FakeSessionRepository,
    cohort_repository: FakeCohortRepository,
    config_repository: FakeReplayConfigRepository,
    experiment_repository: FakeExperimentRepository,
    run_repository: FakeExperimentRunRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Finalize the run when its last replay goes terminal."""
    sessions = [await create_session(session_repository, agent.id) for _ in range(2)]
    run, replays = await seed_run(
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
    first, second = replays
    await repository.claim_pending(run.id, "worker-1", 2)
    failed, _ = await service.update_replay(
        first.id,
        ReplayUpdate(status=ReplayStatus.FAILED, error="agent exited with code 1"),
        actor=ACTOR,
    )
    assert failed.status is ReplayStatus.FAILED
    # One terminal replay does not finalize the run.
    assert (await run_repository.get(run.id)).status is ExperimentRunStatus.PENDING

    await service.update_replay(
        second.id, ReplayUpdate(status=ReplayStatus.RUNNING), actor=ACTOR
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
    await service.update_replay(
        second.id,
        ReplayUpdate(
            status=ReplayStatus.COMPLETED,
            passed=True,
            score=0.9,
            scores={"conciseness": 0.9},
        ),
        actor=ACTOR,
    )
    finalized = await run_repository.get(run.id)
    assert finalized.status is ExperimentRunStatus.COMPLETED
    assert finalized.ended_at is not None
    assert finalized.summary is not None
    assert finalized.summary["replay_counts_by_status"] == {
        "failed": 1,
        "completed": 1,
    }
    assert finalized.summary["pass_rate"] == 1.0
    assert finalized.summary["total_cost"]["replay"] == pytest.approx(0.1)


async def test_heartbeat_replay(
    service: ReplayService,
    repository: FakeReplayRepository,
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
    run, replays = await seed_run(
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
    replay = replays[0]
    with pytest.raises(
        ReplayNotActive, match=f"Replay {replay.id} is not claimed or running"
    ):
        await service.heartbeat_replay(replay.id, actor=ACTOR)

    await repository.claim_pending(run.id, "worker-1", 1)
    assert await service.heartbeat_replay(replay.id, actor=ACTOR) is False
    heartbeat_at = (await repository.get(replay.id)).heartbeat_at
    assert heartbeat_at is not None

    canceling = await run_repository.get(run.id)
    canceling.cancel()
    await run_repository.update(canceling)
    assert await service.heartbeat_replay(replay.id, actor=ACTOR) is True

    canceled = await repository.get(replay.id)
    canceled.cancel()
    await repository.update(canceled)
    assert await service.heartbeat_replay(replay.id, actor=ACTOR) is True


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
    service: ReplayService,
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
    replay, _ = await service.create_replay(replay_create(session.id), actor=ACTOR)
    cache_key = tool_call_cache_key("get_weather", inputs)
    found = await service.tool_lookup(
        replay.id, "get_weather", inputs, cache_key, actor=ACTOR
    )
    assert found is not None
    assert found.outputs == {"temp": 21}

    miss = await service.tool_lookup(
        replay.id,
        "get_weather",
        {"city": "Paris"},
        tool_call_cache_key("get_weather", {"city": "Paris"}),
        actor=ACTOR,
    )
    assert miss is None


async def test_tool_lookup_agent_scope(
    service: ReplayService,
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
    replay, _ = await service.create_replay(
        replay_create(
            session.id,
            tool_policy=ToolPolicyConfig(
                default=HistoryPolicy(scope=HistoryScope.AGENT)
            ),
        ),
        actor=ACTOR,
    )
    found = await service.tool_lookup(
        replay.id,
        "get_weather",
        inputs,
        tool_call_cache_key("get_weather", inputs),
        actor=ACTOR,
    )
    assert found is not None
    assert found.session_id == other.id


async def test_tool_lookup_cohort_scope(
    service: ReplayService,
    repository: FakeReplayRepository,
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
    _, replays = await seed_run(
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
    replay = next(
        entry for entry in replays if entry.original_session_id == sessions[0].id
    )
    found = await service.tool_lookup(
        replay.id,
        "get_weather",
        inputs,
        tool_call_cache_key("get_weather", inputs),
        actor=ACTOR,
    )
    assert found is not None
    assert found.session_id == sessions[1].id

    paris = {"city": "Paris"}
    miss = await service.tool_lookup(
        replay.id,
        "get_weather",
        paris,
        tool_call_cache_key("get_weather", paris),
        actor=ACTOR,
    )
    assert miss is None


async def test_tool_lookup_cohort_scope_standalone(
    service: ReplayService,
    repository: FakeReplayRepository,
    session_repository: FakeSessionRepository,
    config_repository: FakeReplayConfigRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Reject a cohort scope on a standalone replay."""
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
    replay = await repository.create(
        Replay(
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
            replay.id,
            "get_weather",
            inputs,
            tool_call_cache_key("get_weather", inputs),
            actor=ACTOR,
        )


async def test_tool_lookup_rejects_mismatch_and_non_history(
    service: ReplayService,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Reject cache key mismatches and tools without a history policy."""
    session = await create_session(session_repository, agent.id)
    replay, _ = await service.create_replay(
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
            replay.id, "get_weather", {"city": "Berlin"}, "a" * 64, actor=ACTOR
        )
    inputs = {"query": "kitaru"}
    with pytest.raises(
        InvalidToolLookup, match="Tool 'search' resolves to no history policy"
    ):
        await service.tool_lookup(
            replay.id,
            "search",
            inputs,
            tool_call_cache_key("search", inputs),
            actor=ACTOR,
        )


async def test_compute_diff_requires_result_session(
    service: ReplayService,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Raise when the replay has no result session yet."""
    session = await create_session(session_repository, agent.id)
    replay, _ = await service.create_replay(replay_create(session.id), actor=ACTOR)
    with pytest.raises(
        ReplayMissingResultSession, match=f"Replay {replay.id} has no result session"
    ):
        await service.compute_diff(replay.id, actor=ACTOR)


async def test_compute_diff(
    service: ReplayService,
    repository: FakeReplayRepository,
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
    replay, _ = await service.create_replay(
        replay_create(session.id, override=ReplayOverride(model="claude-sonnet-5")),
        actor=ACTOR,
    )
    await service.update_replay(
        replay.id, ReplayUpdate(status=ReplayStatus.RUNNING), actor=ACTOR
    )
    result = await link_result_session(
        repository, session_repository, replay.id, agent.id
    )
    await store_tool_result(
        node_repository,
        result.id,
        inputs={"city": "Berlin"},
        outputs={"temp": 21},
        mocked=True,
    )
    diff = await service.compute_diff(replay.id, actor=ACTOR)
    assert diff.replay_id == replay.id
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


async def test_get_replay_reports_staleness(
    repository: FakeReplayRepository,
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
    stale_service = ReplayService(
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
    _, replays = await seed_run(
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
    replay = replays[0]
    assert replay.experiment_run_id is not None
    await repository.claim_pending(replay.experiment_run_id, "worker-1", 1)
    reported, _ = await stale_service.get_replay(replay.id, actor=ACTOR)
    assert reported.status is ReplayStatus.PENDING
    assert reported.attempt == 2
    assert reported.worker_id is None
    # Reporting never writes.
    stored = await repository.get(replay.id)
    assert stored.status is ReplayStatus.CLAIMED
