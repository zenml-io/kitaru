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
"""Tests for replay use cases."""

import uuid

import pytest

from conftest import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeBlobRepository,
    FakeJobRepository,
    FakePluginRepository,
    FakeReplayConfigRepository,
    FakeReplayRepository,
    FakeSessionNodeRepository,
    FakeSessionRepository,
)
from kitaru.hashing import tool_call_cache_key
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.replays import ReplayCreate, ReplayFilter
from kitaru.server.application.services.replay_service import ReplayService
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.agent_version import (
    AgentVersion,
    AgentVersionNotRunnable,
    NoRunnableAgentVersion,
    RunSpec,
)
from kitaru.server.domain.blob import Blob
from kitaru.server.domain.job import (
    InvalidJob,
    JobMissingResultSession,
    JobStatus,
    ReplayJob,
)
from kitaru.server.domain.plugin import (
    Plugin,
    PluginFormat,
    PluginKind,
    PluginNameNotFound,
    PluginVersion,
    PluginVersionNotFound,
)
from kitaru.server.domain.replay import ReplayNotFound
from kitaru.server.domain.replay_config import (
    HistoryPolicy,
    HistoryScope,
    InvalidReplayConfig,
    RegistryScorerConfig,
    ReplayOverride,
    ScoringPolicy,
    ScoringResult,
    SourceRef,
    SourceScorerConfig,
    ToolPolicyConfig,
)
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
def node_repository(
    session_repository: FakeSessionRepository,
) -> FakeSessionNodeRepository:
    """Provide a fake session node repository."""
    return FakeSessionNodeRepository(session_repository)


@pytest.fixture
def config_repository() -> FakeReplayConfigRepository:
    """Provide a fake replay config repository."""
    return FakeReplayConfigRepository()


@pytest.fixture
def blob_repository() -> FakeBlobRepository:
    """Provide a fake blob repository."""
    return FakeBlobRepository()


@pytest.fixture
def plugin_repository(blob_repository: FakeBlobRepository) -> FakePluginRepository:
    """Provide a fake plugin repository."""
    return FakePluginRepository(blob_repository)


@pytest.fixture
def job_repository(
    session_repository: FakeSessionRepository,
    version_repository: FakeAgentVersionRepository,
    plugin_repository: FakePluginRepository,
    blob_repository: FakeBlobRepository,
) -> FakeJobRepository:
    """Provide a fake job repository."""
    return FakeJobRepository(
        session_repository, version_repository, plugin_repository, blob_repository
    )


@pytest.fixture
def repository(
    job_repository: FakeJobRepository,
    config_repository: FakeReplayConfigRepository,
    session_repository: FakeSessionRepository,
) -> FakeReplayRepository:
    """Provide a fake replay repository."""
    return FakeReplayRepository(job_repository, config_repository, session_repository)


@pytest.fixture
def service(
    repository: FakeReplayRepository,
    job_repository: FakeJobRepository,
    config_repository: FakeReplayConfigRepository,
    session_repository: FakeSessionRepository,
    node_repository: FakeSessionNodeRepository,
    version_repository: FakeAgentVersionRepository,
    plugin_repository: FakePluginRepository,
) -> ReplayService:
    """Provide a replay service backed by the fake repositories."""
    return ReplayService(
        repository=repository,
        job_repository=job_repository,
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
        "input_session_id": session_id,
        "scoring_policy": SCORING_POLICY,
        **overrides,
    }
    return ReplayCreate.model_validate(values)


async def store_tool_result(
    node_repository: FakeSessionNodeRepository,
    session_id: uuid.UUID,
    inputs: object,
    outputs: object,
    mocked: bool = False,
) -> SessionNode:
    """Store a completed tool call node with its computed cache key.

    Args:
        node_repository: Fake session node repository.
        session_id: Id of the session.
        inputs: Tool call inputs.
        outputs: Tool call outputs.
        mocked: Whether the node is marked mocked.

    Returns:
        Stored node.
    """
    stored = await node_repository.upsert(
        [
            SessionNode(
                session_id=session_id,
                key="tool_call:get_weather#1",
                sequence=0,
                node_type=NodeType.TOOL_CALL,
                name="get_weather",
                status=NodeStatus.COMPLETED,
                tool_name="get_weather",
                cache_key=tool_call_cache_key("get_weather", inputs),
                inputs=inputs,
                outputs=outputs,
                attributes={"mocked": True} if mocked else {},
            )
        ]
    )
    return stored[0]


async def register_scorer(
    plugin_repository: FakePluginRepository,
    blob_repository: FakeBlobRepository,
    name: str,
) -> Plugin:
    """Register a scorer plugin with one code version.

    Args:
        plugin_repository: Fake plugin repository.
        blob_repository: Fake blob repository.
        name: Scorer name.

    Returns:
        Stored plugin with the latest version counter set.
    """
    plugin = await plugin_repository.create(
        Plugin(owner_id=ACTOR.account.id, kind=PluginKind.SCORER, name=name)
    )
    blob = await blob_repository.create(
        Blob(
            owner_id=ACTOR.account.id,
            sha256=name.ljust(64, "0"),
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


async def test_create_replay_defaults(
    service: ReplayService,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Create a replay with its job, defaulting policy and version."""
    session = await create_session(session_repository, agent.id)
    replay, job, config = await service.create_replay(
        replay_create(session.id), actor=ACTOR
    )
    assert replay.job_id == job.id
    assert replay.experiment_run_id is None
    assert replay.input_session_id == session.id
    assert replay.replay_config_id == config.id
    assert replay.passed is None
    assert replay.error is None
    assert replay.created is not None
    assert replay.updated is not None
    assert job.input_session_id == session.id
    assert job.result_session_id is None
    assert job.agent_version_id == version.id
    assert job.status is JobStatus.PENDING
    assert job.attempt == 1
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
    replay, _, config = await service.create_replay(
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
    with pytest.raises(InvalidJob, match=f"Session {session.id} is in progress"):
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
        InvalidJob,
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


async def test_create_replay_validates_registry_scorers(
    service: ReplayService,
    session_repository: FakeSessionRepository,
    plugin_repository: FakePluginRepository,
    blob_repository: FakeBlobRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Reject a replay naming an unregistered scorer or version."""
    session = await create_session(session_repository, agent.id)
    policy = ScoringPolicy(
        scorers=[RegistryScorerConfig(name="relevance")], pass_threshold=0.5
    )
    with pytest.raises(
        PluginNameNotFound, match="Plugin 'relevance' of kind 'scorer' was not found"
    ):
        await service.create_replay(
            replay_create(session.id, scoring_policy=policy), actor=ACTOR
        )
    plugin = await register_scorer(plugin_repository, blob_repository, "relevance")
    pinned = ScoringPolicy(
        scorers=[RegistryScorerConfig(name="relevance", version=4)],
        pass_threshold=0.5,
    )
    with pytest.raises(
        PluginVersionNotFound, match=f"Plugin {plugin.id} has no version 4"
    ):
        await service.create_replay(
            replay_create(session.id, scoring_policy=pinned), actor=ACTOR
        )
    _, job, _ = await service.create_replay(
        replay_create(session.id, scoring_policy=policy), actor=ACTOR
    )
    assert job.status is JobStatus.PENDING


async def test_get_replay(
    service: ReplayService,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Load a stored replay with its job and config."""
    session = await create_session(session_repository, agent.id)
    created, created_job, created_config = await service.create_replay(
        replay_create(session.id), actor=ACTOR
    )
    replay, job, config = await service.get_replay(created.id, actor=ACTOR)
    assert replay == created
    assert job == created_job
    assert config == created_config


async def test_get_replay_not_found(service: ReplayService) -> None:
    """Raise for an unknown replay id."""
    missing_id = uuid.uuid4()
    with pytest.raises(ReplayNotFound, match=f"Replay {missing_id} was not found"):
        await service.get_replay(missing_id, actor=ACTOR)


async def test_list_replays_filters(
    service: ReplayService,
    repository: FakeReplayRepository,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """List replays filtered by input session and scoring outcome."""
    first = await create_session(session_repository, agent.id)
    second = await create_session(session_repository, agent.id)
    replay_one, _, _ = await service.create_replay(replay_create(first.id), actor=ACTOR)
    await service.create_replay(replay_create(second.id), actor=ACTOR)

    replays, total = await service.list_replays(ReplayFilter(), actor=ACTOR)
    assert total == 2

    replays, total = await service.list_replays(
        ReplayFilter(input_session_id=first.id), actor=ACTOR
    )
    assert total == 1
    assert replays[0][0].id == replay_one.id

    _, total = await service.list_replays(ReplayFilter(passed=True), actor=ACTOR)
    assert total == 0
    replay_one.complete(
        ScoringResult(passed=True, score=0.8, scores={"conciseness": 0.8}), None
    )
    await repository.update(replay_one)
    replays, total = await service.list_replays(ReplayFilter(passed=True), actor=ACTOR)
    assert total == 1
    assert replays[0][0].id == replay_one.id

    replays, total = await service.list_replays(
        ReplayFilter(page=2, page_size=1), actor=ACTOR
    )
    assert total == 2
    assert len(replays) == 1


async def test_compute_diff_requires_result_session(
    service: ReplayService,
    session_repository: FakeSessionRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Raise when the replay's job has no result session yet."""
    session = await create_session(session_repository, agent.id)
    replay, job, _ = await service.create_replay(replay_create(session.id), actor=ACTOR)
    with pytest.raises(
        JobMissingResultSession, match=f"Job {job.id} has no result session"
    ):
        await service.compute_diff(replay.id, actor=ACTOR)


async def test_compute_diff(
    service: ReplayService,
    job_repository: FakeJobRepository,
    session_repository: FakeSessionRepository,
    node_repository: FakeSessionNodeRepository,
    agent: Agent,
    version: AgentVersion,
) -> None:
    """Compute the full diff between input and result session."""
    session = await create_session(session_repository, agent.id)
    await store_tool_result(
        node_repository, session.id, {"city": "Berlin"}, {"temp": 21}
    )
    replay, job, _ = await service.create_replay(
        replay_create(session.id, override=ReplayOverride(model="claude-sonnet-5")),
        actor=ACTOR,
    )
    result = await session_repository.create(
        Session(
            owner_id=ACTOR.account.id,
            agent_id=agent.id,
            origin=SessionOrigin.REPLAY,
            status=SessionStatus.COMPLETED,
        )
    )
    running = await job_repository.get(job.id)
    assert isinstance(running, ReplayJob)
    running.start()
    running.link_result_session(result.id)
    await job_repository.update(running)
    await store_tool_result(
        node_repository, result.id, {"city": "Berlin"}, {"temp": 21}, mocked=True
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
