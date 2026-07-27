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
"""Tests for session use cases."""

import uuid
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from conftest import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeJobRepository,
    FakeReplayConfigRepository,
    FakeSessionNodeRepository,
    FakeSessionRepository,
    FakeTagRepository,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.sessions import (
    SessionCreate,
    SessionFilter,
    SessionUpdate,
)
from kitaru.server.application.services.session_service import SessionService
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent, AgentNotFound
from kitaru.server.domain.agent_version import (
    AgentVersion,
    AgentVersionNotFound,
    RunSpec,
)
from kitaru.server.domain.execution import ExecutionTarget
from kitaru.server.domain.job import (
    JobAlreadyLinked,
    JobNotActive,
    JobNotFound,
    JobStatus,
    Replay,
    SessionRun,
)
from kitaru.server.domain.replay_config import (
    HistoryPolicy,
    ReplayConfig,
    ScoringPolicy,
    SourceRef,
    SourceScorerConfig,
    ToolPolicyConfig,
)
from kitaru.server.domain.session import (
    DuplicateSessionExternalId,
    InvalidSession,
    Session,
    SessionNotFound,
    SessionNotInProgress,
    SessionOrigin,
    SessionProvider,
    SessionStatus,
    TokenUsage,
)
from kitaru.server.domain.session_node import (
    NodeStatus,
    NodeType,
    SessionNode,
)
from kitaru.server.domain.tag import Tag, TagLink, TagResourceType

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))

STARTED_AT = datetime(2026, 7, 1, 12, 0, tzinfo=UTC)
ENDED_AT = datetime(2026, 7, 1, 12, 5, tzinfo=UTC)


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
def tag_repository() -> FakeTagRepository:
    """Provide a fake tag repository."""
    return FakeTagRepository()


@pytest.fixture
def repository(
    agent_repository: FakeAgentRepository,
    version_repository: FakeAgentVersionRepository,
    tag_repository: FakeTagRepository,
) -> FakeSessionRepository:
    """Provide a fake session repository."""
    return FakeSessionRepository(agent_repository, version_repository, tag_repository)


@pytest.fixture
def node_repository(
    repository: FakeSessionRepository,
) -> FakeSessionNodeRepository:
    """Provide a fake session node repository."""
    return FakeSessionNodeRepository(repository)


@pytest.fixture
def config_repository() -> FakeReplayConfigRepository:
    """Provide a fake replay config repository."""
    return FakeReplayConfigRepository()


@pytest.fixture
def job_repository(
    repository: FakeSessionRepository,
    version_repository: FakeAgentVersionRepository,
    config_repository: FakeReplayConfigRepository,
) -> FakeJobRepository:
    """Provide a fake job repository."""
    return FakeJobRepository(repository, version_repository, config_repository)


@pytest.fixture
def service(
    repository: FakeSessionRepository,
    agent_repository: FakeAgentRepository,
    version_repository: FakeAgentVersionRepository,
    node_repository: FakeSessionNodeRepository,
    job_repository: FakeJobRepository,
) -> SessionService:
    """Provide a session service backed by the fake repositories."""
    return SessionService(
        repository=repository,
        agent_repository=agent_repository,
        agent_version_repository=version_repository,
        node_repository=node_repository,
        job_repository=job_repository,
    )


@pytest.fixture
async def agent(agent_repository: FakeAgentRepository) -> Agent:
    """Provide a stored agent."""
    return await agent_repository.create(
        Agent(owner_id=ACTOR.account.id, name="support-bot")
    )


def recorded_command(agent: Agent, **overrides: object) -> SessionCreate:
    """Build a recorded session create command.

    Args:
        agent: Agent the session belongs to.
        **overrides: Field overrides.

    Returns:
        Session create command.
    """
    values: dict[str, object] = {
        "agent_id": agent.id,
        "origin": SessionOrigin.RECORDED,
        "inputs": {"prompt": "hi"},
        "framework": "pydantic_ai",
        "started_at": STARTED_AT,
        **overrides,
    }
    return SessionCreate.model_validate(values)


def imported_command(agent: Agent, **overrides: object) -> SessionCreate:
    """Build an imported session create command.

    Args:
        agent: Agent the session belongs to.
        **overrides: Field overrides.

    Returns:
        Session create command.
    """
    values: dict[str, object] = {
        "agent_id": agent.id,
        "origin": SessionOrigin.IMPORTED,
        "status": SessionStatus.COMPLETED,
        "provider": SessionProvider.LANGFUSE,
        "external_id": "lf-1",
        "outputs": {"answer": "42"},
        **overrides,
    }
    return SessionCreate.model_validate(values)


async def test_create_recorded_session(service: SessionService, agent: Agent) -> None:
    """Create a recorded session opened in progress."""
    session = await service.create_session(recorded_command(agent), actor=ACTOR)
    assert session.owner_id == ACTOR.account.id
    assert session.agent_id == agent.id
    assert session.origin is SessionOrigin.RECORDED
    assert session.status is SessionStatus.IN_PROGRESS
    assert session.inputs == {"prompt": "hi"}
    assert session.framework == "pydantic_ai"
    assert session.started_at == STARTED_AT
    assert session.scores == {}
    assert session.cost is None
    assert session.tokens is None
    assert session.llm_call_count == 0
    assert session.tool_call_count == 0
    assert session.created is not None
    assert session.updated is not None


async def test_create_recorded_session_terminal_status(
    service: SessionService, agent: Agent
) -> None:
    """Reject a recorded session created in a terminal status."""
    with pytest.raises(
        InvalidSession, match="Recorded sessions must be created in progress"
    ):
        await service.create_session(
            recorded_command(agent, status=SessionStatus.COMPLETED), actor=ACTOR
        )


async def test_create_replay_session_without_replay_id(
    service: SessionService, agent: Agent
) -> None:
    """Reject the job origin without a job id."""
    with pytest.raises(
        InvalidSession, match="Session origin 'replay' requires a job id"
    ):
        await service.create_session(
            recorded_command(agent, origin=SessionOrigin.REPLAY), actor=ACTOR
        )


async def test_create_imported_session(service: SessionService, agent: Agent) -> None:
    """Create an imported session already terminal."""
    session = await service.create_session(imported_command(agent), actor=ACTOR)
    assert session.origin is SessionOrigin.IMPORTED
    assert session.status is SessionStatus.COMPLETED
    assert session.provider is SessionProvider.LANGFUSE
    assert session.external_id == "lf-1"
    assert session.outputs == {"answer": "42"}


async def test_create_imported_session_without_provider(
    service: SessionService, agent: Agent
) -> None:
    """Reject an imported session without provider and external id."""
    with pytest.raises(
        InvalidSession, match="Imported sessions require a provider and an external id"
    ):
        await service.create_session(
            imported_command(agent, provider=None), actor=ACTOR
        )


async def test_create_imported_session_in_progress(
    service: SessionService, agent: Agent
) -> None:
    """Reject an imported session without a terminal status."""
    with pytest.raises(InvalidSession, match="Imported sessions cannot be in progress"):
        await service.create_session(imported_command(agent, status=None), actor=ACTOR)


async def test_create_recorded_session_with_provider(
    service: SessionService, agent: Agent
) -> None:
    """Reject a recorded session carrying a provider."""
    with pytest.raises(InvalidSession, match="Only imported sessions carry a provider"):
        await service.create_session(
            recorded_command(agent, provider=SessionProvider.LANGFUSE), actor=ACTOR
        )


async def test_create_session_unknown_agent(service: SessionService) -> None:
    """Raise for an unknown agent id."""
    missing_id = uuid.uuid4()
    command = SessionCreate(agent_id=missing_id, origin=SessionOrigin.RECORDED)
    with pytest.raises(AgentNotFound, match=f"Agent {missing_id} was not found"):
        await service.create_session(command, actor=ACTOR)


async def test_create_session_unknown_agent_version(
    service: SessionService, agent: Agent
) -> None:
    """Raise for an unknown agent version id."""
    missing_id = uuid.uuid4()
    with pytest.raises(
        AgentVersionNotFound, match=f"Agent version {missing_id} was not found"
    ):
        await service.create_session(
            recorded_command(agent, agent_version_id=missing_id), actor=ACTOR
        )


async def test_create_session_version_of_other_agent(
    service: SessionService,
    agent_repository: FakeAgentRepository,
    version_repository: FakeAgentVersionRepository,
    agent: Agent,
) -> None:
    """Reject an agent version that belongs to another agent."""
    other = await agent_repository.create(
        Agent(owner_id=ACTOR.account.id, name="triage-bot")
    )
    version = await version_repository.create(
        AgentVersion(owner_id=ACTOR.account.id, agent_id=other.id, version="v1")
    )
    with pytest.raises(
        InvalidSession,
        match=f"Agent version {version.id} does not belong to agent {agent.id}",
    ):
        await service.create_session(
            recorded_command(agent, agent_version_id=version.id), actor=ACTOR
        )


async def test_create_duplicate_import(service: SessionService, agent: Agent) -> None:
    """Reject a second import with the same provider and external id."""
    await service.create_session(imported_command(agent), actor=ACTOR)
    with pytest.raises(
        DuplicateSessionExternalId,
        match="Session external id 'lf-1' is already registered for provider "
        "'langfuse'",
    ):
        await service.create_session(imported_command(agent), actor=ACTOR)


async def test_get_session(service: SessionService, agent: Agent) -> None:
    """Load a stored session by id."""
    created = await service.create_session(recorded_command(agent), actor=ACTOR)
    loaded = await service.get_session(created.id, actor=ACTOR)
    assert loaded == created


async def test_get_session_not_found(service: SessionService) -> None:
    """Raise for an unknown session id."""
    missing_id = uuid.uuid4()
    with pytest.raises(SessionNotFound, match=f"Session {missing_id} was not found"):
        await service.get_session(missing_id, actor=ACTOR)


async def test_list_sessions(service: SessionService, agent: Agent) -> None:
    """List sessions with filters and pagination."""
    for name in ["one", "two", "three"]:
        await service.create_session(recorded_command(agent, name=name), actor=ACTOR)
    await service.create_session(imported_command(agent), actor=ACTOR)

    sessions, total = await service.list_sessions(SessionFilter(), actor=ACTOR)
    assert total == 4

    sessions, total = await service.list_sessions(
        SessionFilter(origin=SessionOrigin.RECORDED), actor=ACTOR
    )
    assert total == 3
    assert [session.name for session in sessions] == ["one", "two", "three"]

    sessions, total = await service.list_sessions(
        SessionFilter(origin=SessionOrigin.RECORDED, page=2, page_size=2), actor=ACTOR
    )
    assert total == 3
    assert [session.name for session in sessions] == ["three"]

    sessions, total = await service.list_sessions(
        SessionFilter(name="two"), actor=ACTOR
    )
    assert total == 1

    sessions, total = await service.list_sessions(
        SessionFilter(provider=SessionProvider.LANGFUSE, external_id="lf-1"),
        actor=ACTOR,
    )
    assert total == 1
    assert sessions[0].origin is SessionOrigin.IMPORTED


async def test_list_sessions_by_tag(
    service: SessionService,
    tag_repository: FakeTagRepository,
    agent: Agent,
) -> None:
    """List sessions attached to a tag name."""
    tagged = await service.create_session(recorded_command(agent), actor=ACTOR)
    await service.create_session(recorded_command(agent), actor=ACTOR)
    tag = await tag_repository.create(Tag(owner_id=ACTOR.account.id, name="prod"))
    await tag_repository.create_link(
        TagLink(
            tag_id=tag.id,
            resource_type=TagResourceType.SESSION,
            resource_id=tagged.id,
        )
    )

    sessions, total = await service.list_sessions(
        SessionFilter(tag="prod"), actor=ACTOR
    )
    assert total == 1
    assert sessions[0].id == tagged.id

    sessions, total = await service.list_sessions(
        SessionFilter(tag="missing"), actor=ACTOR
    )
    assert total == 0


async def test_list_sessions_unknown_agent(service: SessionService) -> None:
    """Raise for an unknown agent id."""
    missing_id = uuid.uuid4()
    with pytest.raises(AgentNotFound, match=f"Agent {missing_id} was not found"):
        await service.list_sessions(SessionFilter(agent_id=missing_id), actor=ACTOR)


async def test_update_session_fields(service: SessionService, agent: Agent) -> None:
    """Update name, expected, and metadata without finishing."""
    created = await service.create_session(recorded_command(agent), actor=ACTOR)
    updated = await service.update_session(
        created.id,
        SessionUpdate(
            name="run-1", expected={"answer": "42"}, metadata={"env": "prod"}
        ),
        actor=ACTOR,
    )
    assert updated.name == "run-1"
    assert updated.expected == {"answer": "42"}
    assert updated.metadata == {"env": "prod"}
    assert updated.status is SessionStatus.IN_PROGRESS
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated > created.updated


async def test_update_session_absent_fields_unchanged(
    service: SessionService, agent: Agent
) -> None:
    """Keep every field on an update without set fields."""
    created = await service.create_session(
        recorded_command(
            agent,
            name="run-1",
            expected={"answer": "42"},
            metadata={"env": "prod"},
        ),
        actor=ACTOR,
    )
    updated = await service.update_session(created.id, SessionUpdate(), actor=ACTOR)
    assert updated.name == "run-1"
    assert updated.expected == {"answer": "42"}
    assert updated.metadata == {"env": "prod"}
    assert updated.status is SessionStatus.IN_PROGRESS


async def test_update_session_null_clears_fields(
    service: SessionService, agent: Agent
) -> None:
    """Clear name and expected and reset metadata on explicit nulls."""
    created = await service.create_session(
        recorded_command(
            agent,
            name="run-1",
            expected={"answer": "42"},
            metadata={"env": "prod"},
        ),
        actor=ACTOR,
    )
    updated = await service.update_session(
        created.id,
        SessionUpdate(name=None, expected=None, metadata=None),
        actor=ACTOR,
    )
    assert updated.name is None
    assert updated.expected is None
    assert updated.metadata == {}


async def test_update_session_null_status_rejected(
    service: SessionService, agent: Agent
) -> None:
    """Reject an explicit null for the status."""
    created = await service.create_session(recorded_command(agent), actor=ACTOR)
    with pytest.raises(InvalidSession, match="Session status cannot be null"):
        await service.update_session(
            created.id, SessionUpdate(status=None), actor=ACTOR
        )


async def test_update_fields_on_terminal_session(
    service: SessionService, agent: Agent
) -> None:
    """Update name, expected, and metadata on a terminal session."""
    created = await service.create_session(imported_command(agent), actor=ACTOR)
    updated = await service.update_session(
        created.id, SessionUpdate(name="import-1"), actor=ACTOR
    )
    assert updated.name == "import-1"
    assert updated.status is SessionStatus.COMPLETED


async def test_finish_session_computes_rollups(
    service: SessionService,
    node_repository: FakeSessionNodeRepository,
    agent: Agent,
) -> None:
    """Finish a session and roll up cost, tokens, and call counts."""
    created = await service.create_session(recorded_command(agent), actor=ACTOR)
    await node_repository.upsert(
        [
            SessionNode(
                session_id=created.id,
                key="llm_call:chat",
                sequence=0,
                node_type=NodeType.LLM_CALL,
                name="chat",
                status=NodeStatus.COMPLETED,
                tokens=TokenUsage(input_tokens=100, output_tokens=20),
                cost=Decimal("0.5"),
            ),
            SessionNode(
                session_id=created.id,
                key="llm_call:chat#2",
                sequence=1,
                node_type=NodeType.LLM_CALL,
                name="chat",
                status=NodeStatus.COMPLETED,
                tokens=TokenUsage(input_tokens=50, reasoning_tokens=10),
                cost=Decimal("0.25"),
            ),
            SessionNode(
                session_id=created.id,
                key="tool_call:get_weather",
                sequence=2,
                node_type=NodeType.TOOL_CALL,
                name="get_weather",
                status=NodeStatus.COMPLETED,
            ),
            SessionNode(
                session_id=created.id,
                key="span:setup",
                sequence=3,
                node_type=NodeType.SPAN,
                name="setup",
                status=NodeStatus.COMPLETED,
            ),
        ]
    )
    finished = await service.update_session(
        created.id,
        SessionUpdate(
            status=SessionStatus.COMPLETED,
            outputs={"answer": "sunny"},
            ended_at=ENDED_AT,
            log_uri="s3://logs/run-1",
        ),
        actor=ACTOR,
    )
    assert finished.status is SessionStatus.COMPLETED
    assert finished.outputs == {"answer": "sunny"}
    assert finished.ended_at == ENDED_AT
    assert finished.log_uri == "s3://logs/run-1"
    assert finished.cost == Decimal("0.75")
    assert finished.tokens == TokenUsage(
        input_tokens=150, output_tokens=20, reasoning_tokens=10
    )
    assert finished.llm_call_count == 2
    assert finished.tool_call_count == 1


async def test_finish_session_without_nodes(
    service: SessionService, agent: Agent
) -> None:
    """Finish a session with no nodes and empty rollups."""
    created = await service.create_session(recorded_command(agent), actor=ACTOR)
    finished = await service.update_session(
        created.id,
        SessionUpdate(status=SessionStatus.FAILED, error="boom"),
        actor=ACTOR,
    )
    assert finished.status is SessionStatus.FAILED
    assert finished.error == "boom"
    assert finished.cost is None
    assert finished.tokens is None
    assert finished.llm_call_count == 0
    assert finished.tool_call_count == 0


async def test_finish_terminal_session(service: SessionService, agent: Agent) -> None:
    """Reject finishing a session that is not in progress."""
    created = await service.create_session(recorded_command(agent), actor=ACTOR)
    await service.update_session(
        created.id, SessionUpdate(status=SessionStatus.COMPLETED), actor=ACTOR
    )
    with pytest.raises(
        SessionNotInProgress, match=f"Session {created.id} is not in progress"
    ):
        await service.update_session(
            created.id, SessionUpdate(status=SessionStatus.FAILED), actor=ACTOR
        )


async def test_finish_with_in_progress_status(
    service: SessionService, agent: Agent
) -> None:
    """Reject finishing with a non-terminal status."""
    created = await service.create_session(recorded_command(agent), actor=ACTOR)
    with pytest.raises(
        InvalidSession, match="Session finish requires a terminal status"
    ):
        await service.update_session(
            created.id, SessionUpdate(status=SessionStatus.IN_PROGRESS), actor=ACTOR
        )


async def test_update_session_not_found(service: SessionService) -> None:
    """Raise for an unknown session id."""
    with pytest.raises(SessionNotFound):
        await service.update_session(uuid.uuid4(), SessionUpdate(name="x"), actor=ACTOR)


async def test_merge_scores(service: SessionService, agent: Agent) -> None:
    """Merge score values with latest wins per scorer name."""
    created = await service.create_session(recorded_command(agent), actor=ACTOR)
    updated = await service.merge_scores(
        created.id, {"conciseness": 0.5, "accuracy": 0.9}, actor=ACTOR
    )
    assert updated.scores == {"conciseness": 0.5, "accuracy": 0.9}
    updated = await service.merge_scores(
        created.id, {"conciseness": 0.7, "tone": 1.0}, actor=ACTOR
    )
    assert updated.scores == {"conciseness": 0.7, "accuracy": 0.9, "tone": 1.0}


async def test_merge_scores_not_found(service: SessionService) -> None:
    """Raise for an unknown session id."""
    with pytest.raises(SessionNotFound):
        await service.merge_scores(uuid.uuid4(), {"a": 1.0}, actor=ACTOR)


async def test_delete_session(
    service: SessionService,
    node_repository: FakeSessionNodeRepository,
    tag_repository: FakeTagRepository,
    agent: Agent,
) -> None:
    """Delete a session with its nodes and tag links."""
    created = await service.create_session(recorded_command(agent), actor=ACTOR)
    await node_repository.upsert(
        [
            SessionNode(
                session_id=created.id,
                key="span:run",
                sequence=0,
                node_type=NodeType.SPAN,
                name="run",
                status=NodeStatus.COMPLETED,
            )
        ]
    )
    tag = await tag_repository.create(Tag(owner_id=ACTOR.account.id, name="prod"))
    await tag_repository.create_link(
        TagLink(
            tag_id=tag.id,
            resource_type=TagResourceType.SESSION,
            resource_id=created.id,
        )
    )

    await service.delete_session(created.id, actor=ACTOR)
    with pytest.raises(SessionNotFound):
        await service.get_session(created.id, actor=ACTOR)
    assert (
        await node_repository.list_for_session(created.id, include_payloads=True) == []
    )
    assert tag_repository.linked_resource_ids("prod", TagResourceType.SESSION) == set()


async def test_delete_session_not_found(service: SessionService) -> None:
    """Raise for an unknown session id."""
    with pytest.raises(SessionNotFound):
        await service.delete_session(uuid.uuid4(), actor=ACTOR)


async def create_running_job(
    repository: FakeSessionRepository,
    version_repository: FakeAgentVersionRepository,
    config_repository: FakeReplayConfigRepository,
    job_repository: FakeJobRepository,
    agent: Agent,
    status: JobStatus = JobStatus.RUNNING,
) -> Replay:
    """Store a job of a completed session in a given status.

    Args:
        repository: Fake session repository.
        version_repository: Fake agent version repository.
        config_repository: Fake replay config repository.
        job_repository: Fake job repository.
        agent: Agent of the session.
        status: Job status.

    Returns:
        Stored job.
    """
    version = await version_repository.create(
        AgentVersion(
            owner_id=ACTOR.account.id,
            agent_id=agent.id,
            version=f"v-{uuid.uuid4().hex[:8]}",
            run_spec=RunSpec(command="python agent.py", timeout_seconds=600),
        )
    )
    original = await repository.create(
        Session(
            owner_id=ACTOR.account.id,
            agent_id=agent.id,
            origin=SessionOrigin.RECORDED,
            status=SessionStatus.COMPLETED,
        )
    )
    config = await config_repository.create(
        ReplayConfig(
            owner_id=ACTOR.account.id,
            tool_policy=ToolPolicyConfig(default=HistoryPolicy()),
            scoring_policy=ScoringPolicy(
                scorers=[
                    SourceScorerConfig(
                        name="conciseness",
                        source=SourceRef(
                            module="my_pkg.scorers", attribute="conciseness"
                        ),
                    )
                ],
                pass_threshold=0.5,
            ),
        )
    )
    job = await job_repository.create(
        Replay(
            replay_config_id=config.id,
            agent_version_id=version.id,
            input_session_id=original.id,
            status=status,
            execution_target=ExecutionTarget.POOL,
        )
    )
    assert isinstance(job, Replay)
    return job


async def test_create_session_links_job(
    service: SessionService,
    repository: FakeSessionRepository,
    version_repository: FakeAgentVersionRepository,
    config_repository: FakeReplayConfigRepository,
    job_repository: FakeJobRepository,
    agent: Agent,
) -> None:
    """Link a created session to its job and rewrite the origin."""
    job = await create_running_job(
        repository, version_repository, config_repository, job_repository, agent
    )
    session = await service.create_session(
        recorded_command(agent, job_id=job.id), actor=ACTOR
    )
    assert session.origin is SessionOrigin.REPLAY
    assert session.status is SessionStatus.IN_PROGRESS
    linked = await job_repository.get(job.id)
    assert linked.result_session_id == session.id


async def test_create_session_links_claimed_job(
    service: SessionService,
    repository: FakeSessionRepository,
    version_repository: FakeAgentVersionRepository,
    config_repository: FakeReplayConfigRepository,
    job_repository: FakeJobRepository,
    agent: Agent,
) -> None:
    """Accept the link while the job is still claimed."""
    job = await create_running_job(
        repository,
        version_repository,
        config_repository,
        job_repository,
        agent,
        status=JobStatus.CLAIMED,
    )
    session = await service.create_session(
        recorded_command(agent, job_id=job.id), actor=ACTOR
    )
    linked = await job_repository.get(job.id)
    assert linked.result_session_id == session.id


async def test_create_session_link_requires_active_job(
    service: SessionService,
    repository: FakeSessionRepository,
    version_repository: FakeAgentVersionRepository,
    config_repository: FakeReplayConfigRepository,
    job_repository: FakeJobRepository,
    agent: Agent,
) -> None:
    """Reject linking a job that is not claimed or running."""
    job = await create_running_job(
        repository,
        version_repository,
        config_repository,
        job_repository,
        agent,
        status=JobStatus.PENDING,
    )
    with pytest.raises(JobNotActive, match=f"Job {job.id} is not claimed or running"):
        await service.create_session(
            recorded_command(agent, job_id=job.id), actor=ACTOR
        )
    # The failed link stores no session.
    _, total = await service.list_sessions(SessionFilter(), actor=ACTOR)
    assert total == 1


async def test_create_session_link_rejects_linked_job(
    service: SessionService,
    repository: FakeSessionRepository,
    version_repository: FakeAgentVersionRepository,
    config_repository: FakeReplayConfigRepository,
    job_repository: FakeJobRepository,
    agent: Agent,
) -> None:
    """Reject linking a job that already has a result session."""
    job = await create_running_job(
        repository, version_repository, config_repository, job_repository, agent
    )
    await service.create_session(recorded_command(agent, job_id=job.id), actor=ACTOR)
    with pytest.raises(
        JobAlreadyLinked,
        match=f"Job {job.id} already has a result session",
    ):
        await service.create_session(
            recorded_command(agent, job_id=job.id), actor=ACTOR
        )


async def test_create_session_link_unknown_job(
    service: SessionService, agent: Agent
) -> None:
    """Raise for an unknown job id."""
    missing_id = uuid.uuid4()
    with pytest.raises(JobNotFound, match=f"Job {missing_id} was not found"):
        await service.create_session(
            recorded_command(agent, job_id=missing_id), actor=ACTOR
        )


async def test_create_session_link_requires_recorded_origin(
    service: SessionService,
    repository: FakeSessionRepository,
    version_repository: FakeAgentVersionRepository,
    config_repository: FakeReplayConfigRepository,
    job_repository: FakeJobRepository,
    agent: Agent,
) -> None:
    """Reject a job link on a non-recorded origin."""
    job = await create_running_job(
        repository, version_repository, config_repository, job_repository, agent
    )
    with pytest.raises(
        InvalidSession, match="Sessions linked to a job require origin 'recorded'"
    ):
        await service.create_session(
            imported_command(agent, job_id=job.id), actor=ACTOR
        )


async def test_create_session_links_session_run_keeps_origin(
    service: SessionService,
    repository: FakeSessionRepository,
    version_repository: FakeAgentVersionRepository,
    job_repository: FakeJobRepository,
    agent: Agent,
) -> None:
    """Link a created session to a session run and keep origin recorded."""
    version = await version_repository.create(
        AgentVersion(
            owner_id=ACTOR.account.id,
            agent_id=agent.id,
            version="v1",
            run_spec=RunSpec(command="python agent.py", timeout_seconds=600),
        )
    )
    job = await job_repository.create(
        SessionRun(
            agent_version_id=version.id,
            status=JobStatus.RUNNING,
            execution_target=ExecutionTarget.POOL,
        )
    )
    session = await service.create_session(
        recorded_command(agent, job_id=job.id), actor=ACTOR
    )
    assert session.origin is SessionOrigin.RECORDED
    linked = await job_repository.get(job.id)
    assert linked.result_session_id == session.id
