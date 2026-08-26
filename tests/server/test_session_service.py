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
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from conftest import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeBlobDataStore,
    FakeBlobRepository,
    FakeReplayRepository,
    FakeSessionRepository,
    FakeTaskRepository,
    build_payload_store,
    create_agent,
    create_agent_task,
    create_agent_version,
    create_import_task,
    create_replay,
    create_session,
)
from kitaru.analytics.events import AnalyticsEvent
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.session import SessionOrigin, SessionStatus, TokenUsage
from kitaru.server.application.models.auth import (
    AuthContext,
    GrantKind,
    TaskPrincipal,
)
from kitaru.server.application.models.session import (
    SessionCreate,
    SessionFilter,
    SessionUpdate,
)
from kitaru.server.application.services.server_analytics import ServerAnalytics
from kitaru.server.application.services.session_service import SessionService
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent_version import (
    AgentVersion,
    AgentVersionAgentMismatch,
    AgentVersionNotFound,
)
from kitaru.server.domain.blob import BlobStorageBackend
from kitaru.server.domain.payload import PayloadMediaType
from kitaru.server.domain.replay import Replay
from kitaru.server.domain.session import (
    Session,
    SessionAccessDenied,
    SessionAgentMismatch,
    SessionAgentRequired,
    SessionAgentVersionMismatch,
    SessionBaselineNotFound,
    SessionInUse,
    SessionNotFound,
    SessionNotUpdatable,
    SessionStatusCannotBeCleared,
)
from kitaru.server.domain.task import (
    AgentTask,
    Task,
    TaskAttemptMismatch,
    TaskNotFound,
    TaskNotRunning,
    TaskResultSessionAlreadyLinked,
)
from kitaru.server.filtering import FilterCondition

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))


class _RecordingAnalytics(ServerAnalytics):
    """Analytics tracker recording track calls instead of buffering them."""

    def __init__(self) -> None:
        """Initialize the recorder."""
        self.tracked: list[tuple[uuid.UUID, str, dict[str, Any]]] = []

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
def repository() -> FakeSessionRepository:
    """Provide a fake session repository."""
    return FakeSessionRepository()


@pytest.fixture
def task_repository() -> FakeTaskRepository:
    """Provide a fake task repository."""
    return FakeTaskRepository()


@pytest.fixture
def agent_repository() -> FakeAgentRepository:
    """Provide a fake agent repository."""
    return FakeAgentRepository()


@pytest.fixture
def agent_version_repository(
    agent_repository: FakeAgentRepository,
) -> FakeAgentVersionRepository:
    """Provide a fake agent version repository sharing the agent repository."""
    return FakeAgentVersionRepository(agent_repository)


@pytest.fixture
def replay_repository(repository: FakeSessionRepository) -> FakeReplayRepository:
    """Provide a fake replay repository sharing the session repository."""
    return FakeReplayRepository(sessions=repository)


@pytest.fixture
def service(
    repository: FakeSessionRepository,
    task_repository: FakeTaskRepository,
    agent_version_repository: FakeAgentVersionRepository,
    replay_repository: FakeReplayRepository,
) -> SessionService:
    """Provide a session service backed by the fake repositories."""
    return SessionService(
        repository=repository,
        task_repository=task_repository,
        agent_version_repository=agent_version_repository,
        replay_repository=replay_repository,
        payload_store=build_payload_store().store,
    )


async def _stored_agent_version(
    agent_repository: FakeAgentRepository,
    agent_version_repository: FakeAgentVersionRepository,
    name: str = "assistant",
) -> AgentVersion:
    """Store an agent and one version of it."""
    agent = await create_agent(agent_repository, ACTOR.account.id, name=name)
    return await create_agent_version(
        agent_version_repository, agent.id, ACTOR.account.id
    )


async def _start(task_repository: FakeTaskRepository, task: Task) -> Task:
    """Claim and start a stored task."""
    task.claim(uuid.uuid4(), datetime.now(UTC))
    task.start(datetime.now(UTC))
    return await task_repository.update(task)


async def _running_agent_task(
    task_repository: FakeTaskRepository, agent_version_id: uuid.UUID | None = None
) -> Task:
    """Store an agent task claimed by a worker and running."""
    task = await create_agent_task(
        task_repository, uuid.uuid4(), agent_version_id=agent_version_id
    )
    return await _start(task_repository, task)


async def test_create_session_defaults_status_in_progress(
    service: SessionService,
) -> None:
    """Default a session with no status to in_progress."""
    agent_id = uuid.uuid4()
    session = await service.create_session(
        SessionCreate(agent_id=agent_id, origin=SessionOrigin.RECORDED),
        actor=ACTOR,
    )
    assert session.agent_id == agent_id
    assert session.owner_id == ACTOR.account.id
    assert session.status == SessionStatus.IN_PROGRESS
    assert session.created is not None
    assert session.updated is not None


async def test_create_session_numbers_sessions_per_agent(
    service: SessionService,
) -> None:
    """Number an agent's sessions sequentially, each agent counting alone."""
    agent_id = uuid.uuid4()
    other_agent_id = uuid.uuid4()
    first = await service.create_session(
        SessionCreate(agent_id=agent_id, origin=SessionOrigin.RECORDED),
        actor=ACTOR,
    )
    second = await service.create_session(
        SessionCreate(agent_id=agent_id, origin=SessionOrigin.RECORDED),
        actor=ACTOR,
    )
    other = await service.create_session(
        SessionCreate(agent_id=other_agent_id, origin=SessionOrigin.RECORDED),
        actor=ACTOR,
    )
    assert first.number == 1
    assert second.number == 2
    assert other.number == 1


async def test_create_session_honors_explicit_status(
    service: SessionService,
) -> None:
    """Store an explicit initial status, for example an imported session."""
    session = await service.create_session(
        SessionCreate(
            agent_id=uuid.uuid4(),
            origin=SessionOrigin.IMPORTED,
            status=SessionStatus.COMPLETED,
        ),
        actor=ACTOR,
    )
    assert session.status == SessionStatus.COMPLETED


async def test_get_session(service: SessionService) -> None:
    """Load a stored session by id."""
    created = await service.create_session(
        SessionCreate(agent_id=uuid.uuid4(), origin=SessionOrigin.RECORDED),
        actor=ACTOR,
    )
    loaded = await service.get_session(created.id, actor=ACTOR)
    assert loaded == created


async def test_get_session_not_found(service: SessionService) -> None:
    """Raise for an unknown session id."""
    missing_id = uuid.uuid4()
    with pytest.raises(SessionNotFound, match=f"Session {missing_id} was not found"):
        await service.get_session(missing_id, actor=ACTOR)


async def _replayed_session(
    repository: FakeSessionRepository,
    task_repository: FakeTaskRepository,
    replay_repository: FakeReplayRepository,
) -> tuple[Session, Session]:
    """Store a baseline session and the replay result session produced from it."""
    baseline = await create_session(repository, ACTOR.account.id, uuid.uuid4())
    job_id = uuid.uuid4()
    task = await create_agent_task(task_repository, job_id)
    result = await create_session(
        repository, ACTOR.account.id, uuid.uuid4(), task_id=task.id
    )
    await replay_repository.create(
        Replay(
            owner_id=ACTOR.account.id,
            job_id=job_id,
            replay_config_id=uuid.uuid4(),
            baseline_session_id=baseline.id,
            result_session_id=result.id,
        )
    )
    return baseline, result


async def test_get_baseline_session(
    service: SessionService,
    repository: FakeSessionRepository,
    task_repository: FakeTaskRepository,
    replay_repository: FakeReplayRepository,
) -> None:
    """Resolve a replay result session to the baseline the replay ran against."""
    baseline, result = await _replayed_session(
        repository, task_repository, replay_repository
    )
    loaded = await service.get_baseline_session(result.id, actor=ACTOR)
    assert loaded.id == baseline.id


async def test_get_baseline_session_after_the_task_link_is_cleared(
    service: SessionService,
    repository: FakeSessionRepository,
    task_repository: FakeTaskRepository,
    replay_repository: FakeReplayRepository,
) -> None:
    """Resolve the baseline through the replay after a requeue unlinked the task."""
    baseline, result = await _replayed_session(
        repository, task_repository, replay_repository
    )
    result.unlink_task()
    await repository.update(result)
    loaded = await service.get_baseline_session(result.id, actor=ACTOR)
    assert loaded.id == baseline.id


async def test_get_baseline_session_without_a_task(
    service: SessionService, repository: FakeSessionRepository
) -> None:
    """Raise for a session that no task produced."""
    session = await create_session(repository, ACTOR.account.id, uuid.uuid4())
    with pytest.raises(
        SessionBaselineNotFound,
        match=f"Session {session.id} was not produced by a replay",
    ):
        await service.get_baseline_session(session.id, actor=ACTOR)


async def test_get_baseline_session_outside_a_replay_job(
    service: SessionService,
    repository: FakeSessionRepository,
    task_repository: FakeTaskRepository,
) -> None:
    """Raise for a session whose task belongs to a job that holds no replay."""
    task = await create_agent_task(task_repository, uuid.uuid4())
    session = await create_session(
        repository, ACTOR.account.id, uuid.uuid4(), task_id=task.id
    )
    with pytest.raises(SessionBaselineNotFound):
        await service.get_baseline_session(session.id, actor=ACTOR)


async def test_get_baseline_session_not_found(service: SessionService) -> None:
    """Raise for an unknown session id."""
    missing_id = uuid.uuid4()
    with pytest.raises(SessionNotFound):
        await service.get_baseline_session(missing_id, actor=ACTOR)


async def test_list_sessions_scoped_by_agent(service: SessionService) -> None:
    """List only the sessions of the requested agent."""
    agent_id = uuid.uuid4()
    other_agent_id = uuid.uuid4()
    first = await service.create_session(
        SessionCreate(agent_id=agent_id, origin=SessionOrigin.RECORDED), actor=ACTOR
    )
    second = await service.create_session(
        SessionCreate(agent_id=agent_id, origin=SessionOrigin.RECORDED), actor=ACTOR
    )
    await service.create_session(
        SessionCreate(agent_id=other_agent_id, origin=SessionOrigin.RECORDED),
        actor=ACTOR,
    )

    sessions, next_cursor = await service.list_sessions(
        SessionFilter(
            expression=FilterCondition(field="agent_id", op=FilterOp.EQ, value=agent_id)
        ),
        actor=ACTOR,
    )
    assert next_cursor is None
    assert {session.id for session in sessions} == {first.id, second.id}


async def test_update_session_clears_outputs_with_explicit_null(
    service: SessionService,
) -> None:
    """Clear outputs with an explicit null passed alongside status."""
    created = await service.create_session(
        SessionCreate(
            agent_id=uuid.uuid4(),
            origin=SessionOrigin.RECORDED,
            outputs={"answer": 42},
        ),
        actor=ACTOR,
    )
    updated = await service.update_session(
        created.id,
        SessionUpdate(status=SessionStatus.COMPLETED, outputs=None),
        actor=ACTOR,
    )
    assert updated.outputs is None
    assert updated.status == SessionStatus.COMPLETED


async def test_update_session_omitted_fields_unchanged(
    service: SessionService,
) -> None:
    """Leave outputs unchanged when the command omits them."""
    created = await service.create_session(
        SessionCreate(
            agent_id=uuid.uuid4(),
            origin=SessionOrigin.RECORDED,
            outputs={"answer": 42},
        ),
        actor=ACTOR,
    )
    updated = await service.update_session(
        created.id, SessionUpdate(name="renamed"), actor=ACTOR
    )
    assert updated.outputs is not None
    assert updated.outputs.value == {"answer": 42}
    assert updated.name == "renamed"
    assert updated.status == SessionStatus.IN_PROGRESS


async def test_update_session_metadata_replaced_whole(
    service: SessionService,
) -> None:
    """Replace metadata whole rather than merging keys."""
    created = await service.create_session(
        SessionCreate(
            agent_id=uuid.uuid4(),
            origin=SessionOrigin.RECORDED,
            metadata={"a": 1, "b": 2},
        ),
        actor=ACTOR,
    )
    updated = await service.update_session(
        created.id, SessionUpdate(metadata={"c": 3}), actor=ACTOR
    )
    assert updated.metadata == {"c": 3}


async def test_update_session_metadata_null_clears(service: SessionService) -> None:
    """Clear metadata to an empty dict with an explicit null."""
    created = await service.create_session(
        SessionCreate(
            agent_id=uuid.uuid4(), origin=SessionOrigin.RECORDED, metadata={"a": 1}
        ),
        actor=ACTOR,
    )
    updated = await service.update_session(
        created.id, SessionUpdate(metadata=None), actor=ACTOR
    )
    assert updated.metadata == {}


async def test_update_session_status_transition_completes_session(
    service: SessionService,
) -> None:
    """Move an in_progress session to completed via the update endpoint."""
    created = await service.create_session(
        SessionCreate(agent_id=uuid.uuid4(), origin=SessionOrigin.RECORDED),
        actor=ACTOR,
    )
    updated = await service.update_session(
        created.id,
        SessionUpdate(status=SessionStatus.COMPLETED, error=None),
        actor=ACTOR,
    )
    assert updated.status == SessionStatus.COMPLETED
    assert updated.ended_at is None


async def test_update_session_status_cannot_be_cleared(
    service: SessionService,
) -> None:
    """Reject an explicit null status."""
    created = await service.create_session(
        SessionCreate(agent_id=uuid.uuid4(), origin=SessionOrigin.RECORDED),
        actor=ACTOR,
    )
    with pytest.raises(SessionStatusCannotBeCleared):
        await service.update_session(
            created.id, SessionUpdate(status=None), actor=ACTOR
        )


async def test_update_session_rejects_terminal_back_to_in_progress(
    service: SessionService,
) -> None:
    """Reject moving a terminal session back to in_progress."""
    created = await service.create_session(
        SessionCreate(agent_id=uuid.uuid4(), origin=SessionOrigin.RECORDED),
        actor=ACTOR,
    )
    await service.update_session(
        created.id, SessionUpdate(status=SessionStatus.FAILED), actor=ACTOR
    )
    with pytest.raises(SessionNotUpdatable):
        await service.update_session(
            created.id, SessionUpdate(status=SessionStatus.IN_PROGRESS), actor=ACTOR
        )


async def test_update_session_rejects_terminal_to_other_terminal(
    service: SessionService,
) -> None:
    """Reject moving a terminal session to another terminal status."""
    created = await service.create_session(
        SessionCreate(agent_id=uuid.uuid4(), origin=SessionOrigin.RECORDED),
        actor=ACTOR,
    )
    await service.update_session(
        created.id, SessionUpdate(status=SessionStatus.COMPLETED), actor=ACTOR
    )
    with pytest.raises(SessionNotUpdatable):
        await service.update_session(
            created.id, SessionUpdate(status=SessionStatus.FAILED), actor=ACTOR
        )


async def test_update_session_rejects_any_update_on_finished_session(
    service: SessionService,
) -> None:
    """Reject a non-status update on a finished session."""
    created = await service.create_session(
        SessionCreate(agent_id=uuid.uuid4(), origin=SessionOrigin.RECORDED),
        actor=ACTOR,
    )
    await service.update_session(
        created.id, SessionUpdate(status=SessionStatus.COMPLETED), actor=ACTOR
    )
    with pytest.raises(SessionNotUpdatable):
        await service.update_session(
            created.id, SessionUpdate(name="renamed"), actor=ACTOR
        )


async def test_update_session_transition_to_terminal_tracks_analytics_event(
    repository: FakeSessionRepository,
    task_repository: FakeTaskRepository,
    agent_version_repository: FakeAgentVersionRepository,
) -> None:
    """Track a session_completed event when a session turns terminal."""
    analytics = _RecordingAnalytics()
    service = SessionService(
        repository=repository,
        task_repository=task_repository,
        agent_version_repository=agent_version_repository,
        replay_repository=FakeReplayRepository(),
        payload_store=build_payload_store().store,
        analytics=analytics,
    )
    started_at = datetime.now(UTC)
    created = await create_session(
        repository,
        owner_id=ACTOR.account.id,
        agent_id=uuid.uuid4(),
        origin=SessionOrigin.RECORDED,
        started_at=started_at,
        tokens=TokenUsage(input_tokens=100, output_tokens=50),
        framework="langgraph",
        adapter_version="0.1.0",
        llm_call_count=3,
        tool_call_count=5,
    )
    ended_at = started_at + timedelta(seconds=30)
    await service.update_session(
        created.id,
        SessionUpdate(status=SessionStatus.COMPLETED, ended_at=ended_at),
        actor=ACTOR,
    )

    assert len(analytics.tracked) == 1
    tracked_user_id, tracked_event, tracked_properties = analytics.tracked[0]
    assert tracked_user_id == ACTOR.account.id
    assert tracked_event == AnalyticsEvent.SESSION_COMPLETED
    assert tracked_properties == {
        "origin": "recorded",
        "status": "completed",
        "duration_seconds": 30.0,
        "framework": "langgraph",
        "adapter_version": "0.1.0",
        "llm_call_count": 3,
        "tool_call_count": 5,
        "input_tokens": 100,
        "output_tokens": 50,
    }


async def test_update_session_non_status_update_tracks_nothing(
    repository: FakeSessionRepository,
    task_repository: FakeTaskRepository,
    agent_version_repository: FakeAgentVersionRepository,
) -> None:
    """Skip tracking when the update touches no status-related field."""
    analytics = _RecordingAnalytics()
    service = SessionService(
        repository=repository,
        task_repository=task_repository,
        agent_version_repository=agent_version_repository,
        replay_repository=FakeReplayRepository(),
        payload_store=build_payload_store().store,
        analytics=analytics,
    )
    created = await service.create_session(
        SessionCreate(agent_id=uuid.uuid4(), origin=SessionOrigin.RECORDED),
        actor=ACTOR,
    )
    await service.update_session(created.id, SessionUpdate(name="renamed"), actor=ACTOR)

    assert analytics.tracked == []


async def test_create_session_with_terminal_status_tracks_analytics_event(
    repository: FakeSessionRepository,
    task_repository: FakeTaskRepository,
    agent_version_repository: FakeAgentVersionRepository,
) -> None:
    """Track a session_completed event when a session is created terminal."""
    analytics = _RecordingAnalytics()
    service = SessionService(
        repository=repository,
        task_repository=task_repository,
        agent_version_repository=agent_version_repository,
        replay_repository=FakeReplayRepository(),
        payload_store=build_payload_store().store,
        analytics=analytics,
    )
    created = await service.create_session(
        SessionCreate(
            agent_id=uuid.uuid4(),
            origin=SessionOrigin.IMPORTED,
            status=SessionStatus.FAILED,
        ),
        actor=ACTOR,
    )

    assert len(analytics.tracked) == 1
    tracked_user_id, tracked_event, tracked_properties = analytics.tracked[0]
    assert tracked_user_id == created.owner_id
    assert tracked_event == AnalyticsEvent.SESSION_COMPLETED
    assert tracked_properties == {
        "origin": "imported",
        "status": "failed",
        "llm_call_count": 0,
        "tool_call_count": 0,
    }


async def test_create_session_in_progress_tracks_nothing(
    repository: FakeSessionRepository,
    task_repository: FakeTaskRepository,
    agent_version_repository: FakeAgentVersionRepository,
) -> None:
    """Skip tracking when a session is created in progress."""
    analytics = _RecordingAnalytics()
    service = SessionService(
        repository=repository,
        task_repository=task_repository,
        agent_version_repository=agent_version_repository,
        replay_repository=FakeReplayRepository(),
        payload_store=build_payload_store().store,
        analytics=analytics,
    )
    await service.create_session(
        SessionCreate(agent_id=uuid.uuid4(), origin=SessionOrigin.RECORDED),
        actor=ACTOR,
    )

    assert analytics.tracked == []


async def test_update_session_transition_with_analytics_none_is_safe(
    service: SessionService,
) -> None:
    """Transition a session to terminal without an analytics tracker configured."""
    created = await service.create_session(
        SessionCreate(agent_id=uuid.uuid4(), origin=SessionOrigin.RECORDED),
        actor=ACTOR,
    )
    updated = await service.update_session(
        created.id, SessionUpdate(status=SessionStatus.FAILED), actor=ACTOR
    )
    assert updated.status == SessionStatus.FAILED


async def test_update_session_not_found(service: SessionService) -> None:
    """Raise for an unknown session id."""
    with pytest.raises(SessionNotFound):
        await service.update_session(uuid.uuid4(), SessionUpdate(name="x"), actor=ACTOR)


async def test_delete_session(service: SessionService) -> None:
    """Delete a stored session."""
    created = await service.create_session(
        SessionCreate(agent_id=uuid.uuid4(), origin=SessionOrigin.RECORDED),
        actor=ACTOR,
    )
    await service.delete_session(created.id, actor=ACTOR)
    with pytest.raises(SessionNotFound):
        await service.get_session(created.id, actor=ACTOR)


async def test_delete_session_not_found(service: SessionService) -> None:
    """Raise for an unknown session id."""
    with pytest.raises(SessionNotFound):
        await service.delete_session(uuid.uuid4(), actor=ACTOR)


async def test_delete_session_restricted_by_replay_baseline(
    service: SessionService, replay_repository: FakeReplayRepository
) -> None:
    """Reject deleting a session that is a replay's baseline."""
    created = await service.create_session(
        SessionCreate(agent_id=uuid.uuid4(), origin=SessionOrigin.RECORDED),
        actor=ACTOR,
    )
    await replay_repository.create(
        Replay(
            owner_id=ACTOR.account.id,
            job_id=uuid.uuid4(),
            replay_config_id=uuid.uuid4(),
            baseline_session_id=created.id,
        )
    )

    with pytest.raises(SessionInUse):
        await service.delete_session(created.id, actor=ACTOR)


async def test_create_session_duplicate_external_id_conflict(
    service: SessionService,
) -> None:
    """Reject a duplicate imported_from and external id pair under one agent."""
    agent_id = uuid.uuid4()
    await service.create_session(
        SessionCreate(
            agent_id=agent_id,
            origin=SessionOrigin.IMPORTED,
            imported_from="langsmith",
            external_id="run-1",
        ),
        actor=ACTOR,
    )
    with pytest.raises(Exception, match="already registered"):
        await service.create_session(
            SessionCreate(
                agent_id=agent_id,
                origin=SessionOrigin.IMPORTED,
                imported_from="langsmith",
                external_id="run-1",
            ),
            actor=ACTOR,
        )


async def test_create_session_helper_defaults(
    repository: FakeSessionRepository,
) -> None:
    """Store a session through the create_session test helper."""
    owner_id = uuid.uuid4()
    session = await create_session(repository, owner_id, agent_id=uuid.uuid4())
    assert session.owner_id == owner_id
    assert session.status == SessionStatus.IN_PROGRESS


async def test_create_session_requires_the_principals_task_to_exist(
    service: SessionService,
) -> None:
    """A task principal whose task is unknown conflicts."""
    with pytest.raises(TaskNotFound):
        await service.create_session(
            SessionCreate(agent_id=uuid.uuid4(), origin=SessionOrigin.RECORDED),
            actor=_task_principal(uuid.uuid4()),
        )


async def test_create_session_requires_the_principals_task_to_be_running(
    service: SessionService, task_repository: FakeTaskRepository
) -> None:
    """A task principal whose task is pending conflicts."""
    task = await create_agent_task(task_repository, uuid.uuid4())
    with pytest.raises(TaskNotRunning):
        await service.create_session(
            SessionCreate(agent_id=uuid.uuid4(), origin=SessionOrigin.RECORDED),
            actor=_task_principal(task.id),
        )


async def test_create_session_rejects_a_stale_task_attempt(
    service: SessionService,
    repository: FakeSessionRepository,
    task_repository: FakeTaskRepository,
    agent_repository: FakeAgentRepository,
    agent_version_repository: FakeAgentVersionRepository,
) -> None:
    """A superseded attempt cannot claim the result session of a newer one."""
    version = await _stored_agent_version(agent_repository, agent_version_repository)
    task = await _running_agent_task(task_repository, version.id)
    stale_actor = _task_principal(task.id, attempt=task.attempt)
    task.requeue()
    task = await _start(task_repository, task)
    with pytest.raises(TaskAttemptMismatch):
        await service.create_session(
            SessionCreate(origin=SessionOrigin.RECORDED),
            actor=stale_actor,
        )
    assert await repository.get_by_task_id(task.id, include_payloads=True) is None
    session = await service.create_session(
        SessionCreate(origin=SessionOrigin.RECORDED),
        actor=_task_principal(task.id, attempt=task.attempt),
    )
    assert session.task_id == task.id


async def test_create_session_links_the_session_to_its_agent_task(
    service: SessionService,
    repository: FakeSessionRepository,
    task_repository: FakeTaskRepository,
    agent_repository: FakeAgentRepository,
    agent_version_repository: FakeAgentVersionRepository,
) -> None:
    """Creating a session for a running agent task links it as the result session."""
    version = await _stored_agent_version(agent_repository, agent_version_repository)
    task = await _running_agent_task(task_repository, version.id)
    session = await service.create_session(
        SessionCreate(origin=SessionOrigin.RECORDED),
        actor=_task_principal(task.id),
    )
    assert session.task_id == task.id
    linked = await repository.get_by_task_id(task.id, include_payloads=True)
    assert linked is not None
    assert linked.id == session.id


async def test_create_session_links_the_replays_result_session(
    service: SessionService,
    task_repository: FakeTaskRepository,
    agent_repository: FakeAgentRepository,
    agent_version_repository: FakeAgentVersionRepository,
    replay_repository: FakeReplayRepository,
) -> None:
    """Creating a session for a replay's agent task links it on the replay row."""
    version = await _stored_agent_version(agent_repository, agent_version_repository)
    task = await _running_agent_task(task_repository, version.id)
    replay = await create_replay(
        replay_repository,
        ACTOR.account.id,
        job_id=task.job_id,
        replay_config_id=uuid.uuid4(),
        baseline_session_id=uuid.uuid4(),
    )
    session = await service.create_session(
        SessionCreate(origin=SessionOrigin.RECORDED),
        actor=_task_principal(task.id),
    )
    stored_replay = await replay_repository.get(replay.id)
    assert stored_replay.result_session_id == session.id


async def test_create_session_rejects_a_second_link_to_an_agent_task(
    service: SessionService,
    task_repository: FakeTaskRepository,
    agent_repository: FakeAgentRepository,
    agent_version_repository: FakeAgentVersionRepository,
) -> None:
    """A second session cannot link to an agent task that already has one."""
    version = await _stored_agent_version(agent_repository, agent_version_repository)
    task = await _running_agent_task(task_repository, version.id)
    actor = _task_principal(task.id)
    await service.create_session(
        SessionCreate(origin=SessionOrigin.RECORDED),
        actor=actor,
    )
    with pytest.raises(TaskResultSessionAlreadyLinked):
        await service.create_session(
            SessionCreate(origin=SessionOrigin.RECORDED),
            actor=actor,
        )


async def test_create_session_links_many_sessions_to_an_import_task(
    service: SessionService, task_repository: FakeTaskRepository
) -> None:
    """An import task links every session it creates, not just one."""
    task = await _start(
        task_repository, await create_import_task(task_repository, uuid.uuid4())
    )
    actor = _task_principal(task.id)
    first = await service.create_session(
        SessionCreate(origin=SessionOrigin.IMPORTED),
        actor=actor,
    )
    second = await service.create_session(
        SessionCreate(origin=SessionOrigin.IMPORTED),
        actor=actor,
    )
    assert first.task_id == task.id
    assert second.task_id == task.id


async def test_create_session_infers_agent_and_version_from_an_agent_task(
    service: SessionService,
    task_repository: FakeTaskRepository,
    agent_repository: FakeAgentRepository,
    agent_version_repository: FakeAgentVersionRepository,
) -> None:
    """An agent task's version and its owning agent land on the session."""
    version = await _stored_agent_version(agent_repository, agent_version_repository)
    task = await _running_agent_task(task_repository, version.id)
    session = await service.create_session(
        SessionCreate(origin=SessionOrigin.RECORDED),
        actor=_task_principal(task.id),
    )
    assert session.agent_version_id == version.id
    assert session.agent_id == version.agent_id


async def test_create_session_accepts_the_agent_task_version_it_was_given(
    service: SessionService,
    task_repository: FakeTaskRepository,
    agent_repository: FakeAgentRepository,
    agent_version_repository: FakeAgentVersionRepository,
) -> None:
    """A command repeating the task's own agent version is stored unchanged."""
    version = await _stored_agent_version(agent_repository, agent_version_repository)
    task = await _running_agent_task(task_repository, version.id)
    session = await service.create_session(
        SessionCreate(
            agent_id=version.agent_id,
            agent_version_id=version.id,
            origin=SessionOrigin.RECORDED,
        ),
        actor=_task_principal(task.id),
    )
    assert session.agent_version_id == version.id


async def test_create_session_rejects_a_version_the_agent_task_does_not_run(
    service: SessionService,
    task_repository: FakeTaskRepository,
    agent_repository: FakeAgentRepository,
    agent_version_repository: FakeAgentVersionRepository,
) -> None:
    """A command naming another agent version than its task runs is rejected."""
    version = await _stored_agent_version(agent_repository, agent_version_repository)
    task = await _running_agent_task(task_repository, version.id)
    with pytest.raises(SessionAgentVersionMismatch):
        await service.create_session(
            SessionCreate(
                agent_version_id=uuid.uuid4(),
                origin=SessionOrigin.RECORDED,
            ),
            actor=_task_principal(task.id),
        )


async def test_create_session_rejects_an_agent_the_version_does_not_belong_to(
    service: SessionService,
    task_repository: FakeTaskRepository,
    agent_repository: FakeAgentRepository,
    agent_version_repository: FakeAgentVersionRepository,
) -> None:
    """A command naming another agent than its task's version belongs to is rejected."""
    version = await _stored_agent_version(agent_repository, agent_version_repository)
    task = await _running_agent_task(task_repository, version.id)
    with pytest.raises(AgentVersionAgentMismatch):
        await service.create_session(
            SessionCreate(
                agent_id=uuid.uuid4(),
                origin=SessionOrigin.RECORDED,
            ),
            actor=_task_principal(task.id),
        )


async def test_create_session_infers_the_agent_from_a_version_without_a_task(
    service: SessionService,
    agent_repository: FakeAgentRepository,
    agent_version_repository: FakeAgentVersionRepository,
) -> None:
    """A task-less session naming only a version takes the version's agent."""
    version = await _stored_agent_version(agent_repository, agent_version_repository)
    session = await service.create_session(
        SessionCreate(agent_version_id=version.id, origin=SessionOrigin.RECORDED),
        actor=ACTOR,
    )
    assert session.agent_id == version.agent_id
    assert session.agent_version_id == version.id


async def test_create_session_rejects_a_task_less_version_of_another_agent(
    service: SessionService,
    agent_repository: FakeAgentRepository,
    agent_version_repository: FakeAgentVersionRepository,
) -> None:
    """A task-less session pairing an agent with another agent's version fails."""
    version = await _stored_agent_version(agent_repository, agent_version_repository)
    other = await _stored_agent_version(
        agent_repository, agent_version_repository, name="other"
    )
    with pytest.raises(AgentVersionAgentMismatch):
        await service.create_session(
            SessionCreate(
                agent_id=other.agent_id,
                agent_version_id=version.id,
                origin=SessionOrigin.RECORDED,
            ),
            actor=ACTOR,
        )


async def test_create_session_rejects_an_unknown_agent_version(
    service: SessionService,
) -> None:
    """A task-less session naming a version that does not exist is rejected."""
    with pytest.raises(AgentVersionNotFound):
        await service.create_session(
            SessionCreate(agent_version_id=uuid.uuid4(), origin=SessionOrigin.RECORDED),
            actor=ACTOR,
        )


async def test_create_session_requires_an_agent_without_a_task_or_version(
    service: SessionService,
) -> None:
    """A session naming neither a task nor a version must carry an agent."""
    with pytest.raises(SessionAgentRequired):
        await service.create_session(
            SessionCreate(origin=SessionOrigin.RECORDED), actor=ACTOR
        )


async def test_create_session_takes_an_import_tasks_agent_and_version(
    service: SessionService,
    task_repository: FakeTaskRepository,
    agent_repository: FakeAgentRepository,
    agent_version_repository: FakeAgentVersionRepository,
) -> None:
    """An import task stamps its agent and version on every session it creates."""
    version = await _stored_agent_version(agent_repository, agent_version_repository)
    task = await _start(
        task_repository,
        await create_import_task(
            task_repository,
            uuid.uuid4(),
            agent_id=version.agent_id,
            agent_version_id=version.id,
        ),
    )
    session = await service.create_session(
        SessionCreate(origin=SessionOrigin.IMPORTED),
        actor=_task_principal(task.id),
    )
    assert session.agent_id == version.agent_id
    assert session.agent_version_id == version.id


async def test_create_session_leaves_the_version_empty_for_a_versionless_import(
    service: SessionService, task_repository: FakeTaskRepository
) -> None:
    """An import task carrying no version creates sessions carrying none."""
    agent_id = uuid.uuid4()
    task = await _start(
        task_repository,
        await create_import_task(task_repository, uuid.uuid4(), agent_id=agent_id),
    )
    session = await service.create_session(
        SessionCreate(origin=SessionOrigin.IMPORTED),
        actor=_task_principal(task.id),
    )
    assert session.agent_id == agent_id
    assert session.agent_version_id is None


async def test_create_session_rejects_an_agent_the_import_task_does_not_use(
    service: SessionService, task_repository: FakeTaskRepository
) -> None:
    """A command naming another agent than its import task creates under fails."""
    task = await _start(
        task_repository, await create_import_task(task_repository, uuid.uuid4())
    )
    with pytest.raises(SessionAgentMismatch):
        await service.create_session(
            SessionCreate(
                agent_id=uuid.uuid4(),
                origin=SessionOrigin.IMPORTED,
            ),
            actor=_task_principal(task.id),
        )


async def test_create_session_with_a_task_principal_binds_the_principals_task_id(
    service: SessionService,
    task_repository: FakeTaskRepository,
    agent_repository: FakeAgentRepository,
    agent_version_repository: FakeAgentVersionRepository,
) -> None:
    """A task principal's session links its own task.

    The account on a task principal's context is the owner of the task's
    job, resolved by the auth layer from the task token, not the account
    that registered the worker running it.
    """
    version = await _stored_agent_version(agent_repository, agent_version_repository)
    task = await _running_agent_task(task_repository, version.id)
    job_owner = Account(id=uuid.uuid4(), name="job-owner")
    actor = AuthContext(
        account=job_owner,
        principal=TaskPrincipal(
            task_id=task.id,
            attempt=task.attempt,
            worker_id=uuid.uuid4(),
            job_id=uuid.uuid4(),
        ),
    )
    session = await service.create_session(
        SessionCreate(origin=SessionOrigin.RECORDED),
        actor=actor,
    )
    assert session.task_id == task.id
    assert session.owner_id == job_owner.id


def _task_principal(
    task_id: uuid.UUID,
    granted_session_id: uuid.UUID | None = None,
    attempt: int = 1,
) -> AuthContext:
    """Build an auth context for a task principal owning the given task."""
    grants: dict[GrantKind, frozenset[uuid.UUID]] = {}
    if granted_session_id is not None:
        grants[GrantKind.SESSION] = frozenset({granted_session_id})
    return AuthContext(
        account=Account(id=uuid.uuid4(), name="job-owner"),
        principal=TaskPrincipal(
            task_id=task_id,
            attempt=attempt,
            worker_id=uuid.uuid4(),
            job_id=uuid.uuid4(),
            grants=grants,
        ),
    )


async def test_get_session_denies_a_task_principal_for_another_tasks_session(
    service: SessionService, repository: FakeSessionRepository
) -> None:
    """Reject a task principal reading a session it does not own."""
    session = await create_session(
        repository, uuid.uuid4(), uuid.uuid4(), task_id=uuid.uuid4()
    )
    actor = _task_principal(uuid.uuid4())
    with pytest.raises(SessionAccessDenied):
        await service.get_session(session.id, actor=actor)


async def test_get_session_allows_a_task_principal_for_its_own_session(
    service: SessionService, repository: FakeSessionRepository
) -> None:
    """Allow a task principal to read the session linked to its own task."""
    task_id = uuid.uuid4()
    session = await create_session(
        repository, uuid.uuid4(), uuid.uuid4(), task_id=task_id
    )
    actor = _task_principal(task_id)
    loaded = await service.get_session(session.id, actor=actor)
    assert loaded.id == session.id


async def test_get_session_allows_a_task_principal_for_its_input_session(
    service: SessionService, repository: FakeSessionRepository
) -> None:
    """Allow a task principal to read the session named as its input session."""
    session = await create_session(
        repository, uuid.uuid4(), uuid.uuid4(), task_id=uuid.uuid4()
    )
    actor = _task_principal(uuid.uuid4(), granted_session_id=session.id)
    loaded = await service.get_session(session.id, actor=actor)
    assert loaded.id == session.id


async def test_update_session_denies_a_task_principal_for_another_tasks_session(
    service: SessionService, repository: FakeSessionRepository
) -> None:
    """Reject a task principal updating a session it does not own."""
    session = await create_session(
        repository, uuid.uuid4(), uuid.uuid4(), task_id=uuid.uuid4()
    )
    actor = _task_principal(uuid.uuid4())
    with pytest.raises(SessionAccessDenied):
        await service.update_session(
            session.id, SessionUpdate(name="renamed"), actor=actor
        )


async def test_update_session_allows_a_task_principal_for_its_own_session(
    service: SessionService,
    repository: FakeSessionRepository,
    task_repository: FakeTaskRepository,
) -> None:
    """Allow a task principal to update the session linked to its own task."""
    task = await task_repository.create(
        AgentTask(job_id=uuid.uuid4(), agent_version_id=uuid.uuid4(), attempt=1)
    )
    task_id = task.id
    session = await create_session(
        repository, uuid.uuid4(), uuid.uuid4(), task_id=task_id
    )
    actor = _task_principal(task_id)
    updated = await service.update_session(
        session.id, SessionUpdate(name="renamed"), actor=actor
    )
    assert updated.name == "renamed"


async def test_update_session_denies_a_task_principal_for_its_input_session(
    service: SessionService, repository: FakeSessionRepository
) -> None:
    """Reject a task principal writing to its input session, a read-only relation."""
    session = await create_session(
        repository, uuid.uuid4(), uuid.uuid4(), task_id=uuid.uuid4()
    )
    actor = _task_principal(uuid.uuid4(), granted_session_id=session.id)
    with pytest.raises(SessionAccessDenied):
        await service.update_session(
            session.id, SessionUpdate(name="renamed"), actor=actor
        )


def _service_with_threshold(
    repository: FakeSessionRepository,
    task_repository: FakeTaskRepository,
    agent_version_repository: FakeAgentVersionRepository,
    threshold_bytes: int,
) -> tuple[SessionService, FakeBlobRepository, FakeBlobDataStore]:
    """Build a session service backed by a payload store at an offload threshold."""
    fakes = build_payload_store(threshold_bytes)
    service = SessionService(
        repository=repository,
        task_repository=task_repository,
        agent_version_repository=agent_version_repository,
        replay_repository=FakeReplayRepository(),
        payload_store=fakes.store,
    )
    return service, fakes.blob_repository, fakes.blob_data_store


async def test_create_session_offloads_over_threshold_inputs_and_outputs(
    repository: FakeSessionRepository,
    task_repository: FakeTaskRepository,
    agent_version_repository: FakeAgentVersionRepository,
) -> None:
    """Offload inputs and outputs above the threshold, keeping the response inline."""
    service, blob_repository, _ = _service_with_threshold(
        repository, task_repository, agent_version_repository, threshold_bytes=10
    )
    inputs = {"a": "x" * 50}
    outputs = {"b": "y" * 50}
    session = await service.create_session(
        SessionCreate(
            agent_id=uuid.uuid4(),
            origin=SessionOrigin.RECORDED,
            inputs=inputs,
            outputs=outputs,
        ),
        actor=ACTOR,
    )
    assert session.inputs is not None
    assert session.inputs.value == inputs
    assert session.inputs.blob_id is not None
    assert session.outputs is not None
    assert session.outputs.value == outputs
    assert session.outputs.blob_id is not None

    inputs_blob = await blob_repository.get(session.inputs.blob_id)
    assert inputs_blob.owner_id == ACTOR.account.id
    assert inputs_blob.media_type == PayloadMediaType.JSON
    assert inputs_blob.stored_in == BlobStorageBackend.DATABASE


async def test_create_session_under_threshold_stays_inline(
    repository: FakeSessionRepository,
    task_repository: FakeTaskRepository,
    agent_version_repository: FakeAgentVersionRepository,
) -> None:
    """Keep small inputs and outputs inline, with no blob reference."""
    service, _, _ = _service_with_threshold(
        repository, task_repository, agent_version_repository, threshold_bytes=1024
    )
    session = await service.create_session(
        SessionCreate(
            agent_id=uuid.uuid4(),
            origin=SessionOrigin.RECORDED,
            inputs={"a": 1},
            outputs={"b": 2},
        ),
        actor=ACTOR,
    )
    raw = await repository.get(session.id, include_payloads=True)
    assert raw.inputs is not None
    assert raw.inputs.value == {"a": 1}
    assert raw.inputs.blob_id is None
    assert raw.outputs is not None
    assert raw.outputs.value == {"b": 2}
    assert raw.outputs.blob_id is None


async def test_get_session_hydrates_offloaded_inputs_and_outputs(
    repository: FakeSessionRepository,
    task_repository: FakeTaskRepository,
    agent_version_repository: FakeAgentVersionRepository,
) -> None:
    """Return the original inputs and outputs for a session with offloaded payloads."""
    service, _, _ = _service_with_threshold(
        repository, task_repository, agent_version_repository, threshold_bytes=10
    )
    inputs = {"a": "x" * 50}
    outputs = {"b": "y" * 50}
    created = await service.create_session(
        SessionCreate(
            agent_id=uuid.uuid4(),
            origin=SessionOrigin.RECORDED,
            inputs=inputs,
            outputs=outputs,
        ),
        actor=ACTOR,
    )
    loaded = await service.get_session(created.id, actor=ACTOR)
    assert loaded.inputs is not None
    assert loaded.inputs.value == inputs
    assert loaded.outputs is not None
    assert loaded.outputs.value == outputs


async def test_update_session_offloads_new_outputs_above_threshold(
    repository: FakeSessionRepository,
    task_repository: FakeTaskRepository,
    agent_version_repository: FakeAgentVersionRepository,
) -> None:
    """Offload a finish transition's outputs above the threshold."""
    service, _, _ = _service_with_threshold(
        repository, task_repository, agent_version_repository, threshold_bytes=10
    )
    created = await service.create_session(
        SessionCreate(agent_id=uuid.uuid4(), origin=SessionOrigin.RECORDED),
        actor=ACTOR,
    )
    outputs = {"b": "y" * 50}
    updated = await service.update_session(
        created.id,
        SessionUpdate(status=SessionStatus.COMPLETED, outputs=outputs),
        actor=ACTOR,
    )
    assert updated.outputs is not None
    assert updated.outputs.value == outputs
    assert updated.outputs.blob_id is not None

    raw = await repository.get(created.id, include_payloads=True)
    assert raw.outputs is not None
    assert raw.outputs.blob_id is not None


async def test_update_session_without_touching_outputs_preserves_offloaded_outputs(
    repository: FakeSessionRepository,
    task_repository: FakeTaskRepository,
    agent_version_repository: FakeAgentVersionRepository,
) -> None:
    """An update that never sets outputs leaves the offloaded outputs intact on read."""
    service, _, _ = _service_with_threshold(
        repository, task_repository, agent_version_repository, threshold_bytes=10
    )
    outputs = {"b": "y" * 50}
    created = await service.create_session(
        SessionCreate(
            agent_id=uuid.uuid4(), origin=SessionOrigin.RECORDED, outputs=outputs
        ),
        actor=ACTOR,
    )

    updated = await service.update_session(
        created.id, SessionUpdate(name="renamed"), actor=ACTOR
    )
    assert updated.outputs is not None
    assert updated.outputs.value == outputs

    reloaded = await service.get_session(created.id, actor=ACTOR)
    assert reloaded.outputs is not None
    assert reloaded.outputs.value == outputs
