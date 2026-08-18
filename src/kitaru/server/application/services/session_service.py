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
"""Session use cases."""

import uuid

from kitaru.analytics.events import AnalyticsEvent
from kitaru.server.application.interfaces.agent_version_repository import (
    AgentVersionRepository,
)
from kitaru.server.application.interfaces.replay_repository import ReplayRepository
from kitaru.server.application.interfaces.session_repository import (
    SessionRepository,
)
from kitaru.server.application.interfaces.task_repository import TaskRepository
from kitaru.server.application.models.auth import AuthContext, TaskPrincipal
from kitaru.server.application.models.session import (
    SessionCreate,
    SessionFilter,
    SessionUpdate,
)
from kitaru.server.application.services import analytics_events
from kitaru.server.application.services.agent_version_resolution import resolve_agent_id
from kitaru.server.application.services.resource_access import (
    check_task_attempt,
    check_task_session_read,
    check_task_session_write,
)
from kitaru.server.application.services.server_analytics import ServerAnalytics
from kitaru.server.domain.session import (
    Session,
    SessionAgentMismatch,
    SessionAgentRequired,
    SessionAgentVersionMismatch,
    SessionBaselineNotFound,
    SessionStatus,
    SessionStatusCannotBeCleared,
)
from kitaru.server.domain.task import (
    AgentTask,
    ImportTask,
    Task,
    TaskResultSessionAlreadyLinked,
)


class SessionService:
    """Session use cases."""

    def __init__(
        self,
        repository: SessionRepository,
        task_repository: TaskRepository,
        agent_version_repository: AgentVersionRepository,
        replay_repository: ReplayRepository,
        analytics: ServerAnalytics | None = None,
    ) -> None:
        """Initialize the service.

        Args:
            repository: Session repository.
            task_repository: Task repository, for the create-time task link.
            agent_version_repository: Agent version repository, for the agent
                a version belongs to.
            replay_repository: Replay repository, for the baseline lookup.
            analytics: Analytics tracker, None skips tracking.
        """
        self._repository = repository
        self._tasks = task_repository
        self._agent_versions = agent_version_repository
        self._replays = replay_repository
        self._analytics = analytics

    async def create_session(
        self, command: SessionCreate, actor: AuthContext
    ) -> Session:
        """Create a session owned by the caller.

        A task principal's session is linked to the principal's task, which
        must be running. An agent task links exactly one session, and its
        result session is found through that link. An import task links
        every session it creates. A replay owning the agent task's job stores
        the session as its result session. The task is the source of truth
        for the agent and the agent version. The session takes the next
        number of its agent, allocated outside the request transaction, so a
        failed create leaves a gap.

        Args:
            command: Fields for the new session.
            actor: Caller context.

        Raises:
            TaskNotFound: No task has the principal's task id.
            TaskNotRunning: The principal's task is not running.
            TaskAttemptMismatch: The principal's token is fenced by an attempt
                the task has moved past.
            TaskResultSessionAlreadyLinked: The principal's agent task already
                links a session.
            SessionAgentVersionMismatch: The command names a different agent
                version than the task runs.
            SessionAgentMismatch: The command names a different agent than the
                task creates sessions under.
            SessionAgentRequired: The command names no agent and no task to
                infer one from.
            AgentVersionNotFound: No agent version has the resolved id.
            AgentVersionAgentMismatch: The resolved agent version belongs to
                another agent.
            AgentNotFound: No agent has the resolved id.
            DuplicateSessionExternalId: The imported_from and external id pair is
                already registered.

        Returns:
            Created session.
        """
        task_id = None
        task = None
        if isinstance(actor.principal, TaskPrincipal):
            task_id = actor.principal.task_id
            task = await self._tasks.get(task_id, exclusive=True)
            task.check_running()
            # Check the attempt on the locked task so a requeue cannot race
            # the result link.
            task.check_attempt(actor.principal.attempt)
            if (
                isinstance(task, AgentTask)
                and await self._repository.get_by_task_id(task.id) is not None
            ):
                raise TaskResultSessionAlreadyLinked(task.id)
        agent_id, agent_version_id = await self._resolve_agent(command, task)
        number = await self._repository.allocate_session_number(agent_id)
        session = Session(
            owner_id=actor.account.id,
            agent_id=agent_id,
            number=number,
            agent_version_id=agent_version_id,
            task_id=task_id,
            origin=command.origin,
            status=command.status
            if command.status is not None
            else SessionStatus.IN_PROGRESS,
            name=command.name,
            inputs=command.inputs,
            outputs=command.outputs,
            error=command.error,
            started_at=command.started_at,
            ended_at=command.ended_at,
            external_id=command.external_id,
            metadata=command.metadata,
            imported_from=command.imported_from,
            framework=command.framework,
            adapter_version=command.adapter_version,
        )
        stored = await self._repository.create(session)
        if isinstance(task, AgentTask):
            replay = await self._replays.get_by_job_id(task.job_id)
            if replay is not None:
                replay.link_result_session(stored.id)
                await self._replays.update(replay)
        if self._analytics is not None and stored.status != SessionStatus.IN_PROGRESS:
            self._analytics.track(
                stored.owner_id,
                AnalyticsEvent.SESSION_COMPLETED,
                analytics_events.build_session_completed_properties(stored),
            )
        return stored

    async def _resolve_agent(
        self, command: SessionCreate, task: Task | None
    ) -> tuple[uuid.UUID, uuid.UUID | None]:
        """Resolve the agent and agent version a new session records.

        A session produced by an agent or import task takes both from that
        task, and a command naming a different one is rejected. Without a
        task the command carries them, and the agent is inferred whenever a
        version is named.

        Args:
            command: Fields for the new session.
            task: Task the session was produced by, None when the caller
                has none.

        Raises:
            SessionAgentVersionMismatch: The command names a different agent
                version than the task runs.
            SessionAgentMismatch: The command names a different agent than the
                task creates sessions under.
            SessionAgentRequired: The command names no agent and no task to
                infer one from.
            AgentVersionNotFound: No agent version has the resolved id.
            AgentVersionAgentMismatch: The resolved agent version belongs to
                another agent.

        Returns:
            Agent id and agent version id for the session.
        """
        agent_id = command.agent_id
        agent_version_id = command.agent_version_id
        if isinstance(task, AgentTask | ImportTask):
            if (
                agent_version_id is not None
                and agent_version_id != task.agent_version_id
            ):
                raise SessionAgentVersionMismatch(
                    task.id, agent_version_id, task.agent_version_id
                )
            agent_version_id = task.agent_version_id
            if isinstance(task, ImportTask):
                if agent_id is not None and agent_id != task.agent_id:
                    raise SessionAgentMismatch(task.id, agent_id, task.agent_id)
                agent_id = task.agent_id
        if agent_version_id is not None:
            agent_id = await resolve_agent_id(
                agent_version_id, agent_id, self._agent_versions
            )
        if agent_id is None:
            raise SessionAgentRequired()
        return agent_id, agent_version_id

    async def get_session(self, session_id: uuid.UUID, actor: AuthContext) -> Session:
        """Get a session by id.

        A task principal reads a session it owns or holds as its task's
        input session.

        Args:
            session_id: Id of the session.
            actor: Caller context.

        Raises:
            SessionNotFound: No session has this id.
            SessionAccessDenied: A task principal owns neither the session nor
                holds it as its task's input session.

        Returns:
            Stored session.
        """
        session = await self._repository.get(session_id)
        check_task_session_read(session_id, session.task_id, actor)
        return session

    async def get_baseline_session(
        self, session_id: uuid.UUID, actor: AuthContext
    ) -> Session:
        """Get the baseline session a replayed session was produced from.

        The link runs from the replay holding the session as its result. A
        session no replay holds as its result has no baseline.

        Args:
            session_id: Id of the replayed session.
            actor: Caller context.

        Raises:
            SessionNotFound: No session has this id, or the baseline it
                resolves to is gone.
            SessionAccessDenied: A task principal owns neither the session nor
                holds it as its task's input session.
            SessionBaselineNotFound: The session was not produced by a replay.

        Returns:
            Baseline session the replay ran against.
        """
        session = await self._repository.get(session_id)
        check_task_session_read(session_id, session.task_id, actor)
        replay = await self._replays.get_by_result_session_id(session_id)
        if replay is None:
            raise SessionBaselineNotFound(session_id)
        return await self._repository.get(replay.baseline_session_id)

    async def list_sessions(
        self, session_filter: SessionFilter, actor: AuthContext
    ) -> tuple[list[Session], str | None]:
        """List sessions matching a filter.

        Args:
            session_filter: Filter and pagination parameters.
            actor: Caller context.

        Returns:
            Page of matching sessions and the next cursor.
        """
        _ = actor
        return await self._repository.query(session_filter)

    async def update_session(
        self, session_id: uuid.UUID, command: SessionUpdate, actor: AuthContext
    ) -> Session:
        """Partially update a session.

        ``outputs``, ``error``, and ``ended_at`` only ever change together
        with a status transition, applied through ``Session.finish``. When
        the command sets none of ``status``, ``outputs``, ``error``, or
        ``ended_at``, the session's current status carries through as a
        no-op transition, which leaves those fields untouched. A task
        principal writes only a session it owns.

        Args:
            session_id: Id of the session.
            command: Fields to change, built from the request's set fields.
            actor: Caller context.

        Raises:
            SessionNotFound: No session has this id.
            SessionAccessDenied: A task principal does not own the session.
            SessionStatusCannotBeCleared: The command clears the status with
                an explicit null.
            IllegalSessionStatusTransition: The session is terminal and the
                command moves it back to in_progress.

        Returns:
            Updated session.
        """
        session = await self._repository.get(session_id, exclusive=True)
        check_task_session_write(session_id, session.task_id, actor)
        await check_task_attempt(actor, self._tasks)
        fields = command.model_fields_set
        if {"status", "outputs", "error", "ended_at"} & fields:
            previous_status = session.status
            target_status = session.status
            if "status" in fields:
                if command.status is None:
                    raise SessionStatusCannotBeCleared(session_id)
                target_status = command.status
            session.finish(
                status=target_status,
                outputs=command.outputs if "outputs" in fields else session.outputs,
                error=command.error if "error" in fields else session.error,
                ended_at=command.ended_at if "ended_at" in fields else session.ended_at,
            )
            if (
                self._analytics is not None
                and previous_status == SessionStatus.IN_PROGRESS
                and session.status != SessionStatus.IN_PROGRESS
            ):
                self._analytics.track(
                    session.owner_id,
                    AnalyticsEvent.SESSION_COMPLETED,
                    analytics_events.build_session_completed_properties(session),
                )
        if "name" in fields:
            session.update_name(command.name)
        if "metadata" in fields:
            session.update_metadata(
                command.metadata if command.metadata is not None else {}
            )
        return await self._repository.update(session)

    async def delete_session(self, session_id: uuid.UUID, actor: AuthContext) -> None:
        """Delete a session.

        Deleting a session cascades its nodes.

        Args:
            session_id: Id of the session.
            actor: Caller context.

        Raises:
            SessionNotFound: No session has this id.
            SessionInUse: The session is referenced by a cohort version,
                investigation, or replay and cannot be deleted.
        """
        _ = actor
        await self._repository.delete(session_id)
