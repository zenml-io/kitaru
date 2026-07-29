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

from kitaru.server.application.interfaces.session_repository import (
    SessionRepository,
)
from kitaru.server.application.interfaces.task_repository import TaskRepository
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.session import (
    SessionCreate,
    SessionFilter,
    SessionUpdate,
)
from kitaru.server.domain.session import (
    Session,
    SessionStatus,
    SessionStatusCannotBeCleared,
)
from kitaru.server.domain.task import AgentTask, TaskResultSessionAlreadyLinked


class SessionService:
    """Session use cases."""

    def __init__(
        self, repository: SessionRepository, task_repository: TaskRepository
    ) -> None:
        """Initialize the service.

        Args:
            repository: Session repository.
            task_repository: Task repository, for the create-time task link.
        """
        self._repository = repository
        self._tasks = task_repository

    async def create_session(
        self, command: SessionCreate, actor: AuthContext
    ) -> Session:
        """Create a session owned by the caller.

        A session naming a task requires that task to be running. An agent
        task links exactly one session and gets its result session written in
        the same transaction, an import task links every session it creates.

        Args:
            command: Fields for the new session.
            actor: Caller context.

        Raises:
            TaskNotFound: No task has the named id.
            TaskNotRunning: The named task is not running.
            TaskResultSessionAlreadyLinked: The named agent task already
                links a session.
            DuplicateSessionExternalId: The provider and external id pair is
                already registered.

        Returns:
            Created session.
        """
        task = None
        if command.task_id is not None:
            task = await self._tasks.get(command.task_id, exclusive=True)
            task.check_running()
            if isinstance(task, AgentTask) and task.result_session_id is not None:
                raise TaskResultSessionAlreadyLinked(task.id)
        session = Session(
            owner_id=actor.account.id,
            agent_id=command.agent_id,
            agent_version_id=command.agent_version_id,
            task_id=command.task_id,
            origin=command.origin,
            status=command.status
            if command.status is not None
            else SessionStatus.IN_PROGRESS,
            name=command.name,
            inputs=command.inputs,
            outputs=command.outputs,
            expected=command.expected,
            error=command.error,
            started_at=command.started_at,
            ended_at=command.ended_at,
            external_id=command.external_id,
            metadata=command.metadata,
            provider=command.provider,
            framework=command.framework,
            adapter_version=command.adapter_version,
        )
        stored = await self._repository.create(session)
        if isinstance(task, AgentTask):
            task.link_result_session(stored.id)
            await self._tasks.update(task)
        return stored

    async def get_session(self, session_id: uuid.UUID, actor: AuthContext) -> Session:
        """Get a session by id.

        Args:
            session_id: Id of the session.
            actor: Caller context.

        Raises:
            SessionNotFound: No session has this id.

        Returns:
            Stored session.
        """
        _ = actor
        return await self._repository.get(session_id)

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
        no-op transition, which leaves those fields untouched.

        Args:
            session_id: Id of the session.
            command: Fields to change, built from the request's set fields.
            actor: Caller context.

        Raises:
            SessionNotFound: No session has this id.
            SessionStatusCannotBeCleared: The command clears the status with
                an explicit null.
            IllegalSessionStatusTransition: The session is terminal and the
                command moves it back to in_progress.

        Returns:
            Updated session.
        """
        _ = actor
        session = await self._repository.get(session_id, exclusive=True)
        fields = command.model_fields_set
        if {"status", "outputs", "error", "ended_at"} & fields:
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
        if "name" in fields:
            session.update_name(command.name)
        if "expected" in fields:
            session.update_expected(command.expected)
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
        """
        _ = actor
        await self._repository.delete(session_id)
