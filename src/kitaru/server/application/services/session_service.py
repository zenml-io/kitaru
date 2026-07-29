#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
"""Session lifecycle use cases."""

import uuid

from kitaru.server.application.interfaces.agent_repository import (
    AgentRepository,
    AgentVersionRepository,
)
from kitaru.server.application.interfaces.session_repository import SessionRepository
from kitaru.server.application.interfaces.task_repository import TaskRepository
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.session import (
    SessionCreate,
    SessionFilter,
    SessionUpdate,
)
from kitaru.server.domain.base import ConflictError, ValidationError
from kitaru.server.domain.session import Session
from kitaru.server.domain.task import AgentTask, ImportTask, TaskStatus


class SessionService:
    """Session lifecycle use cases."""

    def __init__(
        self,
        repository: SessionRepository,
        agent_repository: AgentRepository,
        agent_version_repository: AgentVersionRepository,
        task_repository: TaskRepository,
    ) -> None:
        self._repository = repository
        self._agent_repository = agent_repository
        self._agent_version_repository = agent_version_repository
        self._task_repository = task_repository

    async def create_session(
        self, command: SessionCreate, actor: AuthContext
    ) -> Session:
        """Create a session and, for agent tasks, link it as the result."""
        await self._agent_repository.get(command.agent_id)
        if command.agent_version_id is not None:
            version = await self._agent_version_repository.get(command.agent_version_id)
            if version.agent_id != command.agent_id:
                raise ValidationError(
                    "Session agent version does not belong to its agent"
                )
        session = Session(owner_id=actor.account.id, **command.model_dump())
        linked_task = None
        if command.task_id is not None:
            linked_task = await self._task_repository.get(
                command.task_id, exclusive=True
            )
            if linked_task.status is not TaskStatus.RUNNING:
                raise ConflictError(f"Task {linked_task.id} is not running")
            if isinstance(linked_task, AgentTask):
                if command.agent_version_id != linked_task.agent_version_id:
                    raise ValidationError(
                        "Session agent version does not match its task"
                    )
                linked_task.link_result_session(session.id)
            elif isinstance(linked_task, ImportTask):
                if command.agent_id != linked_task.agent_id:
                    raise ValidationError("Session agent does not match its task")
            else:
                raise ValidationError("Evaluator tasks cannot create linked sessions")
        stored = await self._repository.create(session)
        if linked_task is not None and isinstance(linked_task, AgentTask):
            await self._task_repository.update(linked_task)
        return stored

    async def get_session(self, session_id: uuid.UUID, actor: AuthContext) -> Session:
        """Get a session."""
        _ = actor
        return await self._repository.get(session_id)

    async def list_sessions(
        self, session_filter: SessionFilter, actor: AuthContext
    ) -> tuple[list[Session], str | None]:
        """List sessions."""
        _ = actor
        return await self._repository.query(session_filter)

    async def update_session(
        self,
        session_id: uuid.UUID,
        command: SessionUpdate,
        actor: AuthContext,
    ) -> Session:
        """Partially update or finish a session."""
        _ = actor
        session = await self._repository.get(session_id)
        if "status" in command.model_fields_set:
            if command.status is None:
                raise ValidationError("Session status cannot be null")
            session.finish(
                command.status,
                outputs=command.outputs,
                error=command.error,
                ended_at=command.ended_at,
            )
        if "name" in command.model_fields_set:
            session.update_name(command.name)
        if "expected" in command.model_fields_set:
            session.update_expected(command.expected)
        if "metadata" in command.model_fields_set:
            session.update_metadata(command.metadata or {})
        return await self._repository.update(session)

    async def delete_session(self, session_id: uuid.UUID, actor: AuthContext) -> None:
        """Delete a session."""
        _ = actor
        await self._repository.get(session_id)
        await self._repository.delete(session_id)
