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
"""Session entity, rollups, and errors."""

import uuid
from collections.abc import Iterable
from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import Field

from kitaru.api_models.v1.session import SessionOrigin, SessionStatus, TokenUsage
from kitaru.base import FrozenModel
from kitaru.server.domain.base import (
    ConflictError,
    DomainModel,
    ForbiddenError,
    NotFoundError,
    ValidationError,
)
from kitaru.server.domain.ids import uuid7


class SessionNotFound(NotFoundError):
    """Raised when a session lookup does not resolve."""

    def __init__(self, session_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            session_id: Id of the missing session.
        """
        super().__init__(f"Session {session_id} was not found")


class SessionAccessDenied(ForbiddenError):
    """Raised when the caller's credential does not authorize this session."""

    def __init__(self, session_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            session_id: Id of the session.
        """
        super().__init__(f"Session {session_id} is not accessible to this caller")


class SessionBaselineNotFound(NotFoundError):
    """Raised when a session was not produced by a replay."""

    def __init__(self, session_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            session_id: Id of the session.
        """
        super().__init__(f"Session {session_id} was not produced by a replay")


class DuplicateSessionExternalId(ConflictError):
    """Raised when a provider and external id pair is already registered."""

    def __init__(self, provider: str | None, external_id: str | None) -> None:
        """Initialize the error.

        Args:
            provider: Source system naming the session.
            external_id: Id from the source system.
        """
        super().__init__(
            f"Session with provider '{provider}' and external_id '{external_id}' "
            "is already registered"
        )


class SessionInUse(ConflictError):
    """Raised when a session belongs to a cohort version and cannot be deleted."""

    def __init__(self, session_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            session_id: Id of the session that cannot be deleted.
        """
        super().__init__(
            f"Session {session_id} belongs to a cohort version and cannot be deleted"
        )


class SessionInUseByTask(ConflictError):
    """Raised when a session is referenced by a task and cannot be deleted."""

    def __init__(self, session_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            session_id: Id of the session that cannot be deleted.
        """
        super().__init__(f"Session {session_id} is in use by a task")


class SessionAgentVersionMismatch(ValidationError):
    """Raised when a session names a different agent version than its task runs."""

    def __init__(
        self,
        task_id: uuid.UUID,
        agent_version_id: uuid.UUID | None,
        task_agent_version_id: uuid.UUID | None,
    ) -> None:
        """Initialize the error.

        Args:
            task_id: Id of the producing task.
            agent_version_id: Agent version the session names.
            task_agent_version_id: Agent version the task runs.
        """
        super().__init__(
            f"Session names agent version {agent_version_id}, task {task_id} "
            f"runs agent version {task_agent_version_id}"
        )


class SessionAgentMismatch(ValidationError):
    """Raised when a session names a different agent than its task creates under."""

    def __init__(
        self, task_id: uuid.UUID, agent_id: uuid.UUID | None, task_agent_id: uuid.UUID
    ) -> None:
        """Initialize the error.

        Args:
            task_id: Id of the producing task.
            agent_id: Agent the session names.
            task_agent_id: Agent the task creates sessions under.
        """
        super().__init__(
            f"Session names agent {agent_id}, task {task_id} creates sessions "
            f"under agent {task_agent_id}"
        )


class SessionAgentRequired(ValidationError):
    """Raised when a session create carries no agent and none can be inferred."""

    def __init__(self) -> None:
        """Initialize the error."""
        super().__init__("Session names no agent and no task to infer one from")


class SessionStatusCannotBeCleared(ValidationError):
    """Raised when a session update tries to clear the status."""

    def __init__(self, session_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            session_id: Id of the session.
        """
        super().__init__(f"Session {session_id} status cannot be cleared")


class IllegalSessionStatusTransition(ConflictError):
    """Raised when a session status transition is not allowed."""

    def __init__(
        self,
        session_id: uuid.UUID,
        current: SessionStatus,
        target: SessionStatus,
    ) -> None:
        """Initialize the error.

        Args:
            session_id: Id of the session.
            current: Current session status.
            target: Target session status.
        """
        super().__init__(
            f"Session {session_id} cannot transition from {current} to {target}"
        )


class SessionNotIngestable(ConflictError):
    """Raised when a session does not currently accept node ingestion."""

    def __init__(self, session_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            session_id: Id of the session.
        """
        super().__init__(f"Session {session_id} does not accept node ingestion")


class SessionRollups(FrozenModel):
    """Session rollup deltas."""

    cost: Decimal = Decimal(0)
    input_tokens: int = 0
    output_tokens: int = 0
    cached_input_tokens: int = 0
    reasoning_tokens: int = 0
    llm_call_count: int = 0
    tool_call_count: int = 0


def combine_rollups(deltas: Iterable[SessionRollups]) -> SessionRollups:
    """Sum a sequence of rollup deltas into one total.

    Returns:
        Combined rollup delta.
    """
    cost = Decimal(0)
    input_tokens = 0
    output_tokens = 0
    cached_input_tokens = 0
    reasoning_tokens = 0
    llm_call_count = 0
    tool_call_count = 0
    for delta in deltas:
        cost += delta.cost
        input_tokens += delta.input_tokens
        output_tokens += delta.output_tokens
        cached_input_tokens += delta.cached_input_tokens
        reasoning_tokens += delta.reasoning_tokens
        llm_call_count += delta.llm_call_count
        tool_call_count += delta.tool_call_count
    return SessionRollups(
        cost=cost,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cached_input_tokens=cached_input_tokens,
        reasoning_tokens=reasoning_tokens,
        llm_call_count=llm_call_count,
        tool_call_count=tool_call_count,
    )


def rollup_delta(old: SessionRollups, new: SessionRollups) -> SessionRollups:
    """Compute the field-wise difference from one rollup bundle to another.

    Returns:
        Rollup delta from ``old`` to ``new``.
    """
    return SessionRollups(
        cost=new.cost - old.cost,
        input_tokens=new.input_tokens - old.input_tokens,
        output_tokens=new.output_tokens - old.output_tokens,
        cached_input_tokens=new.cached_input_tokens - old.cached_input_tokens,
        reasoning_tokens=new.reasoning_tokens - old.reasoning_tokens,
        llm_call_count=new.llm_call_count - old.llm_call_count,
        tool_call_count=new.tool_call_count - old.tool_call_count,
    )


class Session(DomainModel):
    """Session."""

    id: uuid.UUID = Field(default_factory=uuid7)
    owner_id: uuid.UUID
    agent_id: uuid.UUID
    agent_version_id: uuid.UUID | None = None
    task_id: uuid.UUID | None = None
    origin: SessionOrigin
    status: SessionStatus = SessionStatus.IN_PROGRESS
    name: str | None = None
    inputs: Any = None
    outputs: Any = None
    expected: Any = None
    error: str | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    external_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    provider: str | None = None
    framework: str | None = None
    adapter_version: str | None = None
    cost: Decimal | None = None
    tokens: TokenUsage | None = None
    llm_call_count: int = 0
    tool_call_count: int = 0
    created: datetime | None = None
    updated: datetime | None = None

    def update_name(self, name: str | None) -> None:
        """Set a new session name.

        Args:
            name: New name.
        """
        self.name = name

    def update_expected(self, expected: Any) -> None:
        """Set new expected outputs.

        Args:
            expected: New expected outputs.
        """
        self.expected = expected

    def update_metadata(self, metadata: dict[str, Any]) -> None:
        """Set new metadata.

        Args:
            metadata: New metadata.
        """
        self.metadata = metadata

    def link_task(self, task_id: uuid.UUID) -> None:
        """Set the task this session was produced by.

        Args:
            task_id: Id of the producing task.
        """
        self.task_id = task_id

    def unlink_task(self) -> None:
        """Clear the task this session was produced by."""
        self.task_id = None

    def check_node_ingest(self) -> None:
        """Require the session to currently accept node ingestion.

        Raises:
            SessionNotIngestable: The session is not in progress and its
                origin is not imported.
        """
        if self.status == SessionStatus.IN_PROGRESS:
            return
        if self.origin == SessionOrigin.IMPORTED:
            return
        raise SessionNotIngestable(self.id)

    def finish(
        self,
        status: SessionStatus,
        outputs: Any,
        error: str | None,
        ended_at: datetime | None,
    ) -> None:
        """Apply a status transition together with its outputs, error, and end time.

        Raises:
            IllegalSessionStatusTransition: The session is terminal and
                ``status`` changes it.
        """
        if status != self.status and self.status != SessionStatus.IN_PROGRESS:
            raise IllegalSessionStatusTransition(self.id, self.status, status)
        self.status = status
        self.outputs = outputs
        self.error = error
        self.ended_at = ended_at
