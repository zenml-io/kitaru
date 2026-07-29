"""Recorded, imported, and replayed session entity."""

import uuid
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

from pydantic import Field

from kitaru.base import FrozenModel
from kitaru.server.domain.base import (
    ConflictError,
    DomainModel,
    NotFoundError,
    ValidationError,
)
from kitaru.server.domain.ids import uuid7


class SessionOrigin(StrEnum):
    """How a session entered Kitaru."""

    IMPORTED = "imported"
    RECORDED = "recorded"
    REPLAY = "replay"


class SessionStatus(StrEnum):
    """Session lifecycle status."""

    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"

    @property
    def terminal(self) -> bool:
        """Report whether the session has ended."""
        return self in {self.COMPLETED, self.FAILED}


class TokenUsage(FrozenModel):
    """Aggregate token usage."""

    input_tokens: int | None = None
    output_tokens: int | None = None
    cached_input_tokens: int | None = None
    reasoning_tokens: int | None = None


class SessionRollups(FrozenModel):
    """Delta applied to session aggregate fields."""

    cost: Decimal = Decimal(0)
    input_tokens: int = 0
    output_tokens: int = 0
    cached_input_tokens: int = 0
    reasoning_tokens: int = 0
    llm_call_count: int = 0
    tool_call_count: int = 0


class SessionNotFound(NotFoundError):
    """Raised when a session lookup does not resolve."""

    def __init__(self, session_id: uuid.UUID) -> None:
        super().__init__(f"Session {session_id} was not found")


class DuplicateExternalSession(ConflictError):
    """Raised when a provider/external id pair already exists."""

    def __init__(self, provider: str | None, external_id: str) -> None:
        super().__init__(
            f"Session {provider or '<unknown>'}/{external_id} is already stored"
        )


class SessionInUse(ConflictError):
    """Raised when a session has dependent replay or cohort rows."""

    def __init__(self, session_id: uuid.UUID) -> None:
        super().__init__(f"Session {session_id} is in use")


class InvalidSessionTransition(ValidationError):
    """Raised when a session transition is illegal."""


class Session(DomainModel):
    """One agent execution."""

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
        """Set the session name."""
        self.name = name

    def update_expected(self, expected: Any) -> None:
        """Replace the expected output."""
        self.expected = expected

    def update_metadata(self, metadata: dict[str, Any]) -> None:
        """Replace session metadata."""
        self.metadata = metadata

    def check_node_ingest(self) -> None:
        """Require an active session, except for imported snapshots."""
        if self.origin is not SessionOrigin.IMPORTED and self.status.terminal:
            raise InvalidSessionTransition(f"Session {self.id} no longer accepts nodes")

    def finish(
        self,
        status: SessionStatus,
        outputs: Any = None,
        error: str | None = None,
        ended_at: datetime | None = None,
    ) -> None:
        """Set the terminal session outcome."""
        if not status.terminal:
            raise InvalidSessionTransition("A session can only finish as terminal")
        if self.status.terminal and self.status is not status:
            raise InvalidSessionTransition(f"Session {self.id} is already terminal")
        self.status = status
        self.outputs = outputs
        self.error = error
        self.ended_at = ended_at
