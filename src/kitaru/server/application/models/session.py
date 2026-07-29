"""Session filters and commands."""

import uuid
from decimal import Decimal
from typing import Any

from pydantic import AwareDatetime, Field

from kitaru.server.base import FrozenModel, ListFilter
from kitaru.server.domain.session import SessionOrigin, SessionStatus


class SessionFilter(ListFilter):
    """Session list filter."""

    agent_id: uuid.UUID | None = None
    agent_version_id: uuid.UUID | None = None
    task_id: uuid.UUID | None = None
    origin: SessionOrigin | None = None
    status: SessionStatus | None = None
    provider: str | None = None
    external_id: str | None = None
    name: str | None = None
    tag: str | None = None
    started_after: AwareDatetime | None = None
    started_before: AwareDatetime | None = None
    ended_after: AwareDatetime | None = None
    ended_before: AwareDatetime | None = None
    has_evaluation: bool | None = None
    min_cost: Decimal | None = None
    max_cost: Decimal | None = None


class SessionCreate(FrozenModel):
    """Session creation command."""

    agent_id: uuid.UUID
    agent_version_id: uuid.UUID | None = None
    origin: SessionOrigin
    status: SessionStatus = SessionStatus.IN_PROGRESS
    name: str | None = None
    inputs: Any = None
    outputs: Any = None
    expected: Any = None
    error: str | None = None
    started_at: AwareDatetime | None = None
    ended_at: AwareDatetime | None = None
    external_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    provider: str | None = None
    framework: str | None = None
    adapter_version: str | None = None
    task_id: uuid.UUID | None = None


class SessionUpdate(FrozenModel):
    """Partial session update."""

    status: SessionStatus | None = None
    outputs: Any = None
    error: str | None = None
    ended_at: AwareDatetime | None = None
    name: str | None = None
    expected: Any = None
    metadata: dict[str, Any] | None = None
