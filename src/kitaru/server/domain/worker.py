"""Worker registration entity."""

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from pydantic import Field

from kitaru.api_models.v1.task import WorkerScope
from kitaru.base import FrozenModel
from kitaru.server.domain.base import DomainModel, NotFoundError
from kitaru.server.domain.ids import uuid7


class WorkerRuntime(FrozenModel):
    """Detected worker runtime facts."""

    platform: str
    hostname: str | None = None
    os: str | None = None
    arch: str | None = None
    python_version: str | None = None
    kitaru_version: str | None = None
    namespace: str | None = None
    pod: str | None = None


class WorkerNotFound(NotFoundError):
    """Raised when a worker lookup does not resolve."""

    def __init__(self, worker: uuid.UUID | str) -> None:
        super().__init__(f"Worker {worker} was not found")


class Worker(DomainModel):
    """Registered task executor."""

    id: uuid.UUID = Field(default_factory=uuid7)
    owner_id: uuid.UUID
    name: str
    scope: WorkerScope
    runtime: WorkerRuntime
    last_seen_at: datetime
    metadata: dict[str, Any] = Field(default_factory=dict)
    created: datetime | None = None
    updated: datetime | None = None

    def refresh(
        self,
        runtime: WorkerRuntime | None = None,
        scope: WorkerScope | None = None,
        metadata: dict[str, Any] | None = None,
        now: datetime | None = None,
    ) -> None:
        """Refresh registration and liveness data."""
        if runtime is not None:
            self.runtime = runtime
        if scope is not None:
            self.scope = scope
        if metadata is not None:
            self.metadata = metadata
        self.last_seen_at = now or datetime.now(UTC)

    def is_live(self, timeout_seconds: float, now: datetime | None = None) -> bool:
        """Report whether the worker checked in within the timeout."""
        current = now or datetime.now(UTC)
        seen = self.last_seen_at
        if seen.tzinfo is None:
            seen = seen.replace(tzinfo=UTC)
        return current - seen <= timedelta(seconds=timeout_seconds)
