"""Job aggregate state."""

import uuid
from datetime import UTC, datetime
from enum import StrEnum

from pydantic import Field

from kitaru.server.domain.base import ConflictError, DomainModel, NotFoundError
from kitaru.server.domain.ids import uuid7


class JobStatus(StrEnum):
    """Job lifecycle status."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"

    @property
    def terminal(self) -> bool:
        """Report whether no further transition is allowed."""
        return self in {self.COMPLETED, self.FAILED, self.CANCELED}


class JobNotFound(NotFoundError):
    """Raised when a job lookup does not resolve."""

    def __init__(self, job_id: uuid.UUID) -> None:
        super().__init__(f"Job {job_id} was not found")


class JobSettledConflict(ConflictError):
    """Raised when a task would be appended to a settled job."""

    def __init__(self, job_id: uuid.UUID) -> None:
        super().__init__(f"Job {job_id} is already settled")


class Job(DomainModel):
    """Generic group of execution tasks."""

    id: uuid.UUID = Field(default_factory=uuid7)
    owner_id: uuid.UUID
    status: JobStatus = JobStatus.PENDING
    cancel_requested_at: datetime | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    error: str | None = None
    created: datetime | None = None
    updated: datetime | None = None

    def start(self, now: datetime | None = None) -> None:
        """Move a pending job to running."""
        if self.status is JobStatus.PENDING:
            self.status = JobStatus.RUNNING
            self.started_at = now or datetime.now(UTC)

    def request_cancel(self, now: datetime | None = None) -> None:
        """Record a job-level cancellation request."""
        if not self.status.terminal and self.cancel_requested_at is None:
            self.cancel_requested_at = now or datetime.now(UTC)

    def settle(
        self, status: JobStatus, error: str | None = None, now: datetime | None = None
    ) -> None:
        """Write the terminal job outcome."""
        if status not in {
            JobStatus.COMPLETED,
            JobStatus.FAILED,
            JobStatus.CANCELED,
        }:
            raise ValueError("A job can only settle to a terminal status")
        if self.status.terminal:
            return
        self.status = status
        self.error = error
        self.ended_at = now or datetime.now(UTC)
