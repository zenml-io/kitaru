"""Replay pipeline entity."""

import uuid
from datetime import datetime
from enum import StrEnum

from pydantic import Field

from kitaru.server.domain.base import DomainModel, NotFoundError
from kitaru.server.domain.ids import uuid7


class ReplayStatus(StrEnum):
    """Replay pipeline lifecycle status."""

    PENDING = "pending"
    EVALUATING = "evaluating"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"

    @property
    def terminal(self) -> bool:
        """Report whether the replay has settled."""
        return self in {self.COMPLETED, self.FAILED, self.CANCELED}


class ReplayNotFound(NotFoundError):
    """Raised when a replay lookup does not resolve."""

    def __init__(self, replay_id: uuid.UUID) -> None:
        super().__init__(f"Replay {replay_id} was not found")


class Replay(DomainModel):
    """Replay pipeline tied one-to-one to a job."""

    id: uuid.UUID = Field(default_factory=uuid7)
    owner_id: uuid.UUID
    job_id: uuid.UUID
    experiment_run_id: uuid.UUID | None = None
    replay_config_id: uuid.UUID
    baseline_session_id: uuid.UUID
    evaluate_baselines: bool = False
    status: ReplayStatus = ReplayStatus.PENDING
    error: str | None = None
    created: datetime | None = None
    updated: datetime | None = None

    @property
    def settled(self) -> bool:
        """Report whether the replay is terminal."""
        return self.status.terminal

    def start_evaluating(self) -> None:
        """Move the replay into result evaluation."""
        if not self.settled:
            self.status = ReplayStatus.EVALUATING

    def complete(self) -> None:
        """Set the successful outcome."""
        self.status = ReplayStatus.COMPLETED
        self.error = None

    def fail(self, error: str | None) -> None:
        """Set the failed outcome."""
        self.status = ReplayStatus.FAILED
        self.error = error

    def cancel(self) -> None:
        """Set the canceled outcome."""
        self.status = ReplayStatus.CANCELED
