"""Experiment run state and progress."""

import uuid
from datetime import UTC, datetime
from enum import StrEnum

from pydantic import Field

from kitaru.base import FrozenModel
from kitaru.server.domain.base import DomainModel, NotFoundError
from kitaru.server.domain.ids import uuid7


class ExperimentRunStatus(StrEnum):
    """Experiment run lifecycle status."""

    RUNNING = "running"
    CANCELING = "canceling"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"

    @property
    def terminal(self) -> bool:
        """Report whether the run has settled."""
        return self in {self.COMPLETED, self.FAILED, self.CANCELED}


class ExperimentRunProgress(FrozenModel):
    """Replay counts for an experiment run."""

    pending: int = 0
    evaluating: int = 0
    completed: int = 0
    failed: int = 0
    canceled: int = 0
    total: int = 0


class ExperimentRunNotFound(NotFoundError):
    """Raised when an experiment run lookup does not resolve."""

    def __init__(self, run_id: uuid.UUID) -> None:
        super().__init__(f"Experiment run {run_id} was not found")


class ExperimentRun(DomainModel):
    """One execution of an experiment over a cohort."""

    id: uuid.UUID = Field(default_factory=uuid7)
    owner_id: uuid.UUID
    experiment_id: uuid.UUID
    number: int
    status: ExperimentRunStatus = ExperimentRunStatus.RUNNING
    cohort_id: uuid.UUID
    agent_version_id: uuid.UUID
    evaluate_baselines: bool = False
    started_at: datetime | None = None
    ended_at: datetime | None = None
    error: str | None = None
    created: datetime | None = None
    updated: datetime | None = None

    def start(self, now: datetime | None = None) -> None:
        """Stamp the run start time."""
        self.started_at = now or datetime.now(UTC)

    def cancel(self) -> None:
        """Request cancellation of a running run."""
        if not self.status.terminal:
            self.status = ExperimentRunStatus.CANCELING

    def finalize(self, status: ExperimentRunStatus, error: str | None = None) -> None:
        """Write a terminal run outcome."""
        if not status.terminal:
            raise ValueError("An experiment run can only finalize as terminal")
        self.status = status
        self.error = error
        self.ended_at = datetime.now(UTC)
