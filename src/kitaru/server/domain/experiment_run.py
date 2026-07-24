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
"""Experiment run entity, value objects, and errors."""

import uuid
from collections.abc import Sequence
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import Field

from kitaru.server.base import FrozenModel
from kitaru.server.domain.base import (
    ConflictError,
    DomainModel,
    NotFoundError,
    ValidationError,
)
from kitaru.server.domain.ids import uuid7
from kitaru.server.domain.replay import ReplayStatus


class ExperimentRunStatus(StrEnum):
    """Experiment run status."""

    PENDING = "pending"
    RUNNING = "running"
    CANCELING = "canceling"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"


TERMINAL_RUN_STATUSES = frozenset(
    {
        ExperimentRunStatus.COMPLETED,
        ExperimentRunStatus.FAILED,
        ExperimentRunStatus.CANCELED,
    }
)


class ExperimentRunNotFound(NotFoundError):
    """Raised when an experiment run lookup does not resolve."""

    def __init__(self, run_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            run_id: Id of the missing experiment run.
        """
        super().__init__(f"Experiment run {run_id} was not found")


class InvalidExperimentRun(ValidationError):
    """Raised when an experiment run violates its shape rules."""


class InvalidExperimentRunTransition(ConflictError):
    """Raised when an experiment run status transition is illegal."""

    def __init__(
        self,
        run_id: uuid.UUID,
        status: ExperimentRunStatus,
        target: ExperimentRunStatus,
    ) -> None:
        """Initialize the error.

        Args:
            run_id: Id of the experiment run.
            status: Current status.
            target: Requested status.
        """
        super().__init__(
            f"Experiment run {run_id} cannot transition from '{status}' to '{target}'"
        )


class ExperimentRunActive(ConflictError):
    """Raised when an operation requires a terminal experiment run."""

    def __init__(self, run_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            run_id: Id of the experiment run.
        """
        super().__init__(f"Experiment run {run_id} is not terminal")


class ExperimentRunProgress(FrozenModel):
    """Experiment run progress."""

    pending: int = 0
    claimed: int = 0
    running: int = 0
    completed: int = 0
    failed: int = 0
    timed_out: int = 0
    canceled: int = 0
    total: int = 0

    @classmethod
    def from_counts(cls, counts: dict[ReplayStatus, int]) -> "ExperimentRunProgress":
        """Build a progress from replay counts by status.

        Args:
            counts: Replay counts by status.

        Returns:
            Progress with the total set.
        """
        return cls(
            **{status.value: count for status, count in counts.items()},
            total=sum(counts.values()),
        )


class ExperimentRun(DomainModel):
    """Experiment run."""

    id: uuid.UUID = Field(default_factory=uuid7)
    owner_id: uuid.UUID
    experiment_id: uuid.UUID
    number: int = 0
    status: ExperimentRunStatus = ExperimentRunStatus.PENDING
    agent_version_id: uuid.UUID
    score_baselines: bool = False
    started_at: datetime | None = None
    ended_at: datetime | None = None
    summary: dict[str, Any] | None = None
    error: str | None = None
    created: datetime | None = None
    updated: datetime | None = None

    def start(self) -> None:
        """Start the run on its first claim.

        Raises:
            InvalidExperimentRunTransition: The run is not pending.
        """
        if self.status is not ExperimentRunStatus.PENDING:
            raise InvalidExperimentRunTransition(
                self.id, self.status, ExperimentRunStatus.RUNNING
            )
        self.status = ExperimentRunStatus.RUNNING
        self.started_at = datetime.now(UTC)

    def cancel(self) -> None:
        """Request cancellation of the run.

        Raises:
            InvalidExperimentRunTransition: The run is already terminal.
        """
        if self.status in TERMINAL_RUN_STATUSES:
            raise InvalidExperimentRunTransition(
                self.id, self.status, ExperimentRunStatus.CANCELING
            )
        self.status = ExperimentRunStatus.CANCELING

    def finalize(
        self, summary: dict[str, Any], replay_statuses: Sequence[ReplayStatus]
    ) -> None:
        """Finalize the run once its last replay is terminal.

        A canceling run lands on canceled, a run with failed or timed out
        replays on failed with the counts as its error, any other run on
        completed.

        Args:
            summary: Aggregate diff summary.
            replay_statuses: Statuses of the run's replays.

        Raises:
            InvalidExperimentRunTransition: The run is already terminal.
        """
        failed = sum(1 for status in replay_statuses if status is ReplayStatus.FAILED)
        timed_out = sum(
            1 for status in replay_statuses if status is ReplayStatus.TIMED_OUT
        )
        if self.status is ExperimentRunStatus.CANCELING:
            target = ExperimentRunStatus.CANCELED
        elif failed or timed_out:
            target = ExperimentRunStatus.FAILED
        else:
            target = ExperimentRunStatus.COMPLETED
        if self.status in TERMINAL_RUN_STATUSES:
            raise InvalidExperimentRunTransition(self.id, self.status, target)
        if target is ExperimentRunStatus.FAILED:
            total = len(replay_statuses)
            parts = []
            if failed:
                parts.append(f"{failed} of {total} replays failed")
            if timed_out:
                if failed:
                    parts.append(f"{timed_out} timed out")
                else:
                    parts.append(f"{timed_out} of {total} replays timed out")
            self.error = ", ".join(parts)
        self.status = target
        self.summary = summary
        self.ended_at = datetime.now(UTC)
