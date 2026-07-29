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
"""Experiment run entity and errors."""

import uuid
from datetime import datetime

from pydantic import Field

from kitaru.api_models.v1.experiment_run import ExperimentRunStatus
from kitaru.server.domain.base import ConflictError, DomainModel, NotFoundError
from kitaru.server.domain.ids import uuid7

TERMINAL_RUN_STATUSES = frozenset(
    {
        ExperimentRunStatus.COMPLETED,
        ExperimentRunStatus.FAILED,
        ExperimentRunStatus.CANCELED,
    }
)


class ExperimentRunNotFound(NotFoundError):
    """Raised when an experiment run lookup does not resolve."""

    def __init__(self, experiment_run_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            experiment_run_id: Id of the missing run.
        """
        super().__init__(f"Experiment run {experiment_run_id} was not found")


class DuplicateExperimentRunNumber(ConflictError):
    """Raised when an experiment already has a run with a given number."""

    def __init__(self, experiment_id: uuid.UUID, number: int) -> None:
        """Initialize the error.

        Args:
            experiment_id: Id of the experiment.
            number: Run number that is already assigned.
        """
        super().__init__(
            f"Experiment {experiment_id} already has a run numbered {number}"
        )


class IllegalExperimentRunStatusTransition(ConflictError):
    """Raised when an experiment run status transition is not allowed."""

    def __init__(
        self,
        experiment_run_id: uuid.UUID,
        current: ExperimentRunStatus,
        target: ExperimentRunStatus,
    ) -> None:
        """Initialize the error.

        Args:
            experiment_run_id: Id of the run.
            current: Current run status.
            target: Target run status.
        """
        super().__init__(
            f"Experiment run {experiment_run_id} cannot transition from "
            f"{current} to {target}"
        )


class ExperimentRun(DomainModel):
    """Experiment run."""

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

    @property
    def settled(self) -> bool:
        """Whether the run reached a terminal status.

        Returns:
            Whether the run reached a terminal status.
        """
        return self.status in TERMINAL_RUN_STATUSES

    def start(self, now: datetime) -> None:
        """Move the run to running and stamp started_at.

        Args:
            now: Current time.
        """
        self.status = ExperimentRunStatus.RUNNING
        self.started_at = now

    def cancel(self) -> None:
        """Move a running run to canceling.

        Raises:
            IllegalExperimentRunStatusTransition: The run is not running.
        """
        if self.status is not ExperimentRunStatus.RUNNING:
            raise IllegalExperimentRunStatusTransition(
                self.id, self.status, ExperimentRunStatus.CANCELING
            )
        self.status = ExperimentRunStatus.CANCELING

    def finalize(
        self, status: ExperimentRunStatus, error: str | None, now: datetime
    ) -> None:
        """Move a running or canceling run to a terminal status and stamp ended_at.

        Args:
            status: Terminal status to finalize on.
            error: Error of the finalized run, if any.
            now: Current time.

        Raises:
            IllegalExperimentRunStatusTransition: ``status`` is not terminal
                or the run already settled.
        """
        if status not in TERMINAL_RUN_STATUSES or self.settled:
            raise IllegalExperimentRunStatusTransition(self.id, self.status, status)
        self.status = status
        self.error = error
        self.ended_at = now
