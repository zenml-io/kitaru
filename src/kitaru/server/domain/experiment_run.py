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
from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import Field

from kitaru.server.base import FrozenModel
from kitaru.server.domain.base import (
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
