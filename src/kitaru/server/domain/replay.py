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
"""Replay entity and errors."""

import uuid
from datetime import datetime
from typing import Any

from pydantic import Field

from kitaru.server.domain.base import (
    ConflictError,
    DomainModel,
    NotFoundError,
)
from kitaru.server.domain.ids import uuid7
from kitaru.server.domain.replay_config import ScoringResult


class ReplayNotFound(NotFoundError):
    """Raised when a replay lookup does not resolve."""

    def __init__(self, replay_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            replay_id: Id of the missing replay.
        """
        super().__init__(f"Replay {replay_id} was not found")


class ReplayJobNotFound(NotFoundError):
    """Raised when a job has no replay."""

    def __init__(self, job_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            job_id: Id of the job.
        """
        super().__init__(f"Job {job_id} has no replay")


class DuplicateReplayJob(ConflictError):
    """Raised when a job already has a replay."""

    def __init__(self, job_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            job_id: Id of the job.
        """
        super().__init__(f"Job {job_id} already has a replay")


class ReplaySettled(ConflictError):
    """Raised when an operation requires an unsettled replay."""

    def __init__(self, replay_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            replay_id: Id of the replay.
        """
        super().__init__(f"Replay {replay_id} is already settled")


class Replay(DomainModel):
    """Replay."""

    id: uuid.UUID = Field(default_factory=uuid7)
    owner_id: uuid.UUID
    job_id: uuid.UUID
    experiment_run_id: uuid.UUID | None = None
    replay_config_id: uuid.UUID
    input_session_id: uuid.UUID
    passed: bool | None = None
    score: float | None = None
    scores: dict[str, float] | None = None
    diff: dict[str, Any] | None = None
    error: str | None = None
    created: datetime | None = None
    updated: datetime | None = None

    @property
    def settled(self) -> bool:
        """Whether the replay carries a verdict or an error.

        Returns:
            Whether the replay carries a verdict or an error.
        """
        return self.passed is not None or self.error is not None

    def complete(self, result: ScoringResult, diff: dict[str, Any] | None) -> None:
        """Settle the replay with its scoring result.

        Args:
            result: Scoring result computed from the score jobs.
            diff: Diff summary.

        Raises:
            ReplaySettled: The replay already carries a verdict or an
                error.
        """
        if self.settled:
            raise ReplaySettled(self.id)
        self.passed = result.passed
        self.score = result.score
        self.scores = result.scores
        self.diff = diff

    def fail(self, error: str) -> None:
        """Settle the replay with an error.

        Args:
            error: Error message.

        Raises:
            ReplaySettled: The replay already carries a verdict or an
                error.
        """
        if self.settled:
            raise ReplaySettled(self.id)
        self.error = error
