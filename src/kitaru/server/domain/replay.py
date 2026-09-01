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

from pydantic import Field

from kitaru.api_models.v1.replay import BaselineEvaluationMode, ReplayStatus
from kitaru.server.domain.base import (
    ConflictError,
    DomainModel,
    ForbiddenError,
    NotFoundError,
)
from kitaru.server.domain.ids import uuid7

TERMINAL_REPLAY_STATUSES = frozenset(
    {ReplayStatus.COMPLETED, ReplayStatus.FAILED, ReplayStatus.CANCELED}
)


class ReplayNotFound(NotFoundError):
    """Raised when a replay lookup does not resolve."""

    def __init__(self, replay_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            replay_id: Id of the missing replay.
        """
        super().__init__(f"Replay {replay_id} was not found")


class ReplayAccessDenied(ForbiddenError):
    """Raised when the caller's credential does not authorize this replay."""

    def __init__(self, replay_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            replay_id: Id of the replay.
        """
        super().__init__(f"Replay {replay_id} is not accessible to this caller")


class IllegalReplayStatusTransition(ConflictError):
    """Raised when a replay status transition is not allowed."""

    def __init__(
        self, replay_id: uuid.UUID, current: ReplayStatus, target: ReplayStatus
    ) -> None:
        """Initialize the error.

        Args:
            replay_id: Id of the replay.
            current: Current replay status.
            target: Target replay status.
        """
        super().__init__(
            f"Replay {replay_id} cannot transition from {current} to {target}"
        )


class ReplayAlreadyExistsForJob(ConflictError):
    """Raised when a job already has a replay row."""

    def __init__(self, job_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            job_id: Id of the job.
        """
        super().__init__(f"Job {job_id} already has a replay")


class DuplicateReplayForBaseline(ConflictError):
    """Raised when a run already holds a replay for a baseline session."""

    def __init__(
        self, experiment_run_id: uuid.UUID | None, baseline_session_id: uuid.UUID
    ) -> None:
        """Initialize the error.

        Args:
            experiment_run_id: Id of the run.
            baseline_session_id: Id of the baseline session.
        """
        super().__init__(
            f"Experiment run {experiment_run_id} already holds a replay for "
            f"baseline session {baseline_session_id}"
        )


class ReplayInUse(ConflictError):
    """Raised when a replay belongs to an experiment run."""

    def __init__(self, replay_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            replay_id: Id of the replay in use.
        """
        super().__init__(f"Replay {replay_id} is in use by an experiment run")


class Replay(DomainModel):
    """Replay."""

    id: uuid.UUID = Field(default_factory=uuid7)
    owner_id: uuid.UUID
    job_id: uuid.UUID | None = None
    experiment_run_id: uuid.UUID | None = None
    replay_config_id: uuid.UUID
    baseline_session_id: uuid.UUID
    result_session_id: uuid.UUID | None = None
    baseline_evaluation_mode: BaselineEvaluationMode = BaselineEvaluationMode.IF_MISSING
    status: ReplayStatus = ReplayStatus.PENDING
    error: str | None = None
    created: datetime | None = None
    updated: datetime | None = None

    @property
    def settled(self) -> bool:
        """Whether the replay reached a terminal status.

        Returns:
            Whether the replay reached a terminal status.
        """
        return self.status in TERMINAL_REPLAY_STATUSES

    def start_evaluating(self) -> None:
        """Move a pending replay to evaluating.

        Raises:
            IllegalReplayStatusTransition: The replay is not pending.
        """
        if self.status is not ReplayStatus.PENDING:
            raise IllegalReplayStatusTransition(
                self.id, self.status, ReplayStatus.EVALUATING
            )
        self.status = ReplayStatus.EVALUATING

    def complete(self) -> None:
        """Move a pending or evaluating replay to completed.

        Raises:
            IllegalReplayStatusTransition: The replay already settled.
        """
        if self.settled:
            raise IllegalReplayStatusTransition(
                self.id, self.status, ReplayStatus.COMPLETED
            )
        self.status = ReplayStatus.COMPLETED

    def fail(self, error: str | None) -> None:
        """Move a pending or evaluating replay to failed.

        Args:
            error: Error from the job that ran the replay.

        Raises:
            IllegalReplayStatusTransition: The replay already settled.
        """
        if self.settled:
            raise IllegalReplayStatusTransition(
                self.id, self.status, ReplayStatus.FAILED
            )
        self.status = ReplayStatus.FAILED
        self.error = error

    def cancel(self) -> None:
        """Move a pending or evaluating replay to canceled.

        Raises:
            IllegalReplayStatusTransition: The replay already settled.
        """
        if self.settled:
            raise IllegalReplayStatusTransition(
                self.id, self.status, ReplayStatus.CANCELED
            )
        self.status = ReplayStatus.CANCELED

    def link_result_session(self, session_id: uuid.UUID) -> None:
        """Link the session this replay produced.

        Args:
            session_id: Id of the produced session.
        """
        self.result_session_id = session_id

    def unlink_result_session(self) -> None:
        """Clear the session this replay produced."""
        self.result_session_id = None
