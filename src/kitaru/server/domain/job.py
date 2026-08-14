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
"""Job entity and errors."""

import uuid
from datetime import datetime

from pydantic import Field

from kitaru.api_models.v1.job import JobKind, JobStatus
from kitaru.server.domain.base import ConflictError, DomainModel, NotFoundError
from kitaru.server.domain.ids import uuid7

TERMINAL_JOB_STATUSES = frozenset(
    {JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELED}
)


class JobNotFound(NotFoundError):
    """Raised when a job lookup does not resolve."""

    def __init__(self, job_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            job_id: Id of the missing job.
        """
        super().__init__(f"Job {job_id} was not found")


class IllegalJobStatusTransition(ConflictError):
    """Raised when a job status transition is not allowed."""

    def __init__(
        self, job_id: uuid.UUID, current: JobStatus, target: JobStatus
    ) -> None:
        """Initialize the error.

        Args:
            job_id: Id of the job.
            current: Current job status.
            target: Target job status.
        """
        super().__init__(f"Job {job_id} cannot transition from {current} to {target}")


class JobAlreadySettled(ConflictError):
    """Raised when an operation requires a job that has not settled yet."""

    def __init__(self, job_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            job_id: Id of the settled job.
        """
        super().__init__(f"Job {job_id} has already settled")


class Job(DomainModel):
    """Job."""

    id: uuid.UUID = Field(default_factory=uuid7)
    owner_id: uuid.UUID
    kind: JobKind
    status: JobStatus = JobStatus.PENDING
    provisional: bool = False
    cancel_requested_at: datetime | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    error: str | None = None
    created: datetime | None = None
    updated: datetime | None = None

    @property
    def settled(self) -> bool:
        """Whether the job reached a terminal status.

        Returns:
            Whether the job reached a terminal status.
        """
        return self.status in TERMINAL_JOB_STATUSES

    def start(self, now: datetime) -> None:
        """Move a pending job to running and stamp started_at.

        Args:
            now: Current time.

        Raises:
            IllegalJobStatusTransition: The job is not pending.
        """
        if self.status is not JobStatus.PENDING:
            raise IllegalJobStatusTransition(self.id, self.status, JobStatus.RUNNING)
        self.status = JobStatus.RUNNING
        self.started_at = now

    def settle(self, status: JobStatus, error: str | None, now: datetime) -> None:
        """Move a pending or running job to a terminal status and stamp ended_at.

        Args:
            status: Terminal status to settle on.
            error: Error of the first counted task failure.
            now: Current time.

        Raises:
            IllegalJobStatusTransition: ``status`` is not terminal or the job
                already settled.
        """
        if status not in TERMINAL_JOB_STATUSES or self.settled:
            raise IllegalJobStatusTransition(self.id, self.status, status)
        self.status = status
        self.error = error
        self.ended_at = now

    def request_cancel(self, now: datetime) -> None:
        """Stamp cancel_requested_at unless it is already set.

        Args:
            now: Current time.
        """
        if self.cancel_requested_at is None:
            self.cancel_requested_at = now

    def finalize(self) -> None:
        """Clear the provisional flag.

        Raises:
            JobAlreadySettled: The job already reached a terminal status.
        """
        if self.settled:
            raise JobAlreadySettled(self.id)
        self.provisional = False
