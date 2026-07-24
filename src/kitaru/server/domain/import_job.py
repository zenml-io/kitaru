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
"""Trace import job entity and errors."""

import uuid
from datetime import UTC, datetime
from enum import StrEnum

from pydantic import Field

from kitaru.server.base import FrozenModel
from kitaru.server.domain.base import (
    ConflictError,
    DomainModel,
    NotFoundError,
    ValidationError,
)
from kitaru.server.domain.ids import uuid7


class ImportJobStatus(StrEnum):
    """Import job lifecycle status."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    COMPLETED_WITH_ERRORS = "completed_with_errors"
    FAILED = "failed"


class ImportJobNotFound(NotFoundError):
    """Raised when an import job lookup does not resolve."""

    def __init__(self, job_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            job_id: Id of the missing job.
        """
        super().__init__(f"Import job {job_id} was not found")


class ImporterNotFound(NotFoundError):
    """Raised when an importer id is unavailable."""

    def __init__(self, importer_id: str) -> None:
        """Initialize the error.

        Args:
            importer_id: Missing importer id.
        """
        super().__init__(f"Importer '{importer_id}' was not found")


class ImportJobNotPending(ConflictError):
    """Raised when a worker tries to start a non-pending job."""

    def __init__(self, job_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            job_id: Id of the non-pending job.
        """
        super().__init__(f"Import job {job_id} is not pending")


class InvalidImport(ValidationError):
    """Raised when an import file or normalized session is invalid."""


class ImportJobError(FrozenModel):
    """One source session that failed to import."""

    source_id: str | None = None
    message: str


class ImportJob(DomainModel):
    """Background trace import job."""

    id: uuid.UUID = Field(default_factory=uuid7)
    owner_id: uuid.UUID
    agent_version_id: uuid.UUID
    importer_id: str
    importer_version: str
    source_instance: str | None = None
    filename: str
    content: bytes | None = None
    status: ImportJobStatus = ImportJobStatus.PENDING
    worker_id: str | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    source_session_count: int = 0
    imported_count: int = 0
    deduplicated_count: int = 0
    failed_count: int = 0
    session_ids: list[uuid.UUID] = Field(default_factory=list)
    errors: list[ImportJobError] = Field(default_factory=list)
    error: str | None = None
    created: datetime | None = None
    updated: datetime | None = None

    def start(self, worker_id: str) -> None:
        """Mark the job running.

        Args:
            worker_id: Worker claiming the job.

        Raises:
            ImportJobNotPending: The job is not pending.
        """
        if self.status is not ImportJobStatus.PENDING:
            raise ImportJobNotPending(self.id)
        self.status = ImportJobStatus.RUNNING
        self.worker_id = worker_id
        self.started_at = datetime.now(UTC)

    def complete(
        self,
        source_session_count: int,
        imported_count: int,
        deduplicated_count: int,
        session_ids: list[uuid.UUID],
        errors: list[ImportJobError],
    ) -> None:
        """Complete the job and discard its temporary upload.

        Args:
            source_session_count: Number of normalized source sessions.
            imported_count: Number of new sessions created.
            deduplicated_count: Number of exact matches reused.
            session_ids: Created and reused session ids.
            errors: Per-session import errors.
        """
        self.source_session_count = source_session_count
        self.imported_count = imported_count
        self.deduplicated_count = deduplicated_count
        self.failed_count = len(errors)
        self.session_ids = session_ids
        self.errors = errors
        self.content = None
        self.ended_at = datetime.now(UTC)
        self.status = (
            ImportJobStatus.COMPLETED_WITH_ERRORS
            if errors
            else ImportJobStatus.COMPLETED
        )

    def fail(self, error: str) -> None:
        """Fail the whole job and discard its temporary upload.

        Args:
            error: Failure message.
        """
        self.status = ImportJobStatus.FAILED
        self.error = error
        self.content = None
        self.ended_at = datetime.now(UTC)
