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
"""Worker pool entity and errors."""

import uuid
from datetime import datetime

from pydantic import Field, field_validator

from kitaru.api_models.v1.worker import WorkerScope
from kitaru.server.domain.base import (
    ConflictError,
    DomainModel,
    NotFoundError,
    ValidationError,
)
from kitaru.server.domain.ids import uuid7
from kitaru.server.domain.names import Name


class WorkerPoolNotFound(NotFoundError):
    """Raised when a worker pool lookup does not resolve."""

    def __init__(self, worker_pool: uuid.UUID | str) -> None:
        """Initialize the error.

        Args:
            worker_pool: Id or name of the missing worker pool.
        """
        super().__init__(f"Worker pool {worker_pool} was not found")


class DuplicateWorkerPoolName(ConflictError):
    """Raised when a worker pool name is already registered."""

    def __init__(self, name: str) -> None:
        """Initialize the error.

        Args:
            name: Name that is already registered.
        """
        super().__init__(f"Worker pool name '{name}' is already registered")


class WorkerPoolScopePinsJob(ValidationError):
    """Raised when a worker pool's scope names a job."""

    def __init__(self) -> None:
        """Initialize the error."""
        super().__init__("Worker pool scope must not pin a job")


class WorkerPool(DomainModel):
    """Worker pool."""

    id: uuid.UUID = Field(default_factory=uuid7)
    owner_id: uuid.UUID
    name: Name
    scope: WorkerScope
    created: datetime | None = None
    updated: datetime | None = None

    @field_validator("scope")
    @classmethod
    def _forbid_job_pin(cls, value: WorkerScope) -> WorkerScope:
        """Reject a scope that pins a job.

        Args:
            value: Scope to check.

        Raises:
            WorkerPoolScopePinsJob: The scope names a job.

        Returns:
            Validated scope.
        """
        if value.job_id is not None:
            raise WorkerPoolScopePinsJob()
        return value

    def update_name(self, name: str) -> None:
        """Set a new worker pool name.

        Args:
            name: New name.
        """
        self.name = name

    def update_scope(self, scope: WorkerScope) -> None:
        """Set a new claim scope.

        Args:
            scope: New scope.
        """
        self.scope = scope
