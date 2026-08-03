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
"""Worker entity and errors."""

import uuid
from datetime import datetime

from pydantic import Field

from kitaru.api_models.v1.task import WorkerScope
from kitaru.api_models.v1.worker import WorkerRuntime
from kitaru.server.domain.base import DomainModel, ForbiddenError, NotFoundError
from kitaru.server.domain.ids import uuid7
from kitaru.server.domain.names import Name


class WorkerNotFound(NotFoundError):
    """Raised when a worker lookup does not resolve."""

    def __init__(self, worker_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            worker_id: Id of the missing worker.
        """
        super().__init__(f"Worker {worker_id} was not found")


class WorkerCredentialRequired(ForbiddenError):
    """Raised when an operation requires the caller to hold a worker token."""

    def __init__(self) -> None:
        """Initialize the error."""
        super().__init__("This operation requires a worker credential")


class WorkerAccessDenied(ForbiddenError):
    """Raised when the caller's credential does not authorize this worker."""

    def __init__(self, worker_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            worker_id: Id of the worker.
        """
        super().__init__(f"Worker {worker_id} is not accessible to this caller")


class Worker(DomainModel):
    """Worker."""

    id: uuid.UUID = Field(default_factory=uuid7)
    owner_id: uuid.UUID
    name: Name
    scope: WorkerScope
    runtime: WorkerRuntime
    last_seen_at: datetime
    metadata: dict[str, str] = Field(default_factory=dict)
    created: datetime | None = None
    updated: datetime | None = None

    def refresh(
        self,
        scope: WorkerScope,
        runtime: WorkerRuntime,
        metadata: dict[str, str],
        now: datetime,
    ) -> None:
        """Replace the reported scope, runtime, and metadata, and stamp last_seen_at.

        Args:
            scope: New claim scope.
            runtime: New reported runtime.
            metadata: New metadata.
            now: Current time.
        """
        self.scope = scope
        self.runtime = runtime
        self.metadata = metadata
        self.last_seen_at = now

    def is_live(self, now: datetime, timeout_seconds: int) -> bool:
        """Report whether the worker was seen within the liveness window.

        Args:
            now: Current time.
            timeout_seconds: Liveness window in seconds.

        Returns:
            Whether the worker is considered alive.
        """
        return (now - self.last_seen_at).total_seconds() <= timeout_seconds
