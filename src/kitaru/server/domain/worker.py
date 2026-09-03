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

from kitaru.api_models.v1.worker import WorkerClaim, WorkerRuntime, WorkerScope
from kitaru.server.domain.base import (
    DomainModel,
    ForbiddenError,
    NotFoundError,
    UpgradeRequiredError,
)
from kitaru.server.domain.ids import uuid7
from kitaru.server.domain.names import Name
from kitaru.server.domain.task import AgentTask, Task


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


class WorkerClientUnsupported(UpgradeRequiredError):
    """Raised when a worker registers from a client below the supported version."""

    def __init__(self, client_version: str, last_unsupported_version: str) -> None:
        """Initialize the error.

        Args:
            client_version: Version the client reported.
            last_unsupported_version: Newest version not allowed to register.
        """
        super().__init__(
            f"Registering a worker requires kitaru newer than "
            f"{last_unsupported_version}, this client reports {client_version}. "
            "Upgrade the worker"
        )


class WorkerAccessDenied(ForbiddenError):
    """Raised when the caller's credential does not authorize this worker."""

    def __init__(self, worker_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            worker_id: Id of the worker.
        """
        super().__init__(f"Worker {worker_id} is not accessible to this caller")


def _claim_matches(claim: WorkerClaim, task: Task) -> bool:
    """Report whether one claim covers a task.

    Args:
        claim: Claim from the worker's scope.
        task: Candidate task.

    Returns:
        Whether the claim covers the task.
    """
    if claim.agent_version_id is not None:
        return (
            isinstance(task, AgentTask)
            and task.agent_version_id == claim.agent_version_id
        )
    return task.kind is claim.kind


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

    def is_live(self, now: datetime, timeout_seconds: int) -> bool:
        """Report whether the worker was seen within the liveness window.

        Args:
            now: Current time.
            timeout_seconds: Liveness window in seconds.

        Returns:
            Whether the worker is considered alive.
        """
        return (now - self.last_seen_at).total_seconds() <= timeout_seconds

    def covers(self, task: Task) -> bool:
        """Report whether the worker's scope claims the task.

        Mirrors the claim conditions of the SQL task repository, so the two
        must change together.

        Args:
            task: Candidate task.

        Returns:
            Whether the worker would claim the task.
        """
        if self.scope.job_id is not None and task.job_id != self.scope.job_id:
            return False
        for selector in self.scope.selectors or []:
            if selector.key not in task.labels:
                if selector.required:
                    return False
                continue
            if task.labels[selector.key] not in selector.values:
                return False
        return any(_claim_matches(claim, task) for claim in self.scope.claims)
