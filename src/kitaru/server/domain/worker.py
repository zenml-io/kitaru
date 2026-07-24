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
from datetime import UTC, datetime, timedelta
from typing import Any

from pydantic import Field

from kitaru.server.domain.base import (
    ConflictError,
    DomainModel,
    NotFoundError,
)
from kitaru.server.domain.ids import uuid7
from kitaru.server.domain.names import Name


class WorkerNotFound(NotFoundError):
    """Raised when a worker lookup does not resolve."""

    def __init__(self, worker: uuid.UUID | str) -> None:
        """Initialize the error.

        Args:
            worker: Id or name of the missing worker.
        """
        super().__init__(f"Worker {worker} was not found")


class DuplicateWorkerName(ConflictError):
    """Raised when a worker name is already registered."""

    def __init__(self, name: str) -> None:
        """Initialize the error.

        Args:
            name: Name that is already registered.
        """
        super().__init__(f"Worker name '{name}' is already registered")


class Worker(DomainModel):
    """Worker."""

    id: uuid.UUID = Field(default_factory=uuid7)
    owner_id: uuid.UUID
    name: Name
    agent_ids: list[uuid.UUID] = Field(default_factory=list)
    last_seen_at: datetime
    metadata: dict[str, Any] = Field(default_factory=dict)
    created: datetime | None = None
    updated: datetime | None = None

    def refresh(
        self,
        agent_ids: list[uuid.UUID],
        metadata: dict[str, Any],
    ) -> None:
        """Replace the served agents and metadata and record the sighting.

        Args:
            agent_ids: Ids of the served agents, empty means all agents.
            metadata: Worker metadata.
        """
        self.agent_ids = agent_ids
        self.metadata = metadata
        self.last_seen_at = datetime.now(UTC)

    def is_live(self, timeout_seconds: int) -> bool:
        """Report whether the worker was seen within the liveness timeout.

        Args:
            timeout_seconds: Seconds after which a worker counts as dead.

        Returns:
            ``True`` when the worker was last seen within the timeout of
            now.
        """
        return self.last_seen_at >= datetime.now(UTC) - timedelta(
            seconds=timeout_seconds
        )
