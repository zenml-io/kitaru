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
"""Import entity and errors."""

import uuid
from datetime import datetime
from typing import Any

from pydantic import Field

from kitaru.api_models.v1.imports import ImportStats
from kitaru.server.domain.base import DomainModel, NotFoundError
from kitaru.server.domain.ids import uuid7
from kitaru.server.domain.replay_config import EvaluatorConfig


class ImportNotFound(NotFoundError):
    """Raised when an import lookup does not resolve."""

    def __init__(self, import_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            import_id: Id of the missing import.
        """
        super().__init__(f"Import {import_id} was not found")


class ImportWithoutImporterVersion(NotFoundError):
    """Raised when an import whose importer version was deleted is asked to run."""

    def __init__(self, import_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            import_id: Id of the import.
        """
        super().__init__(f"Import {import_id} no longer names an importer version")


class Import(DomainModel):
    """Import."""

    id: uuid.UUID = Field(default_factory=uuid7)
    owner_id: uuid.UUID
    job_id: uuid.UUID | None = None
    agent_id: uuid.UUID
    agent_version_id: uuid.UUID | None = None
    importer_version_id: uuid.UUID | None = None
    payload_blob_id: uuid.UUID
    params: dict[str, Any] = Field(default_factory=dict)
    evaluators: list[EvaluatorConfig] = Field(default_factory=list)
    stats: ImportStats | None = None
    error: str | None = None
    created: datetime | None = None
    updated: datetime | None = None

    def record_stats(self, stats: ImportStats) -> None:
        """Set the stats of a completed import.

        Args:
            stats: Import stats.
        """
        self.stats = stats

    def record_error(self, error: str | None) -> None:
        """Set the error of a failed import.

        Args:
            error: Error from the task that ran the import.
        """
        self.error = error
