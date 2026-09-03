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
"""Insight entity and errors."""

import uuid
from datetime import datetime
from typing import Any

from pydantic import Field

from kitaru.api_models.v1.insight import InsightData
from kitaru.server.domain.base import DomainModel, NotFoundError
from kitaru.server.domain.ids import uuid7
from kitaru.server.domain.names import MAX_NAME_LENGTH


class InsightNotFound(NotFoundError):
    """Raised when an insight lookup does not resolve."""

    def __init__(self, insight_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            insight_id: Id of the missing insight.
        """
        super().__init__(f"Insight {insight_id} was not found")


class Insight(DomainModel):
    """Insight."""

    id: uuid.UUID = Field(default_factory=uuid7)
    owner_id: uuid.UUID
    agent_id: uuid.UUID
    title: str = Field(max_length=MAX_NAME_LENGTH)
    description: str | None = None
    data: InsightData
    metadata: dict[str, Any] = Field(default_factory=dict)
    created: datetime | None = None
    updated: datetime | None = None

    def update_title(self, title: str) -> None:
        """Set a new insight title.

        Args:
            title: New title.
        """
        self.title = title

    def update_description(self, description: str | None) -> None:
        """Set a new insight description.

        Args:
            description: New description.
        """
        self.description = description
