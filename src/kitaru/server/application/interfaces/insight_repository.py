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
"""Insight repository interface."""

import uuid
from typing import Protocol

from kitaru.server.application.models.insight import InsightFilter
from kitaru.server.domain.insight import Insight


class InsightRepository(Protocol):
    """Insight persistence operations."""

    async def create_many(self, insights: list[Insight]) -> list[Insight]:
        """Persist a batch of new insights in one transaction.

        Args:
            insights: Insights to store, in input order.

        Raises:
            AgentNotFound: No agent has the insights' agent id.

        Returns:
            Stored insights in input order, with timestamps set.
        """
        ...

    async def get(self, insight_id: uuid.UUID) -> Insight:
        """Load an insight by id.

        Args:
            insight_id: Id of the insight.

        Raises:
            InsightNotFound: No insight has this id.

        Returns:
            Stored insight.
        """
        ...

    async def query(
        self, insight_filter: InsightFilter
    ) -> tuple[list[Insight], str | None]:
        """Query insights matching a filter.

        Args:
            insight_filter: Filter and pagination parameters.

        Returns:
            Page of matching insights and the next cursor.
        """
        ...

    async def update(self, insight: Insight) -> Insight:
        """Persist changes to an existing insight.

        Args:
            insight: Insight with modified fields.

        Raises:
            InsightNotFound: No insight has this id.

        Returns:
            Stored insight with the updated timestamp renewed.
        """
        ...

    async def delete(self, insight_id: uuid.UUID) -> None:
        """Delete an insight by id.

        Args:
            insight_id: Id of the insight.

        Raises:
            InsightNotFound: No insight has this id.
        """
        ...
