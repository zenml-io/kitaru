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
"""SQL insight repository."""

import uuid
from collections.abc import Mapping

from sqlalchemy import select

from kitaru.server.adapters.db.filtering import FilterBinding, compile_filter_expression
from kitaru.server.adapters.db.orm.insight import (
    INSIGHT_AGENT_ID_FOREIGN_KEY,
    InsightORM,
)
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.application.models.insight import InsightFilter
from kitaru.server.domain.agent import AgentNotFound
from kitaru.server.domain.base import NotFoundError
from kitaru.server.domain.insight import Insight, InsightNotFound

INSIGHT_FILTER_BINDINGS: Mapping[str, FilterBinding] = {
    "id": InsightORM.id,
    "agent_id": InsightORM.agent_id,
    "name": InsightORM.name,
    "type": InsightORM.type,
}


class SQLInsightRepository(BaseSQLRepository[InsightORM]):
    """Insight repository backed by the application database."""

    orm_class = InsightORM

    def _not_found(self, entity_id: uuid.UUID) -> NotFoundError:
        """Build the not-found error for an id.

        Args:
            entity_id: Id of the missing row.

        Returns:
            Not-found error.
        """
        return InsightNotFound(entity_id)

    async def create_many(self, insights: list[Insight]) -> list[Insight]:
        """Persist a batch of new insights in one transaction.

        Args:
            insights: Insights to store, in input order.

        Raises:
            AgentNotFound: No agent has the insights' agent id.

        Returns:
            Stored insights in input order, with timestamps set.
        """
        if not insights:
            return []
        rows = [InsightORM.from_domain(insight) for insight in insights]
        await self._add_all(
            rows,
            {
                INSIGHT_AGENT_ID_FOREIGN_KEY: lambda: AgentNotFound(
                    insights[0].agent_id
                ),
            },
        )
        return [row.to_domain() for row in rows]

    async def get(self, insight_id: uuid.UUID) -> Insight:
        """Load an insight by id.

        Args:
            insight_id: Id of the insight.

        Raises:
            InsightNotFound: No insight has this id.

        Returns:
            Stored insight.
        """
        row = await self._get_row(insight_id)
        return row.to_domain()

    async def query(
        self, insight_filter: InsightFilter
    ) -> tuple[list[Insight], str | None]:
        """Query insights matching a filter.

        Args:
            insight_filter: Filter and pagination parameters.

        Returns:
            Page of matching insights and the next cursor.
        """
        statement = select(InsightORM)
        if insight_filter.expression is not None:
            statement = statement.where(
                compile_filter_expression(
                    insight_filter.expression, INSIGHT_FILTER_BINDINGS
                )
            )
        rows, next_cursor = await paginate(
            self._session, statement, insight_filter, id_column=InsightORM.id
        )
        return [row.to_domain() for row in rows], next_cursor

    async def update(self, insight: Insight) -> Insight:
        """Persist changes to an existing insight.

        Args:
            insight: Insight with modified fields.

        Raises:
            InsightNotFound: No insight has this id.

        Returns:
            Stored insight with the updated timestamp renewed.
        """
        row = await self._get_row(insight.id)
        row.title = insight.title
        row.description = insight.description
        await self._flush()
        return row.to_domain()

    async def delete(self, insight_id: uuid.UUID) -> None:
        """Delete an insight by id.

        Args:
            insight_id: Id of the insight.

        Raises:
            InsightNotFound: No insight has this id.
        """
        await self._delete_row(insight_id)
