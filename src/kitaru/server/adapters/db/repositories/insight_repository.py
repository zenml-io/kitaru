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
    INSIGHT_ANALYZER_VERSION_ID_FOREIGN_KEY,
    INSIGHT_TASK_ID_FOREIGN_KEY,
    InsightORM,
)
from kitaru.server.adapters.db.orm.plugin import PluginORM, PluginVersionORM
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.application.interfaces.insight_repository import InsightWithAnalyzer
from kitaru.server.application.models.insight import InsightFilter
from kitaru.server.domain.agent import AgentNotFound
from kitaru.server.domain.base import NotFoundError
from kitaru.server.domain.insight import Insight, InsightNotFound
from kitaru.server.domain.plugin import PluginVersionIdNotFound
from kitaru.server.domain.task import TaskNotFound

AnalyzerInfo = tuple[str, int]

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

    async def _load_analyzers(
        self, version_ids: set[uuid.UUID]
    ) -> dict[uuid.UUID, AnalyzerInfo]:
        """Bulk-load analyzer name and version for a set of plugin version ids.

        Args:
            version_ids: Ids of the referenced plugin versions.

        Returns:
            Analyzer (name, version) pairs keyed by plugin version id, missing
            ids omitted.
        """
        if not version_ids:
            return {}
        statement = (
            select(PluginVersionORM.id, PluginORM.name, PluginVersionORM.version)
            .join(PluginORM, PluginORM.id == PluginVersionORM.plugin_id)
            .where(PluginVersionORM.id.in_(version_ids))
        )
        rows = (await self._session.execute(statement)).all()
        return {row.id: (row.name, row.version) for row in rows}

    async def create_many(self, insights: list[Insight]) -> list[Insight]:
        """Persist a batch of new insights in one transaction.

        Args:
            insights: Insights to store, in input order.

        Raises:
            AgentNotFound: No agent has the insights' agent id.
            PluginVersionIdNotFound: No plugin version has the analyzer
                version id, including one deleted concurrently with the task
                it analyzed.
            TaskNotFound: No task has the task id, including one deleted
                concurrently with its recording.

        Returns:
            Stored insights in input order, with timestamps set.
        """
        if not insights:
            return []
        rows = [InsightORM.from_domain(insight) for insight in insights]

        def _analyzer_version_not_found() -> PluginVersionIdNotFound:
            analyzer_version_id = insights[0].analyzer_version_id
            assert analyzer_version_id is not None
            return PluginVersionIdNotFound(analyzer_version_id)

        def _task_not_found() -> TaskNotFound:
            task_id = insights[0].task_id
            assert task_id is not None
            return TaskNotFound(task_id)

        await self._add_all(
            rows,
            {
                INSIGHT_AGENT_ID_FOREIGN_KEY: lambda: AgentNotFound(
                    insights[0].agent_id
                ),
                INSIGHT_ANALYZER_VERSION_ID_FOREIGN_KEY: _analyzer_version_not_found,
                INSIGHT_TASK_ID_FOREIGN_KEY: _task_not_found,
            },
        )
        return [row.to_domain() for row in rows]

    async def get(self, insight_id: uuid.UUID) -> InsightWithAnalyzer:
        """Load an insight by id, joined with its analyzer name and version.

        One statement carries both, unlike the bulk lookup ``query()`` uses,
        since there is exactly one row to join here.

        Args:
            insight_id: Id of the insight.

        Raises:
            InsightNotFound: No insight has this id.

        Returns:
            Stored insight paired with its analyzer name and version, both
            ``None`` on a manual insight.
        """
        statement = (
            select(InsightORM, PluginORM.name, PluginVersionORM.version)
            .outerjoin(
                PluginVersionORM,
                PluginVersionORM.id == InsightORM.analyzer_version_id,
            )
            .outerjoin(PluginORM, PluginORM.id == PluginVersionORM.plugin_id)
            .where(InsightORM.id == insight_id)
        )
        row = (await self._session.execute(statement)).one_or_none()
        if row is None:
            raise InsightNotFound(insight_id)
        insight_row, analyzer_name, analyzer_version = row
        return InsightWithAnalyzer(
            insight_row.to_domain(), analyzer_name, analyzer_version
        )

    async def query(
        self, insight_filter: InsightFilter
    ) -> tuple[list[InsightWithAnalyzer], str | None]:
        """Query insights matching a filter.

        Args:
            insight_filter: Filter and pagination parameters.

        Returns:
            Page of matching insights, each paired with its analyzer name and
            version, and the next cursor.
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
        version_ids = {
            row.analyzer_version_id
            for row in rows
            if row.analyzer_version_id is not None
        }
        analyzers = await self._load_analyzers(version_ids)
        items = [
            InsightWithAnalyzer(
                row.to_domain(), *analyzers.get(row.analyzer_version_id, (None, None))
            )
            for row in rows
        ]
        return items, next_cursor

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
