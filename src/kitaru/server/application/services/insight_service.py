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
"""Insight use cases."""

import uuid

from kitaru.analytics.events import AnalyticsEvent
from kitaru.server.application.interfaces.agent_repository import AgentRepository
from kitaru.server.application.interfaces.import_repository import ImportRepository
from kitaru.server.application.interfaces.insight_repository import InsightRepository
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.insight import (
    InsightCreate,
    InsightFilter,
    InsightUpdate,
)
from kitaru.server.application.services.analytics_events import (
    build_insight_created_properties,
)
from kitaru.server.application.services.server_analytics import ServerAnalytics
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.insight import Insight


class InsightService:
    """Insight use cases."""

    def __init__(
        self,
        repository: InsightRepository,
        agent_repository: AgentRepository,
        import_repository: ImportRepository,
        analytics: ServerAnalytics | None = None,
    ) -> None:
        """Initialize the service.

        Args:
            repository: Insight repository.
            agent_repository: Agent repository, to validate the insights'
                agent exists.
            import_repository: Import repository for association validation.
            analytics: Analytics tracker, None skips tracking.
        """
        self._repository = repository
        self._agents = agent_repository
        self._imports = import_repository
        self._analytics = analytics

    async def create_insights(
        self, command: InsightCreate, actor: AuthContext
    ) -> list[Insight]:
        """Create a batch of insights for one agent in one shot.

        Args:
            command: Agent and insights to create, in input order.
            actor: Caller context.

        Raises:
            AgentNotFound: No agent has the command's agent id.
            ImportNotFound: No import has the associated import id.
            ValidationError: The import belongs to another agent.

        Returns:
            Created insights in input order.
        """
        await self._agents.get(command.agent_id)
        if command.import_id is not None:
            await self._validate_import_agent(command.import_id, command.agent_id)
        insights = [
            Insight(
                owner_id=actor.account.id,
                agent_id=command.agent_id,
                import_id=command.import_id,
                name=item.name,
                title=item.title,
                description=item.description,
                data=item.data,
                metadata=item.metadata,
            )
            for item in command.insights
        ]
        insights = await self._repository.create_many(insights)
        if self._analytics is not None:
            for insight in insights:
                self._analytics.track(
                    actor.account.id,
                    AnalyticsEvent.INSIGHT_CREATED,
                    build_insight_created_properties(insight),
                )
        return insights

    async def get_insight(self, insight_id: uuid.UUID, actor: AuthContext) -> Insight:
        """Get an insight by id.

        Args:
            insight_id: Id of the insight.
            actor: Caller context.

        Raises:
            InsightNotFound: No insight has this id.

        Returns:
            Stored insight.
        """
        _ = actor
        return await self._repository.get(insight_id)

    async def list_insights(
        self, insight_filter: InsightFilter, actor: AuthContext
    ) -> tuple[list[Insight], str | None]:
        """List insights matching a filter.

        Args:
            insight_filter: Filter and pagination parameters.
            actor: Caller context.

        Returns:
            Page of matching insights and the next cursor.
        """
        _ = actor
        return await self._repository.query(insight_filter)

    async def update_insight(
        self, insight_id: uuid.UUID, command: InsightUpdate, actor: AuthContext
    ) -> Insight:
        """Partially update an insight's title, description, and import association.

        Args:
            insight_id: Id of the insight.
            command: Fields to change, built from the request's set fields.
            actor: Caller context.

        Raises:
            InsightNotFound: No insight has this id.
            ImportNotFound: No import has the associated import id.
            ValidationError: The title is cleared, the import belongs to another
                agent, or an analyzer-generated association is changed.

        Returns:
            Updated insight.
        """
        _ = actor
        insight = await self._repository.get(insight_id, exclusive=True)
        fields = command.model_fields_set
        if "import_id" in fields:
            insight.update_import_id(command.import_id)
            if command.import_id is not None:
                await self._validate_import_agent(command.import_id, insight.agent_id)
        if "title" in fields:
            if command.title is None:
                raise ValidationError("Insight title cannot be cleared")
            insight.update_title(command.title)
        if "description" in fields:
            insight.update_description(command.description)
        return await self._repository.update(insight)

    async def delete_insight(self, insight_id: uuid.UUID, actor: AuthContext) -> None:
        """Delete an insight.

        Args:
            insight_id: Id of the insight.
            actor: Caller context.

        Raises:
            InsightNotFound: No insight has this id.
        """
        _ = actor
        await self._repository.delete(insight_id)

    async def _validate_import_agent(
        self, import_id: uuid.UUID, agent_id: uuid.UUID
    ) -> None:
        """Require an import to belong to the specified agent.

        Args:
            import_id: Associated import.
            agent_id: Agent the insight belongs to.

        Raises:
            ImportNotFound: No import has this id.
            ValidationError: The import belongs to another agent.
        """
        import_ = await self._imports.get(import_id)
        if import_.agent_id != agent_id:
            raise ValidationError(
                f"Import {import_id} does not belong to agent {agent_id}"
            )
