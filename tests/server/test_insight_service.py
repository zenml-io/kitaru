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
"""Tests for insight use cases."""

import uuid
from typing import Any

import pytest

from conftest import FakeAgentRepository, FakeInsightRepository, create_agent
from kitaru.analytics.events import AnalyticsEvent
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.insight import (
    CategoricalInsightData,
    CategoryValue,
    TextInsightData,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.insight import (
    InsightCreate,
    InsightFilter,
    InsightInput,
    InsightUpdate,
)
from kitaru.server.application.services.insight_service import InsightService
from kitaru.server.application.services.server_analytics import ServerAnalytics
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import AgentNotFound
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.insight import InsightNotFound
from kitaru.server.filtering import FilterCondition

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))


class _RecordingAnalytics(ServerAnalytics):
    """Analytics tracker recording track calls instead of buffering them."""

    def __init__(self) -> None:
        """Initialize the tracker."""
        self.tracked: list[tuple[uuid.UUID, AnalyticsEvent | str, dict[str, Any]]] = []

    def track(
        self,
        user_id: uuid.UUID,
        event: AnalyticsEvent | str,
        properties: dict[str, Any] | None = None,
    ) -> None:
        """Record a track call instead of buffering it.

        Args:
            user_id: User id.
            event: Event name.
            properties: Event properties.
        """
        self.tracked.append((user_id, event, properties or {}))


@pytest.fixture
def agent_repository() -> FakeAgentRepository:
    """Provide a fake agent repository."""
    return FakeAgentRepository()


@pytest.fixture
def insight_repository() -> FakeInsightRepository:
    """Provide a fake insight repository."""
    return FakeInsightRepository()


@pytest.fixture
def service(
    insight_repository: FakeInsightRepository, agent_repository: FakeAgentRepository
) -> InsightService:
    """Provide an insight service backed by fake repositories."""
    return InsightService(
        repository=insight_repository, agent_repository=agent_repository
    )


@pytest.fixture
async def agent_id(agent_repository: FakeAgentRepository) -> uuid.UUID:
    """Provide an agent id owned by the actor."""
    agent = await create_agent(agent_repository, ACTOR.account.id)
    return agent.id


async def test_create_insights(service: InsightService, agent_id: uuid.UUID) -> None:
    """Create a batch of insights in input order with the actor as owner."""
    insights = await service.create_insights(
        InsightCreate(
            agent_id=agent_id,
            insights=[
                InsightInput(
                    name="first", title="first", data=TextInsightData(content="a")
                ),
                InsightInput(
                    name="second",
                    title="second",
                    description="second description",
                    data=TextInsightData(content="b"),
                ),
            ],
        ),
        actor=ACTOR,
    )
    assert [insight.title for insight in insights] == ["first", "second"]
    for insight in insights:
        assert insight.owner_id == ACTOR.account.id
        assert insight.agent_id == agent_id
        assert insight.created is not None
    assert insights[0].description is None
    assert insights[1].description == "second description"


async def test_create_insights_missing_agent(service: InsightService) -> None:
    """Raise when the agent does not exist."""
    with pytest.raises(AgentNotFound):
        await service.create_insights(
            InsightCreate(
                agent_id=uuid.uuid4(),
                insights=[
                    InsightInput(
                        name="first", title="first", data=TextInsightData(content="a")
                    )
                ],
            ),
            actor=ACTOR,
        )


async def test_get_insight(service: InsightService, agent_id: uuid.UUID) -> None:
    """Load a stored insight by id."""
    created = await service.create_insights(
        InsightCreate(
            agent_id=agent_id,
            insights=[
                InsightInput(
                    name="first", title="first", data=TextInsightData(content="a")
                )
            ],
        ),
        actor=ACTOR,
    )
    loaded = await service.get_insight(created[0].id, actor=ACTOR)
    assert loaded == created[0]


async def test_get_insight_not_found(service: InsightService) -> None:
    """Raise for an unknown insight id."""
    missing_id = uuid.uuid4()
    with pytest.raises(InsightNotFound, match=f"Insight {missing_id} was not found"):
        await service.get_insight(missing_id, actor=ACTOR)


async def test_list_insights(service: InsightService, agent_id: uuid.UUID) -> None:
    """List insights and filter by data type."""
    await service.create_insights(
        InsightCreate(
            agent_id=agent_id,
            insights=[
                InsightInput(
                    name="text", title="text", data=TextInsightData(content="a")
                ),
                InsightInput(
                    name="categorical",
                    title="categorical",
                    data=CategoricalInsightData(
                        values=[CategoryValue(label="x", value=1)]
                    ),
                ),
            ],
        ),
        actor=ACTOR,
    )

    insights, next_cursor = await service.list_insights(InsightFilter(), actor=ACTOR)
    assert next_cursor is None
    assert [insight.title for insight in insights] == ["categorical", "text"]

    insights, _ = await service.list_insights(
        InsightFilter(
            expression=FilterCondition(field="type", op=FilterOp.EQ, value="text")
        ),
        actor=ACTOR,
    )
    assert [insight.title for insight in insights] == ["text"]


async def test_list_insights_filters_by_agent_id(
    service: InsightService, agent_repository: FakeAgentRepository, agent_id: uuid.UUID
) -> None:
    """Filter insights scoped to one agent."""
    matching = await service.create_insights(
        InsightCreate(
            agent_id=agent_id,
            insights=[
                InsightInput(
                    name="first", title="first", data=TextInsightData(content="a")
                )
            ],
        ),
        actor=ACTOR,
    )
    other_agent = await create_agent(agent_repository, ACTOR.account.id, name="other")
    await service.create_insights(
        InsightCreate(
            agent_id=other_agent.id,
            insights=[
                InsightInput(
                    name="other", title="other", data=TextInsightData(content="b")
                )
            ],
        ),
        actor=ACTOR,
    )

    insights, _ = await service.list_insights(
        InsightFilter(
            expression=FilterCondition(field="agent_id", op=FilterOp.EQ, value=agent_id)
        ),
        actor=ACTOR,
    )
    assert [insight.id for insight in insights] == [matching[0].id]


async def test_list_insights_filters_by_name(
    service: InsightService, agent_id: uuid.UUID
) -> None:
    """Filter insights by exact name."""
    matching = await service.create_insights(
        InsightCreate(
            agent_id=agent_id,
            insights=[
                InsightInput(
                    name="first", title="first", data=TextInsightData(content="a")
                )
            ],
        ),
        actor=ACTOR,
    )
    await service.create_insights(
        InsightCreate(
            agent_id=agent_id,
            insights=[
                InsightInput(
                    name="other", title="other", data=TextInsightData(content="b")
                )
            ],
        ),
        actor=ACTOR,
    )

    insights, _ = await service.list_insights(
        InsightFilter(
            expression=FilterCondition(field="name", op=FilterOp.EQ, value="first")
        ),
        actor=ACTOR,
    )
    assert [insight.id for insight in insights] == [matching[0].id]


async def test_update_insight_title(
    service: InsightService, agent_id: uuid.UUID
) -> None:
    """Update an insight's title."""
    created = await service.create_insights(
        InsightCreate(
            agent_id=agent_id,
            insights=[
                InsightInput(
                    name="first", title="first", data=TextInsightData(content="a")
                )
            ],
        ),
        actor=ACTOR,
    )
    updated = await service.update_insight(
        created[0].id, InsightUpdate(title="renamed"), actor=ACTOR
    )
    assert updated.title == "renamed"
    assert updated.updated is not None
    assert created[0].updated is not None
    assert updated.updated >= created[0].updated


async def test_update_insight_description(
    service: InsightService, agent_id: uuid.UUID
) -> None:
    """Update an insight's description without touching its title."""
    created = await service.create_insights(
        InsightCreate(
            agent_id=agent_id,
            insights=[
                InsightInput(
                    name="first",
                    title="first",
                    description="old",
                    data=TextInsightData(content="a"),
                )
            ],
        ),
        actor=ACTOR,
    )
    updated = await service.update_insight(
        created[0].id, InsightUpdate(description="new"), actor=ACTOR
    )
    assert updated.title == "first"
    assert updated.description == "new"


async def test_update_insight_description_cleared(
    service: InsightService, agent_id: uuid.UUID
) -> None:
    """Clear an insight's description with an explicit null."""
    created = await service.create_insights(
        InsightCreate(
            agent_id=agent_id,
            insights=[
                InsightInput(
                    name="first",
                    title="first",
                    description="old",
                    data=TextInsightData(content="a"),
                )
            ],
        ),
        actor=ACTOR,
    )
    updated = await service.update_insight(
        created[0].id, InsightUpdate(description=None), actor=ACTOR
    )
    assert updated.description is None


async def test_update_insight_omitted_fields_unchanged(
    service: InsightService, agent_id: uuid.UUID
) -> None:
    """Leave every field unchanged when the command sets none of it."""
    created = await service.create_insights(
        InsightCreate(
            agent_id=agent_id,
            insights=[
                InsightInput(
                    name="first",
                    title="first",
                    description="old",
                    data=TextInsightData(content="a"),
                )
            ],
        ),
        actor=ACTOR,
    )
    updated = await service.update_insight(created[0].id, InsightUpdate(), actor=ACTOR)
    assert updated.title == "first"
    assert updated.description == "old"


async def test_update_insight_cannot_clear_title(
    service: InsightService, agent_id: uuid.UUID
) -> None:
    """Reject clearing the insight title with an explicit null."""
    created = await service.create_insights(
        InsightCreate(
            agent_id=agent_id,
            insights=[
                InsightInput(
                    name="first", title="first", data=TextInsightData(content="a")
                )
            ],
        ),
        actor=ACTOR,
    )
    with pytest.raises(ValidationError, match="Insight title cannot be cleared"):
        await service.update_insight(
            created[0].id, InsightUpdate(title=None), actor=ACTOR
        )


async def test_create_insights_empty_title(
    service: InsightService, agent_id: uuid.UUID
) -> None:
    """Reject an insight with an empty title."""
    with pytest.raises(ValidationError, match="Insight title must not be empty"):
        await service.create_insights(
            InsightCreate(
                agent_id=agent_id,
                insights=[
                    InsightInput(
                        name="first", title="", data=TextInsightData(content="a")
                    )
                ],
            ),
            actor=ACTOR,
        )


async def test_update_insight_empty_title(
    service: InsightService, agent_id: uuid.UUID
) -> None:
    """Reject updating the insight title to an empty string."""
    created = await service.create_insights(
        InsightCreate(
            agent_id=agent_id,
            insights=[
                InsightInput(
                    name="first", title="first", data=TextInsightData(content="a")
                )
            ],
        ),
        actor=ACTOR,
    )
    with pytest.raises(ValidationError, match="Insight title must not be empty"):
        await service.update_insight(
            created[0].id, InsightUpdate(title=""), actor=ACTOR
        )


async def test_update_insight_not_found(service: InsightService) -> None:
    """Raise for an unknown insight id."""
    with pytest.raises(InsightNotFound):
        await service.update_insight(
            uuid.uuid4(), InsightUpdate(title="x"), actor=ACTOR
        )


async def test_delete_insight(service: InsightService, agent_id: uuid.UUID) -> None:
    """Delete a stored insight."""
    created = await service.create_insights(
        InsightCreate(
            agent_id=agent_id,
            insights=[
                InsightInput(
                    name="first", title="first", data=TextInsightData(content="a")
                )
            ],
        ),
        actor=ACTOR,
    )
    await service.delete_insight(created[0].id, actor=ACTOR)
    with pytest.raises(InsightNotFound):
        await service.get_insight(created[0].id, actor=ACTOR)


async def test_delete_insight_not_found(service: InsightService) -> None:
    """Raise for an unknown insight id."""
    with pytest.raises(InsightNotFound):
        await service.delete_insight(uuid.uuid4(), actor=ACTOR)


async def test_create_insights_tracks_insight_created(
    insight_repository: FakeInsightRepository,
    agent_repository: FakeAgentRepository,
    agent_id: uuid.UUID,
) -> None:
    """Fire INSIGHT_CREATED once per insight with its data type."""
    analytics = _RecordingAnalytics()
    service = InsightService(
        repository=insight_repository,
        agent_repository=agent_repository,
        analytics=analytics,
    )

    await service.create_insights(
        InsightCreate(
            agent_id=agent_id,
            insights=[
                InsightInput(
                    name="text", title="text", data=TextInsightData(content="a")
                ),
                InsightInput(
                    name="categorical",
                    title="categorical",
                    data=CategoricalInsightData(
                        values=[CategoryValue(label="x", value=1)]
                    ),
                ),
            ],
        ),
        actor=ACTOR,
    )

    assert len(analytics.tracked) == 2
    for user_id, event, _ in analytics.tracked:
        assert user_id == ACTOR.account.id
        assert event == AnalyticsEvent.INSIGHT_CREATED
    assert [properties for _, _, properties in analytics.tracked] == [
        {"insight_type": "text"},
        {"insight_type": "categorical"},
    ]


async def test_create_insights_without_analytics_tracker(
    service: InsightService, agent_id: uuid.UUID
) -> None:
    """Create insights normally when no analytics tracker is configured."""
    insights = await service.create_insights(
        InsightCreate(
            agent_id=agent_id,
            insights=[
                InsightInput(
                    name="first", title="first", data=TextInsightData(content="a")
                )
            ],
        ),
        actor=ACTOR,
    )
    assert insights[0].owner_id == ACTOR.account.id
