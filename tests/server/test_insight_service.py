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

from kitaru.analytics.events import AnalyticsEvent
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.insight import (
    CategoricalArtifact,
    CategoryValue,
    InsightFinding,
    InsightInput,
    TextArtifact,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.insight import (
    InsightCreate,
    InsightFilter,
    InsightUpdate,
)
from kitaru.server.application.services.insight_service import InsightService
from kitaru.server.application.services.server_analytics import ServerAnalytics
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import AgentNotFound
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.insight import InsightNotFound
from kitaru.server.filtering import FilterCondition
from kitaru_test_support import FakeAgentRepository, FakeInsightRepository, create_agent

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))


def _text_input(
    name: str = "first",
    title: str = "first",
    *,
    summary: str | None = None,
    source_provider: str | None = None,
    content: str = "a",
) -> InsightInput:
    return InsightInput(
        name=name,
        title=title,
        summary=summary,
        source_provider=source_provider,
        findings=[
            InsightFinding(
                id="f1",
                title=title or "finding",
                artifacts=[TextArtifact(content=content)],
            )
        ],
    )


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
        """Record a track call instead of buffering it."""
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
                _text_input("first", "first", content="a"),
                _text_input(
                    "second",
                    "second",
                    summary="second summary",
                    source_provider="Langfuse",
                    content="b",
                ),
            ],
        ),
        actor=ACTOR,
    )
    assert [insight.title for insight in insights] == ["first", "second"]
    for insight in insights:
        assert insight.owner_id == ACTOR.account.id
        assert insight.agent_id == agent_id
        assert insight.status == "new"
        assert insight.created is not None
    assert insights[0].summary is None
    assert insights[1].summary == "second summary"
    assert insights[1].source_provider == "Langfuse"


async def test_create_insights_missing_agent(service: InsightService) -> None:
    """Raise when the agent does not exist."""
    with pytest.raises(AgentNotFound):
        await service.create_insights(
            InsightCreate(agent_id=uuid.uuid4(), insights=[_text_input()]),
            actor=ACTOR,
        )


async def test_get_insight(service: InsightService, agent_id: uuid.UUID) -> None:
    """Load a stored insight by id."""
    created = await service.create_insights(
        InsightCreate(agent_id=agent_id, insights=[_text_input()]),
        actor=ACTOR,
    )
    loaded = await service.get_insight(created[0].id, actor=ACTOR)
    assert loaded == created[0]


async def test_get_insight_not_found(service: InsightService) -> None:
    """Raise for an unknown insight id."""
    missing_id = uuid.uuid4()
    with pytest.raises(InsightNotFound, match=f"Insight {missing_id} was not found"):
        await service.get_insight(missing_id, actor=ACTOR)


async def test_list_insights_filters_by_status_and_source_provider(
    service: InsightService, agent_id: uuid.UUID
) -> None:
    """List insights and filter by status and source_provider."""
    created = await service.create_insights(
        InsightCreate(
            agent_id=agent_id,
            insights=[
                _text_input("first", "first", source_provider="Langfuse"),
                _text_input("second", "second", source_provider="LangSmith"),
            ],
        ),
        actor=ACTOR,
    )
    await service.update_insight(
        created[0].id, InsightUpdate(status="reviewed"), actor=ACTOR
    )

    insights, next_cursor = await service.list_insights(InsightFilter(), actor=ACTOR)
    assert next_cursor is None
    assert [insight.title for insight in insights] == ["second", "first"]

    reviewed, _ = await service.list_insights(
        InsightFilter(
            expression=FilterCondition(field="status", op=FilterOp.EQ, value="reviewed")
        ),
        actor=ACTOR,
    )
    assert [insight.title for insight in reviewed] == ["first"]

    langsmith, _ = await service.list_insights(
        InsightFilter(
            expression=FilterCondition(
                field="source_provider", op=FilterOp.EQ, value="LangSmith"
            )
        ),
        actor=ACTOR,
    )
    assert [insight.title for insight in langsmith] == ["second"]


async def test_list_insights_filters_by_agent_id(
    service: InsightService, agent_repository: FakeAgentRepository, agent_id: uuid.UUID
) -> None:
    """Filter insights scoped to one agent."""
    matching = await service.create_insights(
        InsightCreate(agent_id=agent_id, insights=[_text_input()]),
        actor=ACTOR,
    )
    other_agent = await create_agent(agent_repository, ACTOR.account.id, name="other")
    await service.create_insights(
        InsightCreate(
            agent_id=other_agent.id, insights=[_text_input("other", "other")]
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
        InsightCreate(agent_id=agent_id, insights=[_text_input("first", "first")]),
        actor=ACTOR,
    )
    await service.create_insights(
        InsightCreate(agent_id=agent_id, insights=[_text_input("other", "other")]),
        actor=ACTOR,
    )

    insights, _ = await service.list_insights(
        InsightFilter(
            expression=FilterCondition(field="name", op=FilterOp.EQ, value="first")
        ),
        actor=ACTOR,
    )
    assert [insight.id for insight in insights] == [matching[0].id]


async def test_update_insight_title_summary_and_status(
    service: InsightService, agent_id: uuid.UUID
) -> None:
    """Update an insight's title, summary, and status."""
    created = await service.create_insights(
        InsightCreate(agent_id=agent_id, insights=[_text_input(summary="old")]),
        actor=ACTOR,
    )
    updated = await service.update_insight(
        created[0].id,
        InsightUpdate(title="renamed", summary="new", status="reviewed"),
        actor=ACTOR,
    )
    assert updated.title == "renamed"
    assert updated.summary == "new"
    assert updated.status == "reviewed"
    assert updated.updated is not None
    assert created[0].updated is not None
    assert updated.updated >= created[0].updated


async def test_update_insight_summary_cleared(
    service: InsightService, agent_id: uuid.UUID
) -> None:
    """Clear an insight's summary with an explicit null."""
    created = await service.create_insights(
        InsightCreate(agent_id=agent_id, insights=[_text_input(summary="old")]),
        actor=ACTOR,
    )
    updated = await service.update_insight(
        created[0].id, InsightUpdate(summary=None), actor=ACTOR
    )
    assert updated.summary is None


async def test_update_insight_omitted_fields_unchanged(
    service: InsightService, agent_id: uuid.UUID
) -> None:
    """Leave every field unchanged when the command sets none of it."""
    created = await service.create_insights(
        InsightCreate(agent_id=agent_id, insights=[_text_input(summary="old")]),
        actor=ACTOR,
    )
    updated = await service.update_insight(created[0].id, InsightUpdate(), actor=ACTOR)
    assert updated.title == "first"
    assert updated.summary == "old"
    assert updated.status == "new"


async def test_update_insight_cannot_clear_title_or_status(
    service: InsightService, agent_id: uuid.UUID
) -> None:
    """Reject clearing title or status with an explicit null."""
    created = await service.create_insights(
        InsightCreate(agent_id=agent_id, insights=[_text_input()]),
        actor=ACTOR,
    )
    with pytest.raises(ValidationError, match="Insight title cannot be cleared"):
        await service.update_insight(
            created[0].id, InsightUpdate(title=None), actor=ACTOR
        )
    with pytest.raises(ValidationError, match="Insight status cannot be cleared"):
        await service.update_insight(
            created[0].id, InsightUpdate(status=None), actor=ACTOR
        )


async def test_update_insight_empty_title(
    service: InsightService, agent_id: uuid.UUID
) -> None:
    """Reject updating the insight title to an empty string."""
    created = await service.create_insights(
        InsightCreate(agent_id=agent_id, insights=[_text_input()]),
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
        InsightCreate(agent_id=agent_id, insights=[_text_input()]),
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
    """Fire INSIGHT_CREATED once per insight with finding count and artifact types."""
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
                _text_input("text", "text"),
                InsightInput(
                    name="categorical",
                    title="categorical",
                    findings=[
                        InsightFinding(
                            id="f1",
                            title="Mix",
                            artifacts=[
                                CategoricalArtifact(
                                    unit="sessions",
                                    values=[
                                        CategoryValue(label="x", value=2),
                                        CategoryValue(label="y", value=1),
                                    ],
                                )
                            ],
                        )
                    ],
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
        {"finding_count": 1, "artifact_types": ["text"]},
        {"finding_count": 1, "artifact_types": ["categorical"]},
    ]


async def test_create_insights_without_analytics_tracker(
    service: InsightService, agent_id: uuid.UUID
) -> None:
    """Create insights normally when no analytics tracker is configured."""
    insights = await service.create_insights(
        InsightCreate(agent_id=agent_id, insights=[_text_input()]),
        actor=ACTOR,
    )
    assert insights[0].owner_id == ACTOR.account.id
