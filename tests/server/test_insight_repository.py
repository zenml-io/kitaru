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
"""Contract tests for insight repositories."""

import uuid
from collections.abc import AsyncGenerator, Awaitable, Callable
from typing import Any

import pytest

from conftest import FakeInsightRepository, pg_session_with_engine, postgres_available
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.insight import (
    CategoricalInsightData,
    CategoryValue,
    TextInsightData,
)
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.agent_repository import SQLAgentRepository
from kitaru.server.adapters.db.repositories.insight_repository import (
    SQLInsightRepository,
)
from kitaru.server.application.interfaces.insight_repository import InsightRepository
from kitaru.server.application.models.insight import InsightFilter
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.insight import Insight, InsightNotFound
from kitaru.server.filtering import FilterCondition

Setup = tuple[
    InsightRepository, uuid.UUID, uuid.UUID, Callable[[], Awaitable[uuid.UUID]]
]


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide each insight repository implementation and its collaborators.

    Yields the repository, an owner id, an agent id, and a factory for
    further agent ids under the same owner.
    """
    if request.param == "fake":
        insights = FakeInsightRepository()
        owner_id = uuid.uuid4()
        agent_id = uuid.uuid4()

        async def make_agent_id() -> uuid.UUID:
            return uuid.uuid4()

        yield insights, owner_id, agent_id, make_agent_id
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session_with_engine() as (session, _):
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        agents = SQLAgentRepository(session)
        agent = await agents.create(Agent(owner_id=owner.id, name="assistant"))

        async def make_agent_id() -> uuid.UUID:
            created = await agents.create(
                Agent(owner_id=owner.id, name=f"agent-{uuid.uuid4().hex[:8]}")
            )
            return created.id

        yield SQLInsightRepository(session), owner.id, agent.id, make_agent_id


def _insight(owner_id: uuid.UUID, agent_id: uuid.UUID, **overrides: Any) -> Insight:
    """Build an insight for a create() call.

    Args:
        owner_id: Id of the owning account.
        agent_id: Id of the agent the insight belongs to.
        **overrides: Additional insight fields.

    Returns:
        Insight ready to pass to create().
    """
    values: dict[str, Any] = {
        "owner_id": owner_id,
        "agent_id": agent_id,
        "name": "insight",
        "title": "insight",
        "data": TextInsightData(content="root cause"),
    }
    values.update(overrides)
    return Insight(**values)


async def _create_insight(
    repository: InsightRepository,
    owner_id: uuid.UUID,
    agent_id: uuid.UUID,
    **overrides: Any,
) -> Insight:
    """Store a single insight via a one-item batch.

    Args:
        repository: Insight repository under test.
        owner_id: Id of the owning account.
        agent_id: Id of the agent the insight belongs to.
        **overrides: Additional insight fields.

    Returns:
        Stored insight.
    """
    created = await repository.create_many([_insight(owner_id, agent_id, **overrides)])
    return created[0]


async def test_create_sets_timestamps_and_order(setup: Setup) -> None:
    """Persist a batch of insights and return them in input order, timestamps set."""
    repository, owner_id, agent_id, _ = setup
    insights = [
        _insight(owner_id, agent_id, title="first"),
        _insight(owner_id, agent_id, title="second"),
        _insight(owner_id, agent_id, title="third"),
    ]
    created = await repository.create_many(insights)
    assert [insight.title for insight in created] == ["first", "second", "third"]
    for insight in created:
        assert insight.owner_id == owner_id
        assert insight.agent_id == agent_id
        assert insight.created is not None
        assert insight.updated is not None


async def test_get(setup: Setup) -> None:
    """Load a stored insight by id."""
    repository, owner_id, agent_id, _ = setup
    created = await _create_insight(repository, owner_id, agent_id)
    loaded = await repository.get(created.id)
    assert loaded == created


async def test_get_not_found(setup: Setup) -> None:
    """Raise for an unknown insight id."""
    repository, _, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(InsightNotFound, match=f"Insight {missing_id} was not found"):
        await repository.get(missing_id)


async def test_query_filters_by_agent_id(setup: Setup) -> None:
    """Filter insights scoped to one agent."""
    repository, owner_id, agent_id, make_agent_id = setup
    matching = await _create_insight(repository, owner_id, agent_id)
    await _create_insight(repository, owner_id, await make_agent_id())
    insights, _ = await repository.query(
        InsightFilter(
            expression=FilterCondition(field="agent_id", op=FilterOp.EQ, value=agent_id)
        )
    )
    assert [insight.id for insight in insights] == [matching.id]


async def test_query_filters_by_name(setup: Setup) -> None:
    """Filter insights scoped to one exact name."""
    repository, owner_id, agent_id, _ = setup
    matching = await _create_insight(repository, owner_id, agent_id, name="first")
    await _create_insight(repository, owner_id, agent_id, name="second")
    insights, _ = await repository.query(
        InsightFilter(
            expression=FilterCondition(field="name", op=FilterOp.EQ, value="first")
        )
    )
    assert [insight.id for insight in insights] == [matching.id]


async def test_query_filters_by_type(setup: Setup) -> None:
    """Filter insights scoped to one data type."""
    repository, owner_id, agent_id, _ = setup
    text = await _create_insight(
        repository, owner_id, agent_id, data=TextInsightData(content="root cause")
    )
    await _create_insight(
        repository,
        owner_id,
        agent_id,
        data=CategoricalInsightData(values=[CategoryValue(label="a", value=1)]),
    )
    insights, _ = await repository.query(
        InsightFilter(
            expression=FilterCondition(field="type", op=FilterOp.EQ, value="text")
        )
    )
    assert [insight.id for insight in insights] == [text.id]


async def test_query_walks_pages(setup: Setup) -> None:
    """Walk every page via next_cursor without duplicates or gaps."""
    repository, owner_id, agent_id, _ = setup
    created = [await _create_insight(repository, owner_id, agent_id) for _ in range(5)]
    expected_order = list(reversed(created))

    collected: list[Insight] = []
    cursor = None
    while True:
        insights, next_cursor = await repository.query(
            InsightFilter(cursor=cursor, size=2)
        )
        collected.extend(insights)
        if next_cursor is None:
            break
        cursor = next_cursor

    assert [insight.id for insight in collected] == [
        insight.id for insight in expected_order
    ]
    assert len({insight.id for insight in collected}) == 5


async def test_update(setup: Setup) -> None:
    """Persist a title and description change and renew the updated timestamp."""
    repository, owner_id, agent_id, _ = setup
    created = await _create_insight(repository, owner_id, agent_id)
    created.update_title("renamed")
    created.update_description("new description")
    updated = await repository.update(created)
    assert updated.title == "renamed"
    assert updated.description == "new description"
    assert updated.created == created.created
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated >= created.updated
    loaded = await repository.get(created.id)
    assert loaded == updated


async def test_update_not_found(setup: Setup) -> None:
    """Raise for an unknown insight id."""
    repository, owner_id, agent_id, _ = setup
    insight = _insight(owner_id, agent_id)
    with pytest.raises(InsightNotFound, match=f"Insight {insight.id} was not found"):
        await repository.update(insight)


async def test_delete(setup: Setup) -> None:
    """Delete a stored insight."""
    repository, owner_id, agent_id, _ = setup
    created = await _create_insight(repository, owner_id, agent_id)
    await repository.delete(created.id)
    with pytest.raises(InsightNotFound):
        await repository.get(created.id)


async def test_delete_not_found(setup: Setup) -> None:
    """Raise for an unknown insight id."""
    repository, _, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(InsightNotFound, match=f"Insight {missing_id} was not found"):
        await repository.delete(missing_id)
