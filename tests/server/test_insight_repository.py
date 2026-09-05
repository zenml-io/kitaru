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
from typing import Any, NamedTuple

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from conftest import (
    FakeInsightRepository,
    FakePluginRepository,
    FakeTaskRepository,
    pg_session_with_engine,
    postgres_available,
)
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.insight import (
    CategoricalInsightData,
    CategoryValue,
    TextInsightData,
)
from kitaru.api_models.v1.job import JobKind
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.agent_repository import SQLAgentRepository
from kitaru.server.adapters.db.repositories.blob_repository import SQLBlobRepository
from kitaru.server.adapters.db.repositories.insight_repository import (
    SQLInsightRepository,
)
from kitaru.server.adapters.db.repositories.job_repository import SQLJobRepository
from kitaru.server.adapters.db.repositories.plugin_repository import (
    SQLPluginRepository,
)
from kitaru.server.adapters.db.repositories.task_repository import SQLTaskRepository
from kitaru.server.application.interfaces.insight_repository import InsightRepository
from kitaru.server.application.models.insight import InsightFilter
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.blob import Blob, BlobStorageBackend
from kitaru.server.domain.insight import Insight, InsightNotFound
from kitaru.server.domain.job import Job
from kitaru.server.domain.plugin import (
    Plugin,
    PluginKind,
    PluginVersionIdNotFound,
    ScriptPluginSource,
)
from kitaru.server.domain.task import AnalysisTask, TaskNotFound
from kitaru.server.filtering import FilterCondition


class Setup(NamedTuple):
    """Insight repository under test, plus rows an insight can reference."""

    insights: InsightRepository
    owner_id: uuid.UUID
    agent_id: uuid.UUID
    make_agent_id: Callable[[], Awaitable[uuid.UUID]]
    make_analyzer_version_id: Callable[[], Awaitable[uuid.UUID]]
    make_task_id: Callable[[uuid.UUID], Awaitable[uuid.UUID]]


async def _seed_postgres(session: AsyncSession) -> Setup:
    """Create the account and agent rows an insight references.

    Returns:
        Insight repository and the ids of the rows it can point insights at.
    """
    accounts = SQLAccountRepository(session)
    owner = await accounts.create(Account(name="owner"))
    agents = SQLAgentRepository(session)
    agent = await agents.create(Agent(owner_id=owner.id, name="assistant"))
    blobs = SQLBlobRepository(session)
    plugins = SQLPluginRepository(session)
    jobs = SQLJobRepository(session)
    tasks = SQLTaskRepository(session)

    async def make_agent_id() -> uuid.UUID:
        created = await agents.create(
            Agent(owner_id=owner.id, name=f"agent-{uuid.uuid4().hex[:8]}")
        )
        return created.id

    async def make_analyzer_version_id() -> uuid.UUID:
        code_blob, _ = await blobs.create(
            Blob(
                owner_id=owner.id,
                sha256=uuid.uuid4().hex.ljust(64, "0"),
                size=4,
                media_type="text/x-python",
                stored_in=BlobStorageBackend.DATABASE,
            )
        )
        plugin = await plugins.create(
            Plugin(
                owner_id=owner.id,
                kind=PluginKind.ANALYZER,
                name=f"analyzer-{uuid.uuid4().hex[:8]}",
            )
        )
        version = await plugins.create_version(
            plugin.id,
            ScriptPluginSource(blob_id=code_blob.id, entrypoint="analyze"),
            display_version=None,
        )
        return version.id

    async def make_task_id(analyzer_version_id: uuid.UUID) -> uuid.UUID:
        job = await jobs.create(Job(owner_id=owner.id, kind=JobKind.SESSION_RUN))
        task = await tasks.create(
            AnalysisTask(
                job_id=job.id,
                plugin_version_id=analyzer_version_id,
                agent_id=agent.id,
                input_session_ids=[uuid.uuid4()],
            )
        )
        return task.id

    return Setup(
        insights=SQLInsightRepository(session),
        owner_id=owner.id,
        agent_id=agent.id,
        make_agent_id=make_agent_id,
        make_analyzer_version_id=make_analyzer_version_id,
        make_task_id=make_task_id,
    )


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide each insight repository implementation and its collaborators."""
    if request.param == "fake":
        plugin_repository = FakePluginRepository()
        task_repository = FakeTaskRepository()
        insights = FakeInsightRepository(plugin_repository=plugin_repository)
        owner_id = uuid.uuid4()
        agent_id = uuid.uuid4()

        async def make_agent_id() -> uuid.UUID:
            return uuid.uuid4()

        async def make_analyzer_version_id() -> uuid.UUID:
            plugin = await plugin_repository.create(
                Plugin(owner_id=owner_id, kind=PluginKind.ANALYZER, name="trends")
            )
            version = await plugin_repository.create_version(
                plugin.id,
                ScriptPluginSource(blob_id=uuid.uuid4(), entrypoint="analyze"),
                display_version=None,
            )
            return version.id

        async def make_task_id(analyzer_version_id: uuid.UUID) -> uuid.UUID:
            task = await task_repository.create(
                AnalysisTask(
                    job_id=uuid.uuid4(),
                    plugin_version_id=analyzer_version_id,
                    agent_id=agent_id,
                    input_session_ids=[uuid.uuid4()],
                )
            )
            return task.id

        yield Setup(
            insights=insights,
            owner_id=owner_id,
            agent_id=agent_id,
            make_agent_id=make_agent_id,
            make_analyzer_version_id=make_analyzer_version_id,
            make_task_id=make_task_id,
        )
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session_with_engine() as (session, _):
        yield await _seed_postgres(session)


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
    insights = [
        _insight(setup.owner_id, setup.agent_id, title="first"),
        _insight(setup.owner_id, setup.agent_id, title="second"),
        _insight(setup.owner_id, setup.agent_id, title="third"),
    ]
    created = await setup.insights.create_many(insights)
    assert [insight.title for insight in created] == ["first", "second", "third"]
    for insight in created:
        assert insight.owner_id == setup.owner_id
        assert insight.agent_id == setup.agent_id
        assert insight.created is not None
        assert insight.updated is not None


async def test_create_many_names_a_missing_analyzer_version(setup: Setup) -> None:
    """Raise for an insight naming an analyzer version that does not exist."""
    with pytest.raises(PluginVersionIdNotFound):
        await setup.insights.create_many(
            [_insight(setup.owner_id, setup.agent_id, analyzer_version_id=uuid.uuid4())]
        )


async def test_create_many_names_a_missing_task() -> None:
    """Raise for an insight naming a task that does not exist.

    Postgres-only: the task foreign key is real, unlike the fake, which does
    not yet validate it.
    """
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session_with_engine() as (session, _):
        setup = await _seed_postgres(session)
        analyzer_version_id = await setup.make_analyzer_version_id()
        with pytest.raises(TaskNotFound):
            await setup.insights.create_many(
                [
                    _insight(
                        setup.owner_id,
                        setup.agent_id,
                        analyzer_version_id=analyzer_version_id,
                        task_id=uuid.uuid4(),
                    )
                ]
            )


async def test_create_and_get_carries_provenance(setup: Setup) -> None:
    """Round-trip an insight's provenance fields."""
    analyzer_version_id = await setup.make_analyzer_version_id()
    task_id = await setup.make_task_id(analyzer_version_id)
    created = await _create_insight(
        setup.insights,
        setup.owner_id,
        setup.agent_id,
        analyzer_version_id=analyzer_version_id,
        task_id=task_id,
        analyzer_params={"threshold": 0.5},
        params_hash="a" * 64,
    )
    assert created.analyzer_version_id == analyzer_version_id
    assert created.task_id == task_id
    assert created.analyzer_params == {"threshold": 0.5}
    assert created.params_hash == "a" * 64

    loaded = await setup.insights.get(created.id)
    assert loaded == created


async def test_get_not_found(setup: Setup) -> None:
    """Raise for an unknown insight id."""
    missing_id = uuid.uuid4()
    with pytest.raises(InsightNotFound, match=f"Insight {missing_id} was not found"):
        await setup.insights.get(missing_id)


async def test_query_filters_by_agent_id(setup: Setup) -> None:
    """Filter insights scoped to one agent."""
    matching = await _create_insight(setup.insights, setup.owner_id, setup.agent_id)
    await _create_insight(setup.insights, setup.owner_id, await setup.make_agent_id())
    insights, _ = await setup.insights.query(
        InsightFilter(
            expression=FilterCondition(
                field="agent_id", op=FilterOp.EQ, value=setup.agent_id
            )
        )
    )
    assert [insight.id for insight in insights] == [matching.id]


async def test_query_filters_by_name(setup: Setup) -> None:
    """Filter insights scoped to one exact name."""
    matching = await _create_insight(
        setup.insights, setup.owner_id, setup.agent_id, name="first"
    )
    await _create_insight(setup.insights, setup.owner_id, setup.agent_id, name="second")
    insights, _ = await setup.insights.query(
        InsightFilter(
            expression=FilterCondition(field="name", op=FilterOp.EQ, value="first")
        )
    )
    assert [insight.id for insight in insights] == [matching.id]


async def test_query_filters_by_type(setup: Setup) -> None:
    """Filter insights scoped to one data type."""
    text = await _create_insight(
        setup.insights,
        setup.owner_id,
        setup.agent_id,
        data=TextInsightData(content="root cause"),
    )
    await _create_insight(
        setup.insights,
        setup.owner_id,
        setup.agent_id,
        data=CategoricalInsightData(values=[CategoryValue(label="a", value=1)]),
    )
    insights, _ = await setup.insights.query(
        InsightFilter(
            expression=FilterCondition(field="type", op=FilterOp.EQ, value="text")
        )
    )
    assert [insight.id for insight in insights] == [text.id]


async def test_query_walks_pages(setup: Setup) -> None:
    """Walk every page via next_cursor without duplicates or gaps."""
    created = [
        await _create_insight(setup.insights, setup.owner_id, setup.agent_id)
        for _ in range(5)
    ]
    expected_order = list(reversed(created))

    collected: list[Insight] = []
    cursor = None
    while True:
        insights, next_cursor = await setup.insights.query(
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
    created = await _create_insight(setup.insights, setup.owner_id, setup.agent_id)
    created.update_title("renamed")
    created.update_description("new description")
    updated = await setup.insights.update(created)
    assert updated.title == "renamed"
    assert updated.description == "new description"
    assert updated.created == created.created
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated >= created.updated
    loaded = await setup.insights.get(created.id)
    assert loaded == updated


async def test_update_not_found(setup: Setup) -> None:
    """Raise for an unknown insight id."""
    insight = _insight(setup.owner_id, setup.agent_id)
    with pytest.raises(InsightNotFound, match=f"Insight {insight.id} was not found"):
        await setup.insights.update(insight)


async def test_delete(setup: Setup) -> None:
    """Delete a stored insight."""
    created = await _create_insight(setup.insights, setup.owner_id, setup.agent_id)
    await setup.insights.delete(created.id)
    with pytest.raises(InsightNotFound):
        await setup.insights.get(created.id)


async def test_delete_not_found(setup: Setup) -> None:
    """Raise for an unknown insight id."""
    missing_id = uuid.uuid4()
    with pytest.raises(InsightNotFound, match=f"Insight {missing_id} was not found"):
        await setup.insights.delete(missing_id)
