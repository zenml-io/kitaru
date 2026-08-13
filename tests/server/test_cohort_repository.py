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
"""Contract tests for cohort repositories."""

import uuid
from collections.abc import AsyncGenerator

import pytest

from conftest import (
    FakeCohortRepository,
    FakeTagRepository,
    pg_session,
    postgres_available,
)
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.tag import TagResourceType
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.agent_repository import SQLAgentRepository
from kitaru.server.adapters.db.repositories.cohort_repository import (
    SQLCohortRepository,
)
from kitaru.server.adapters.db.repositories.tag_repository import SQLTagRepository
from kitaru.server.application.interfaces.cohort_repository import CohortRepository
from kitaru.server.application.interfaces.tag_repository import TagRepository
from kitaru.server.application.models.cohort import CohortFilter
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.cohort import Cohort, CohortNotFound, DuplicateCohortName
from kitaru.server.domain.tag import Tag, TagLink
from kitaru.server.filtering import FilterCondition

Setup = tuple[CohortRepository, uuid.UUID, uuid.UUID, TagRepository]


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide each cohort repository implementation, an owner id, an agent
    id to attach cohorts to, and a tag repository sharing the backend."""
    if request.param == "fake":
        tags = FakeTagRepository()
        yield FakeCohortRepository(tags=tags), uuid.uuid4(), uuid.uuid4(), tags
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        agents = SQLAgentRepository(session)
        agent = await agents.create(Agent(owner_id=owner.id, name="assistant"))
        yield (
            SQLCohortRepository(session),
            owner.id,
            agent.id,
            SQLTagRepository(session),
        )


async def test_create_sets_timestamps(setup: Setup) -> None:
    """Store a new cohort namespace with both timestamps set."""
    repository, owner_id, agent_id, _ = setup
    cohort = await repository.create(
        Cohort(
            owner_id=owner_id,
            name="smoke-test",
            description="A cohort",
            agent_id=agent_id,
            metadata={"team": "eval"},
        )
    )
    assert cohort.name == "smoke-test"
    assert cohort.description == "A cohort"
    assert cohort.agent_id == agent_id
    assert cohort.metadata == {"team": "eval"}
    assert cohort.latest_version == 0
    assert cohort.created is not None
    assert cohort.updated is not None


async def test_create_duplicate_name(setup: Setup) -> None:
    """Reject a second cohort with the same name."""
    repository, owner_id, agent_id, _ = setup
    await repository.create(
        Cohort(owner_id=owner_id, name="smoke-test", agent_id=agent_id)
    )
    with pytest.raises(
        DuplicateCohortName, match="Cohort name 'smoke-test' is already registered"
    ):
        await repository.create(
            Cohort(owner_id=owner_id, name="smoke-test", agent_id=agent_id)
        )


async def test_get(setup: Setup) -> None:
    """Load a stored cohort by id."""
    repository, owner_id, agent_id, _ = setup
    created = await repository.create(
        Cohort(owner_id=owner_id, name="cohort", agent_id=agent_id)
    )
    loaded = await repository.get(created.id)
    assert loaded == created


async def test_get_not_found(setup: Setup) -> None:
    """Raise for an unknown cohort id."""
    repository, _, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(CohortNotFound, match=f"Cohort {missing_id} was not found"):
        await repository.get(missing_id)


async def test_query_filters_by_name(setup: Setup) -> None:
    """Filter cohorts by exact name."""
    repository, owner_id, agent_id, _ = setup
    await repository.create(Cohort(owner_id=owner_id, name="alpha", agent_id=agent_id))
    await repository.create(Cohort(owner_id=owner_id, name="beta", agent_id=agent_id))
    cohorts, next_cursor = await repository.query(
        CohortFilter(
            expression=FilterCondition(field="name", op=FilterOp.EQ, value="beta")
        )
    )
    assert next_cursor is None
    assert [cohort.name for cohort in cohorts] == ["beta"]


async def test_query_filters_by_agent(setup: Setup) -> None:
    """Filter cohorts by agent id."""
    repository, owner_id, agent_id, _ = setup
    created = await repository.create(
        Cohort(owner_id=owner_id, name="alpha", agent_id=agent_id)
    )
    cohorts, next_cursor = await repository.query(
        CohortFilter(
            expression=FilterCondition(field="agent_id", op=FilterOp.EQ, value=agent_id)
        )
    )
    assert next_cursor is None
    assert [cohort.id for cohort in cohorts] == [created.id]

    cohorts, _ = await repository.query(
        CohortFilter(
            expression=FilterCondition(
                field="agent_id", op=FilterOp.EQ, value=uuid.uuid4()
            )
        )
    )
    assert cohorts == []


async def test_query_filters_by_tag(setup: Setup) -> None:
    """Filter cohorts linked to a tag through tag_link."""
    repository, owner_id, agent_id, tags = setup
    tagged = await repository.create(
        Cohort(owner_id=owner_id, name="tagged", agent_id=agent_id)
    )
    await repository.create(
        Cohort(owner_id=owner_id, name="untagged", agent_id=agent_id)
    )
    tag = await tags.create(Tag(owner_id=owner_id, name="smoke-test"))
    await tags.create_link(
        TagLink(
            tag_id=tag.id,
            resource_type=TagResourceType.COHORT,
            resource_id=tagged.id,
        )
    )
    cohorts, _ = await repository.query(
        CohortFilter(
            expression=FilterCondition(field="tag", op=FilterOp.EQ, value="smoke-test")
        )
    )
    assert [cohort.id for cohort in cohorts] == [tagged.id]


async def test_query_walks_pages(setup: Setup) -> None:
    """Walk every page via next_cursor without duplicates or gaps."""
    repository, owner_id, agent_id, _ = setup
    created = [
        await repository.create(
            Cohort(owner_id=owner_id, name=f"cohort-{index}", agent_id=agent_id)
        )
        for index in range(5)
    ]
    expected_order = list(reversed(created))

    collected: list[Cohort] = []
    cursor = None
    while True:
        cohorts, next_cursor = await repository.query(
            CohortFilter(cursor=cursor, size=2)
        )
        collected.extend(cohorts)
        if next_cursor is None:
            break
        cursor = next_cursor

    assert collected == expected_order


async def test_update(setup: Setup) -> None:
    """Persist field changes and renew the updated timestamp."""
    repository, owner_id, agent_id, _ = setup
    created = await repository.create(
        Cohort(owner_id=owner_id, name="cohort", description="old", agent_id=agent_id)
    )
    created.update_name("renamed")
    created.update_description("new")
    created.update_metadata({"team": "eval"})
    updated = await repository.update(created)
    assert updated.name == "renamed"
    assert updated.description == "new"
    assert updated.metadata == {"team": "eval"}
    assert updated.created == created.created
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated > created.updated
    loaded = await repository.get(created.id)
    assert loaded == updated


async def test_update_not_found(setup: Setup) -> None:
    """Raise for an unknown cohort id."""
    repository, owner_id, agent_id, _ = setup
    cohort = Cohort(owner_id=owner_id, name="cohort", agent_id=agent_id)
    with pytest.raises(CohortNotFound, match=f"Cohort {cohort.id} was not found"):
        await repository.update(cohort)


async def test_update_duplicate_name(setup: Setup) -> None:
    """Reject renaming a cohort to a registered name."""
    repository, owner_id, agent_id, _ = setup
    await repository.create(Cohort(owner_id=owner_id, name="alpha", agent_id=agent_id))
    other = await repository.create(
        Cohort(owner_id=owner_id, name="beta", agent_id=agent_id)
    )
    other.update_name("alpha")
    with pytest.raises(DuplicateCohortName):
        await repository.update(other)


async def test_delete(setup: Setup) -> None:
    """Delete a stored cohort."""
    repository, owner_id, agent_id, _ = setup
    created = await repository.create(
        Cohort(owner_id=owner_id, name="cohort", agent_id=agent_id)
    )
    await repository.delete(created.id)
    with pytest.raises(CohortNotFound):
        await repository.get(created.id)


async def test_delete_not_found(setup: Setup) -> None:
    """Raise for an unknown cohort id."""
    repository, _, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(CohortNotFound, match=f"Cohort {missing_id} was not found"):
        await repository.delete(missing_id)
