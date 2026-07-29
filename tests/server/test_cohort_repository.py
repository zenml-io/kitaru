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
from collections.abc import AsyncGenerator, Awaitable, Callable

import pytest

from conftest import (
    FakeCohortRepository,
    FakeSessionRepository,
    FakeTagRepository,
    create_session,
    pg_session,
    postgres_available,
)
from kitaru.api_models.v1.session import SessionOrigin
from kitaru.api_models.v1.tag import TagResourceType
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.agent_repository import SQLAgentRepository
from kitaru.server.adapters.db.repositories.cohort_repository import (
    SQLCohortRepository,
)
from kitaru.server.adapters.db.repositories.session_repository import (
    SQLSessionRepository,
)
from kitaru.server.adapters.db.repositories.tag_repository import SQLTagRepository
from kitaru.server.application.interfaces.cohort_repository import CohortRepository
from kitaru.server.application.interfaces.session_repository import SessionRepository
from kitaru.server.application.interfaces.tag_repository import TagRepository
from kitaru.server.application.models.cohort import CohortFilter, CohortSessionsFilter
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.cohort import Cohort, CohortNotFound, DuplicateCohortName
from kitaru.server.domain.session import Session, SessionInUse
from kitaru.server.domain.tag import Tag, TagLink

Setup = tuple[
    CohortRepository,
    SessionRepository,
    uuid.UUID,
    uuid.UUID,
    Callable[[], Awaitable[uuid.UUID]],
    TagRepository,
]


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide each cohort repository implementation, a session repository
    sharing its backend, an owner id, an agent id to attach cohorts and
    sessions to, a factory for further session ids on that agent, and a tag
    repository sharing the backend."""
    if request.param == "fake":
        tags = FakeTagRepository()
        sessions = FakeSessionRepository(tags=tags)
        owner_id = uuid.uuid4()
        agent_id = uuid.uuid4()

        async def make_session_id() -> uuid.UUID:
            created = await create_session(sessions, owner_id, agent_id=agent_id)
            return created.id

        yield (
            FakeCohortRepository(sessions, tags=tags),
            sessions,
            owner_id,
            agent_id,
            make_session_id,
            tags,
        )
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        agents = SQLAgentRepository(session)
        agent = await agents.create(Agent(owner_id=owner.id, name="assistant"))
        sessions_repository = SQLSessionRepository(session)

        async def make_session_id() -> uuid.UUID:
            created = await sessions_repository.create(
                Session(
                    owner_id=owner.id, agent_id=agent.id, origin=SessionOrigin.RECORDED
                )
            )
            return created.id

        yield (
            SQLCohortRepository(session),
            sessions_repository,
            owner.id,
            agent.id,
            make_session_id,
            SQLTagRepository(session),
        )


async def test_create_sets_session_count_and_timestamps(setup: Setup) -> None:
    """Store a new cohort with a denormalized session count."""
    repository, _, owner_id, agent_id, make_session_id, _ = setup
    session_ids = [await make_session_id(), await make_session_id()]
    cohort = await repository.create(
        Cohort(
            owner_id=owner_id,
            name="smoke-test",
            description="A cohort",
            agent_id=agent_id,
            session_count=len(session_ids),
        ),
        session_ids,
    )
    assert cohort.name == "smoke-test"
    assert cohort.description == "A cohort"
    assert cohort.agent_id == agent_id
    assert cohort.session_count == 2
    assert cohort.created is not None
    assert cohort.updated is not None


async def test_create_duplicate_name(setup: Setup) -> None:
    """Reject a second cohort with the same name."""
    repository, _, owner_id, agent_id, make_session_id, _ = setup
    session_id = await make_session_id()
    await repository.create(
        Cohort(
            owner_id=owner_id, name="smoke-test", agent_id=agent_id, session_count=1
        ),
        [session_id],
    )
    other_session_id = await make_session_id()
    with pytest.raises(
        DuplicateCohortName, match="Cohort name 'smoke-test' is already registered"
    ):
        await repository.create(
            Cohort(
                owner_id=owner_id,
                name="smoke-test",
                agent_id=agent_id,
                session_count=1,
            ),
            [other_session_id],
        )


async def test_get(setup: Setup) -> None:
    """Load a stored cohort by id."""
    repository, _, owner_id, agent_id, make_session_id, _ = setup
    session_id = await make_session_id()
    created = await repository.create(
        Cohort(owner_id=owner_id, name="cohort", agent_id=agent_id, session_count=1),
        [session_id],
    )
    loaded = await repository.get(created.id)
    assert loaded == created


async def test_get_not_found(setup: Setup) -> None:
    """Raise for an unknown cohort id."""
    repository, _, _, _, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(CohortNotFound, match=f"Cohort {missing_id} was not found"):
        await repository.get(missing_id)


async def test_query_filters_by_name(setup: Setup) -> None:
    """Filter cohorts by exact name."""
    repository, _, owner_id, agent_id, make_session_id, _ = setup
    await repository.create(
        Cohort(owner_id=owner_id, name="alpha", agent_id=agent_id, session_count=1),
        [await make_session_id()],
    )
    await repository.create(
        Cohort(owner_id=owner_id, name="beta", agent_id=agent_id, session_count=1),
        [await make_session_id()],
    )
    cohorts, next_cursor = await repository.query(CohortFilter(name="beta"))
    assert next_cursor is None
    assert [cohort.name for cohort in cohorts] == ["beta"]


async def test_query_filters_by_tag(setup: Setup) -> None:
    """Filter cohorts linked to a tag through tag_link."""
    repository, _, owner_id, agent_id, make_session_id, tags = setup
    tagged = await repository.create(
        Cohort(owner_id=owner_id, name="tagged", agent_id=agent_id, session_count=1),
        [await make_session_id()],
    )
    await repository.create(
        Cohort(owner_id=owner_id, name="untagged", agent_id=agent_id, session_count=1),
        [await make_session_id()],
    )
    tag = await tags.create(Tag(owner_id=owner_id, name="smoke-test"))
    await tags.create_link(
        TagLink(
            tag_id=tag.id,
            resource_type=TagResourceType.COHORT,
            resource_id=tagged.id,
        )
    )
    cohorts, _ = await repository.query(CohortFilter(tag="smoke-test"))
    assert [cohort.id for cohort in cohorts] == [tagged.id]


async def test_query_walks_pages(setup: Setup) -> None:
    """Walk every page via next_cursor without duplicates or gaps."""
    repository, _, owner_id, agent_id, make_session_id, _ = setup
    created = [
        await repository.create(
            Cohort(
                owner_id=owner_id,
                name=f"cohort-{index}",
                agent_id=agent_id,
                session_count=1,
            ),
            [await make_session_id()],
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
    repository, _, owner_id, agent_id, make_session_id, _ = setup
    created = await repository.create(
        Cohort(
            owner_id=owner_id,
            name="cohort",
            description="old",
            agent_id=agent_id,
            session_count=1,
        ),
        [await make_session_id()],
    )
    created.update_name("renamed")
    created.update_description("new")
    updated = await repository.update(created)
    assert updated.name == "renamed"
    assert updated.description == "new"
    assert updated.created == created.created
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated > created.updated
    loaded = await repository.get(created.id)
    assert loaded == updated


async def test_update_not_found(setup: Setup) -> None:
    """Raise for an unknown cohort id."""
    repository, _, owner_id, agent_id, _, _ = setup
    cohort = Cohort(
        owner_id=owner_id, name="cohort", agent_id=agent_id, session_count=0
    )
    with pytest.raises(CohortNotFound, match=f"Cohort {cohort.id} was not found"):
        await repository.update(cohort)


async def test_update_duplicate_name(setup: Setup) -> None:
    """Reject renaming a cohort to a registered name."""
    repository, _, owner_id, agent_id, make_session_id, _ = setup
    await repository.create(
        Cohort(owner_id=owner_id, name="alpha", agent_id=agent_id, session_count=1),
        [await make_session_id()],
    )
    other = await repository.create(
        Cohort(owner_id=owner_id, name="beta", agent_id=agent_id, session_count=1),
        [await make_session_id()],
    )
    other.update_name("alpha")
    with pytest.raises(DuplicateCohortName):
        await repository.update(other)


async def test_delete(setup: Setup) -> None:
    """Delete a stored cohort."""
    repository, _, owner_id, agent_id, make_session_id, _ = setup
    created = await repository.create(
        Cohort(owner_id=owner_id, name="cohort", agent_id=agent_id, session_count=1),
        [await make_session_id()],
    )
    await repository.delete(created.id)
    with pytest.raises(CohortNotFound):
        await repository.get(created.id)


async def test_delete_not_found(setup: Setup) -> None:
    """Raise for an unknown cohort id."""
    repository, _, _, _, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(CohortNotFound, match=f"Cohort {missing_id} was not found"):
        await repository.delete(missing_id)


async def test_delete_cascades_member_links(setup: Setup) -> None:
    """Free a member session for deletion once its cohort is gone."""
    repository, sessions, owner_id, agent_id, make_session_id, _ = setup
    session_id = await make_session_id()
    created = await repository.create(
        Cohort(owner_id=owner_id, name="cohort", agent_id=agent_id, session_count=1),
        [session_id],
    )
    await repository.delete(created.id)
    await sessions.delete(session_id)


async def test_session_in_cohort_delete_conflict(setup: Setup) -> None:
    """Reject deleting a session that belongs to a cohort."""
    repository, sessions, owner_id, agent_id, make_session_id, _ = setup
    session_id = await make_session_id()
    await repository.create(
        Cohort(owner_id=owner_id, name="cohort", agent_id=agent_id, session_count=1),
        [session_id],
    )
    with pytest.raises(SessionInUse, match=f"Session {session_id} belongs to a cohort"):
        await sessions.delete(session_id)


async def test_list_sessions_preserves_member_order(setup: Setup) -> None:
    """List a cohort's sessions in the order they were given at creation."""
    repository, _, owner_id, agent_id, make_session_id, _ = setup
    session_ids = [
        await make_session_id(),
        await make_session_id(),
        await make_session_id(),
    ]
    created = await repository.create(
        Cohort(
            owner_id=owner_id,
            name="cohort",
            agent_id=agent_id,
            session_count=len(session_ids),
        ),
        session_ids,
    )
    sessions, next_cursor = await repository.list_sessions(
        CohortSessionsFilter(cohort_id=created.id)
    )
    assert next_cursor is None
    assert [session.id for session in sessions] == session_ids


async def test_list_sessions_walks_pages_preserving_order(setup: Setup) -> None:
    """Walk every page of a cohort's sessions in fixed member order."""
    repository, _, owner_id, agent_id, make_session_id, _ = setup
    session_ids = [await make_session_id() for _ in range(5)]
    created = await repository.create(
        Cohort(
            owner_id=owner_id,
            name="cohort",
            agent_id=agent_id,
            session_count=len(session_ids),
        ),
        session_ids,
    )

    collected: list[uuid.UUID] = []
    cursor = None
    while True:
        sessions, next_cursor = await repository.list_sessions(
            CohortSessionsFilter(cohort_id=created.id, cursor=cursor, size=2)
        )
        collected.extend(session.id for session in sessions)
        if next_cursor is None:
            break
        cursor = next_cursor

    assert collected == session_ids
