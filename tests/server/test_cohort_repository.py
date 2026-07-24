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
    FakeAgentRepository,
    FakeCohortRepository,
    FakeSessionRepository,
    FakeTagRepository,
    pg_session,
    postgres_available,
)
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.agent_repository import (
    SQLAgentRepository,
)
from kitaru.server.adapters.db.repositories.cohort_repository import (
    SQLCohortRepository,
)
from kitaru.server.adapters.db.repositories.session_repository import (
    SQLSessionRepository,
)
from kitaru.server.adapters.db.repositories.tag_repository import (
    SQLTagRepository,
)
from kitaru.server.application.interfaces.agent_repository import AgentRepository
from kitaru.server.application.interfaces.cohort_repository import (
    CohortRepository,
)
from kitaru.server.application.interfaces.session_repository import (
    SessionRepository,
)
from kitaru.server.application.interfaces.tag_repository import TagRepository
from kitaru.server.application.models.cohorts import (
    CohortFilter,
    CohortSessionsFilter,
)
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent, AgentNotFound
from kitaru.server.domain.cohort import (
    Cohort,
    CohortNotFound,
    DuplicateCohortName,
)
from kitaru.server.domain.session import (
    Session,
    SessionInUse,
    SessionOrigin,
    SessionStatus,
)
from kitaru.server.domain.tag import (
    Tag,
    TagLink,
    TagLinkNotFound,
    TagResourceType,
)

Setup = tuple[
    CohortRepository,
    SessionRepository,
    AgentRepository,
    TagRepository,
    uuid.UUID,
]


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide each cohort repository implementation plus an owner id."""
    if request.param == "fake":
        agents = FakeAgentRepository()
        tags = FakeTagRepository()
        sessions = FakeSessionRepository(agents, None, tags)
        cohorts = FakeCohortRepository(sessions, agents, tags)
        yield cohorts, sessions, agents, tags, uuid.uuid4()
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        # The owner_id column has a foreign key to the account table, so
        # store the owning account first.
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        yield (
            SQLCohortRepository(session),
            SQLSessionRepository(session),
            SQLAgentRepository(session),
            SQLTagRepository(session),
            owner.id,
        )


async def create_agent(
    repository: AgentRepository, owner_id: uuid.UUID, name: str = "support-bot"
) -> Agent:
    """Store an agent for cohort tests.

    Args:
        repository: Agent repository.
        owner_id: Id of the owning account.
        name: Agent name.

    Returns:
        Stored agent.
    """
    return await repository.create(Agent(owner_id=owner_id, name=name))


async def create_session(
    repository: SessionRepository, owner_id: uuid.UUID, agent_id: uuid.UUID
) -> Session:
    """Store a completed recorded session for cohort tests.

    Args:
        repository: Session repository.
        owner_id: Id of the owning account.
        agent_id: Id of the agent.

    Returns:
        Stored session.
    """
    return await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            origin=SessionOrigin.RECORDED,
            status=SessionStatus.COMPLETED,
        )
    )


def cohort_entity(
    owner_id: uuid.UUID, agent_id: uuid.UUID, **overrides: object
) -> Cohort:
    """Build a cohort entity.

    Args:
        owner_id: Id of the owning account.
        agent_id: Id of the agent.
        **overrides: Field overrides.

    Returns:
        Cohort entity.
    """
    values: dict[str, object] = {
        "owner_id": owner_id,
        "agent_id": agent_id,
        "name": "baseline",
        "session_count": 1,
        **overrides,
    }
    return Cohort.model_validate(values)


async def test_create_sets_timestamps(setup: Setup) -> None:
    """Store a new cohort with both timestamps set."""
    repository, sessions, agents, _, owner_id = setup
    agent = await create_agent(agents, owner_id)
    session = await create_session(sessions, owner_id, agent.id)
    cohort = await repository.create(cohort_entity(owner_id, agent.id), [session.id])
    assert cohort.owner_id == owner_id
    assert cohort.agent_id == agent.id
    assert cohort.created is not None
    assert cohort.updated is not None


async def test_create_round_trips_all_fields(setup: Setup) -> None:
    """Store a cohort and round-trip every field."""
    repository, sessions, agents, _, owner_id = setup
    agent = await create_agent(agents, owner_id)
    session = await create_session(sessions, owner_id, agent.id)
    cohort = cohort_entity(owner_id, agent.id, description="July sessions")
    created = await repository.create(cohort, [session.id])
    loaded = await repository.get(created.id)
    assert loaded == created
    assert loaded.description == "July sessions"
    assert loaded.session_count == 1


async def test_create_duplicate_name(setup: Setup) -> None:
    """Reject a second cohort with the same name."""
    repository, sessions, agents, _, owner_id = setup
    agent = await create_agent(agents, owner_id)
    session = await create_session(sessions, owner_id, agent.id)
    await repository.create(cohort_entity(owner_id, agent.id), [session.id])
    with pytest.raises(
        DuplicateCohortName, match="Cohort name 'baseline' is already registered"
    ):
        await repository.create(cohort_entity(owner_id, agent.id), [session.id])


async def test_create_after_duplicate_failure(setup: Setup) -> None:
    """Keep the repository usable after a duplicate name failure."""
    repository, sessions, agents, _, owner_id = setup
    agent = await create_agent(agents, owner_id)
    session = await create_session(sessions, owner_id, agent.id)
    await repository.create(cohort_entity(owner_id, agent.id), [session.id])
    with pytest.raises(DuplicateCohortName):
        await repository.create(cohort_entity(owner_id, agent.id), [session.id])
    cohort = await repository.create(
        cohort_entity(owner_id, agent.id, name="other"), [session.id]
    )
    assert cohort.name == "other"


async def test_create_unknown_agent(setup: Setup) -> None:
    """Raise for an unknown agent id."""
    repository, _, _, _, owner_id = setup
    missing_id = uuid.uuid4()
    with pytest.raises(AgentNotFound, match=f"Agent {missing_id} was not found"):
        await repository.create(cohort_entity(owner_id, missing_id), [])


async def test_get_not_found(setup: Setup) -> None:
    """Raise for an unknown cohort id."""
    repository, _, _, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(CohortNotFound, match=f"Cohort {missing_id} was not found"):
        await repository.get(missing_id)


async def test_query(setup: Setup) -> None:
    """Query cohorts by name with pagination."""
    repository, sessions, agents, _, owner_id = setup
    agent = await create_agent(agents, owner_id)
    session = await create_session(sessions, owner_id, agent.id)
    for name in ["one", "two", "three"]:
        await repository.create(
            cohort_entity(owner_id, agent.id, name=name), [session.id]
        )

    cohorts, total = await repository.query(CohortFilter())
    assert total == 3
    assert [cohort.name for cohort in cohorts] == ["one", "two", "three"]

    cohorts, total = await repository.query(CohortFilter(name="two"))
    assert total == 1

    cohorts, total = await repository.query(CohortFilter(page=2, page_size=2))
    assert total == 3
    assert [cohort.name for cohort in cohorts] == ["three"]


async def test_query_by_tag(setup: Setup) -> None:
    """Query cohorts attached to a tag name."""
    repository, sessions, agents, tags, owner_id = setup
    agent = await create_agent(agents, owner_id)
    session = await create_session(sessions, owner_id, agent.id)
    tagged = await repository.create(
        cohort_entity(owner_id, agent.id, name="tagged"), [session.id]
    )
    await repository.create(
        cohort_entity(owner_id, agent.id, name="other"), [session.id]
    )
    tag = await tags.create(Tag(owner_id=owner_id, name="prod"))
    await tags.create_link(
        TagLink(
            tag_id=tag.id,
            resource_type=TagResourceType.COHORT,
            resource_id=tagged.id,
        )
    )

    cohorts, total = await repository.query(CohortFilter(tag="prod"))
    assert total == 1
    assert cohorts[0].id == tagged.id

    cohorts, total = await repository.query(CohortFilter(tag="missing"))
    assert total == 0


async def test_query_sessions_ordered_by_position(setup: Setup) -> None:
    """Load member sessions in position order with pagination."""
    repository, sessions, agents, _, owner_id = setup
    agent = await create_agent(agents, owner_id)
    stored = [await create_session(sessions, owner_id, agent.id) for _ in range(3)]
    ordered = [stored[2], stored[0], stored[1]]
    cohort = await repository.create(
        cohort_entity(owner_id, agent.id, session_count=3),
        [session.id for session in ordered],
    )

    members, total = await repository.query_sessions(cohort.id, CohortSessionsFilter())
    assert total == 3
    assert [member.id for member in members] == [session.id for session in ordered]

    members, total = await repository.query_sessions(
        cohort.id, CohortSessionsFilter(page=2, page_size=2)
    )
    assert total == 3
    assert [member.id for member in members] == [ordered[2].id]


async def test_query_sessions_not_found(setup: Setup) -> None:
    """Raise for an unknown cohort id."""
    repository, _, _, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(CohortNotFound, match=f"Cohort {missing_id} was not found"):
        await repository.query_sessions(missing_id, CohortSessionsFilter())


async def test_update(setup: Setup) -> None:
    """Persist field changes and renew the updated timestamp."""
    repository, sessions, agents, _, owner_id = setup
    agent = await create_agent(agents, owner_id)
    session = await create_session(sessions, owner_id, agent.id)
    created = await repository.create(cohort_entity(owner_id, agent.id), [session.id])
    created.update_name("july")
    created.update_description("July sessions")
    updated = await repository.update(created)
    assert updated.name == "july"
    assert updated.description == "July sessions"
    assert updated.created == created.created
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated > created.updated
    loaded = await repository.get(created.id)
    assert loaded == updated


async def test_update_duplicate_name(setup: Setup) -> None:
    """Reject a new name that is already registered."""
    repository, sessions, agents, _, owner_id = setup
    agent = await create_agent(agents, owner_id)
    session = await create_session(sessions, owner_id, agent.id)
    await repository.create(cohort_entity(owner_id, agent.id), [session.id])
    other = await repository.create(
        cohort_entity(owner_id, agent.id, name="other"), [session.id]
    )
    other.update_name("baseline")
    with pytest.raises(
        DuplicateCohortName, match="Cohort name 'baseline' is already registered"
    ):
        await repository.update(other)


async def test_update_not_found(setup: Setup) -> None:
    """Raise for an unknown cohort id."""
    repository, _, agents, _, owner_id = setup
    agent = await create_agent(agents, owner_id)
    cohort = cohort_entity(owner_id, agent.id)
    with pytest.raises(CohortNotFound, match=f"Cohort {cohort.id} was not found"):
        await repository.update(cohort)


async def test_delete(setup: Setup) -> None:
    """Delete a stored cohort with its membership."""
    repository, sessions, agents, _, owner_id = setup
    agent = await create_agent(agents, owner_id)
    session = await create_session(sessions, owner_id, agent.id)
    created = await repository.create(cohort_entity(owner_id, agent.id), [session.id])
    await repository.delete(created.id)
    with pytest.raises(CohortNotFound):
        await repository.get(created.id)
    # The membership is gone, so the session deletes without a conflict.
    await sessions.delete(session.id)


async def test_delete_removes_tag_links(setup: Setup) -> None:
    """Remove the cohort's tag links on delete."""
    repository, sessions, agents, tags, owner_id = setup
    agent = await create_agent(agents, owner_id)
    session = await create_session(sessions, owner_id, agent.id)
    created = await repository.create(cohort_entity(owner_id, agent.id), [session.id])
    tag = await tags.create(Tag(owner_id=owner_id, name="prod"))
    await tags.create_link(
        TagLink(
            tag_id=tag.id,
            resource_type=TagResourceType.COHORT,
            resource_id=created.id,
        )
    )
    await repository.delete(created.id)
    with pytest.raises(TagLinkNotFound):
        await tags.delete_link(tag.id, TagResourceType.COHORT, created.id)


async def test_delete_not_found(setup: Setup) -> None:
    """Raise for an unknown cohort id."""
    repository, _, _, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(CohortNotFound, match=f"Cohort {missing_id} was not found"):
        await repository.delete(missing_id)


async def test_session_delete_blocked_while_in_cohort(setup: Setup) -> None:
    """Block deleting a session that is a member of a cohort."""
    repository, sessions, agents, _, owner_id = setup
    agent = await create_agent(agents, owner_id)
    session = await create_session(sessions, owner_id, agent.id)
    cohort = await repository.create(cohort_entity(owner_id, agent.id), [session.id])
    with pytest.raises(
        SessionInUse, match=f"Session {session.id} is referenced by cohorts"
    ):
        await sessions.delete(session.id)
    # The failed delete leaves both repositories usable.
    loaded = await sessions.get(session.id)
    assert loaded.id == session.id
    await repository.delete(cohort.id)
    await sessions.delete(session.id)
