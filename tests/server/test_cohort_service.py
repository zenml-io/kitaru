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
"""Tests for cohort use cases."""

import uuid

import pytest

from conftest import (
    FakeAgentRepository,
    FakeCohortRepository,
    FakeSessionRepository,
    FakeTagRepository,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.cohorts import (
    CohortCreate,
    CohortFilter,
    CohortSessionsFilter,
    CohortUpdate,
)
from kitaru.server.application.services.cohort_service import CohortService
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent, AgentNotFound
from kitaru.server.domain.cohort import (
    CohortNotFound,
    DuplicateCohortName,
    InvalidCohort,
)
from kitaru.server.domain.session import (
    Session,
    SessionNotFound,
    SessionOrigin,
    SessionStatus,
)
from kitaru.server.domain.tag import Tag, TagLink, TagResourceType

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))


@pytest.fixture
def agent_repository() -> FakeAgentRepository:
    """Provide a fake agent repository."""
    return FakeAgentRepository()


@pytest.fixture
def tag_repository() -> FakeTagRepository:
    """Provide a fake tag repository."""
    return FakeTagRepository()


@pytest.fixture
def session_repository(
    agent_repository: FakeAgentRepository,
    tag_repository: FakeTagRepository,
) -> FakeSessionRepository:
    """Provide a fake session repository."""
    return FakeSessionRepository(agent_repository, None, tag_repository)


@pytest.fixture
def repository(
    session_repository: FakeSessionRepository,
    agent_repository: FakeAgentRepository,
    tag_repository: FakeTagRepository,
) -> FakeCohortRepository:
    """Provide a fake cohort repository."""
    return FakeCohortRepository(session_repository, agent_repository, tag_repository)


@pytest.fixture
def service(
    repository: FakeCohortRepository,
    session_repository: FakeSessionRepository,
    agent_repository: FakeAgentRepository,
) -> CohortService:
    """Provide a cohort service backed by the fake repositories."""
    return CohortService(
        repository=repository,
        session_repository=session_repository,
        agent_repository=agent_repository,
    )


@pytest.fixture
async def agent(agent_repository: FakeAgentRepository) -> Agent:
    """Provide a stored agent."""
    return await agent_repository.create(
        Agent(owner_id=ACTOR.account.id, name="support-bot")
    )


async def create_session(
    repository: FakeSessionRepository,
    agent_id: uuid.UUID,
    status: SessionStatus = SessionStatus.COMPLETED,
    name: str | None = None,
) -> Session:
    """Store a recorded session for cohort tests.

    Args:
        repository: Fake session repository.
        agent_id: Id of the agent.
        status: Session status.
        name: Session name.

    Returns:
        Stored session.
    """
    return await repository.create(
        Session(
            owner_id=ACTOR.account.id,
            agent_id=agent_id,
            origin=SessionOrigin.RECORDED,
            status=status,
            name=name,
        )
    )


async def test_create_cohort_from_session_ids(
    service: CohortService,
    session_repository: FakeSessionRepository,
    agent: Agent,
) -> None:
    """Create a cohort from explicit session ids."""
    first = await create_session(session_repository, agent.id)
    second = await create_session(session_repository, agent.id)
    cohort = await service.create_cohort(
        CohortCreate(
            name="baseline",
            description="July sessions",
            agent_id=agent.id,
            session_ids=[first.id, second.id],
        ),
        actor=ACTOR,
    )
    assert cohort.owner_id == ACTOR.account.id
    assert cohort.name == "baseline"
    assert cohort.description == "July sessions"
    assert cohort.agent_id == agent.id
    assert cohort.session_count == 2
    assert cohort.created is not None
    assert cohort.updated is not None


async def test_create_cohort_preserves_member_order(
    service: CohortService,
    session_repository: FakeSessionRepository,
    agent: Agent,
) -> None:
    """Keep the given session id order as the member positions."""
    first = await create_session(session_repository, agent.id, name="one")
    second = await create_session(session_repository, agent.id, name="two")
    cohort = await service.create_cohort(
        CohortCreate(
            name="baseline", agent_id=agent.id, session_ids=[second.id, first.id]
        ),
        actor=ACTOR,
    )
    sessions, total = await service.list_cohort_sessions(
        cohort.id, CohortSessionsFilter(), actor=ACTOR
    )
    assert total == 2
    assert [session.id for session in sessions] == [second.id, first.id]


async def test_create_cohort_duplicate_session_ids(
    service: CohortService,
    session_repository: FakeSessionRepository,
    agent: Agent,
) -> None:
    """Reject duplicate session ids."""
    session = await create_session(session_repository, agent.id)
    with pytest.raises(InvalidCohort, match="Session ids contain duplicates"):
        await service.create_cohort(
            CohortCreate(
                name="baseline",
                agent_id=agent.id,
                session_ids=[session.id, session.id],
            ),
            actor=ACTOR,
        )


async def test_create_cohort_unknown_agent(service: CohortService) -> None:
    """Raise for an unknown agent id."""
    missing_id = uuid.uuid4()
    with pytest.raises(AgentNotFound, match=f"Agent {missing_id} was not found"):
        await service.create_cohort(
            CohortCreate(
                name="baseline", agent_id=missing_id, session_ids=[uuid.uuid4()]
            ),
            actor=ACTOR,
        )


async def test_create_cohort_unknown_session(
    service: CohortService, agent: Agent
) -> None:
    """Raise for an unknown session id."""
    missing_id = uuid.uuid4()
    with pytest.raises(SessionNotFound, match=f"Session {missing_id} was not found"):
        await service.create_cohort(
            CohortCreate(name="baseline", agent_id=agent.id, session_ids=[missing_id]),
            actor=ACTOR,
        )


async def test_create_cohort_agent_mismatch(
    service: CohortService,
    session_repository: FakeSessionRepository,
    agent_repository: FakeAgentRepository,
    agent: Agent,
) -> None:
    """Reject a member session that belongs to another agent."""
    other = await agent_repository.create(
        Agent(owner_id=ACTOR.account.id, name="triage-bot")
    )
    session = await create_session(session_repository, other.id)
    with pytest.raises(
        InvalidCohort,
        match=f"Session {session.id} does not belong to agent {agent.id}",
    ):
        await service.create_cohort(
            CohortCreate(name="baseline", agent_id=agent.id, session_ids=[session.id]),
            actor=ACTOR,
        )


async def test_create_cohort_in_progress_member(
    service: CohortService,
    session_repository: FakeSessionRepository,
    agent: Agent,
) -> None:
    """Reject a member session that is in progress."""
    session = await create_session(
        session_repository, agent.id, status=SessionStatus.IN_PROGRESS
    )
    with pytest.raises(InvalidCohort, match=f"Session {session.id} is in progress"):
        await service.create_cohort(
            CohortCreate(name="baseline", agent_id=agent.id, session_ids=[session.id]),
            actor=ACTOR,
        )


async def test_create_cohort_empty_session_ids(
    service: CohortService, agent: Agent
) -> None:
    """Reject an empty session id list."""
    with pytest.raises(InvalidCohort, match="Cohort requires at least one session"):
        await service.create_cohort(
            CohortCreate(name="baseline", agent_id=agent.id, session_ids=[]),
            actor=ACTOR,
        )


async def test_create_cohort_duplicate_name(
    service: CohortService,
    session_repository: FakeSessionRepository,
    agent: Agent,
) -> None:
    """Reject a second cohort with the same name."""
    session = await create_session(session_repository, agent.id)
    command = CohortCreate(name="baseline", agent_id=agent.id, session_ids=[session.id])
    await service.create_cohort(command, actor=ACTOR)
    with pytest.raises(
        DuplicateCohortName, match="Cohort name 'baseline' is already registered"
    ):
        await service.create_cohort(command, actor=ACTOR)


async def test_get_cohort(
    service: CohortService,
    session_repository: FakeSessionRepository,
    agent: Agent,
) -> None:
    """Load a stored cohort by id."""
    session = await create_session(session_repository, agent.id)
    created = await service.create_cohort(
        CohortCreate(name="baseline", agent_id=agent.id, session_ids=[session.id]),
        actor=ACTOR,
    )
    loaded = await service.get_cohort(created.id, actor=ACTOR)
    assert loaded == created


async def test_get_cohort_not_found(service: CohortService) -> None:
    """Raise for an unknown cohort id."""
    missing_id = uuid.uuid4()
    with pytest.raises(CohortNotFound, match=f"Cohort {missing_id} was not found"):
        await service.get_cohort(missing_id, actor=ACTOR)


async def test_list_cohorts(
    service: CohortService,
    session_repository: FakeSessionRepository,
    agent: Agent,
) -> None:
    """List cohorts with filters and pagination."""
    session = await create_session(session_repository, agent.id)
    for name in ["one", "two", "three"]:
        await service.create_cohort(
            CohortCreate(name=name, agent_id=agent.id, session_ids=[session.id]),
            actor=ACTOR,
        )

    cohorts, total = await service.list_cohorts(CohortFilter(), actor=ACTOR)
    assert total == 3
    assert [cohort.name for cohort in cohorts] == ["one", "two", "three"]

    cohorts, total = await service.list_cohorts(
        CohortFilter(page=2, page_size=2), actor=ACTOR
    )
    assert total == 3
    assert [cohort.name for cohort in cohorts] == ["three"]

    cohorts, total = await service.list_cohorts(CohortFilter(name="two"), actor=ACTOR)
    assert total == 1


async def test_list_cohorts_by_tag(
    service: CohortService,
    session_repository: FakeSessionRepository,
    tag_repository: FakeTagRepository,
    agent: Agent,
) -> None:
    """List cohorts attached to a tag name."""
    session = await create_session(session_repository, agent.id)
    tagged = await service.create_cohort(
        CohortCreate(name="tagged", agent_id=agent.id, session_ids=[session.id]),
        actor=ACTOR,
    )
    await service.create_cohort(
        CohortCreate(name="other", agent_id=agent.id, session_ids=[session.id]),
        actor=ACTOR,
    )
    tag = await tag_repository.create(Tag(owner_id=ACTOR.account.id, name="prod"))
    await tag_repository.create_link(
        TagLink(
            tag_id=tag.id,
            resource_type=TagResourceType.COHORT,
            resource_id=tagged.id,
        )
    )

    cohorts, total = await service.list_cohorts(CohortFilter(tag="prod"), actor=ACTOR)
    assert total == 1
    assert cohorts[0].id == tagged.id

    cohorts, total = await service.list_cohorts(
        CohortFilter(tag="missing"), actor=ACTOR
    )
    assert total == 0


async def test_list_cohort_sessions_pagination(
    service: CohortService,
    session_repository: FakeSessionRepository,
    agent: Agent,
) -> None:
    """List member sessions with pagination in position order."""
    sessions = [await create_session(session_repository, agent.id) for _ in range(3)]
    cohort = await service.create_cohort(
        CohortCreate(
            name="baseline",
            agent_id=agent.id,
            session_ids=[session.id for session in sessions],
        ),
        actor=ACTOR,
    )
    members, total = await service.list_cohort_sessions(
        cohort.id, CohortSessionsFilter(page=2, page_size=2), actor=ACTOR
    )
    assert total == 3
    assert [member.id for member in members] == [sessions[2].id]


async def test_list_cohort_sessions_not_found(service: CohortService) -> None:
    """Raise for an unknown cohort id."""
    missing_id = uuid.uuid4()
    with pytest.raises(CohortNotFound, match=f"Cohort {missing_id} was not found"):
        await service.list_cohort_sessions(
            missing_id, CohortSessionsFilter(), actor=ACTOR
        )


async def test_update_cohort(
    service: CohortService,
    session_repository: FakeSessionRepository,
    agent: Agent,
) -> None:
    """Update name and description."""
    session = await create_session(session_repository, agent.id)
    created = await service.create_cohort(
        CohortCreate(name="baseline", agent_id=agent.id, session_ids=[session.id]),
        actor=ACTOR,
    )
    updated = await service.update_cohort(
        created.id,
        CohortUpdate(name="july", description="July sessions"),
        actor=ACTOR,
    )
    assert updated.name == "july"
    assert updated.description == "July sessions"
    assert updated.session_count == 1
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated > created.updated


async def test_update_cohort_absent_fields_unchanged(
    service: CohortService,
    session_repository: FakeSessionRepository,
    agent: Agent,
) -> None:
    """Keep every field on an update without set fields."""
    session = await create_session(session_repository, agent.id)
    created = await service.create_cohort(
        CohortCreate(
            name="baseline",
            description="July sessions",
            agent_id=agent.id,
            session_ids=[session.id],
        ),
        actor=ACTOR,
    )
    updated = await service.update_cohort(created.id, CohortUpdate(), actor=ACTOR)
    assert updated.name == "baseline"
    assert updated.description == "July sessions"


async def test_update_cohort_null_clears_description(
    service: CohortService,
    session_repository: FakeSessionRepository,
    agent: Agent,
) -> None:
    """Clear the description on an explicit null."""
    session = await create_session(session_repository, agent.id)
    created = await service.create_cohort(
        CohortCreate(
            name="baseline",
            description="July sessions",
            agent_id=agent.id,
            session_ids=[session.id],
        ),
        actor=ACTOR,
    )
    updated = await service.update_cohort(
        created.id, CohortUpdate(description=None), actor=ACTOR
    )
    assert updated.description is None


async def test_update_cohort_null_name_rejected(
    service: CohortService,
    session_repository: FakeSessionRepository,
    agent: Agent,
) -> None:
    """Reject an explicit null for the name."""
    session = await create_session(session_repository, agent.id)
    created = await service.create_cohort(
        CohortCreate(name="baseline", agent_id=agent.id, session_ids=[session.id]),
        actor=ACTOR,
    )
    with pytest.raises(InvalidCohort, match="Cohort name cannot be null"):
        await service.update_cohort(created.id, CohortUpdate(name=None), actor=ACTOR)


async def test_update_cohort_duplicate_name(
    service: CohortService,
    session_repository: FakeSessionRepository,
    agent: Agent,
) -> None:
    """Reject a new name that is already registered."""
    session = await create_session(session_repository, agent.id)
    await service.create_cohort(
        CohortCreate(name="baseline", agent_id=agent.id, session_ids=[session.id]),
        actor=ACTOR,
    )
    other = await service.create_cohort(
        CohortCreate(name="other", agent_id=agent.id, session_ids=[session.id]),
        actor=ACTOR,
    )
    with pytest.raises(
        DuplicateCohortName, match="Cohort name 'baseline' is already registered"
    ):
        await service.update_cohort(
            other.id, CohortUpdate(name="baseline"), actor=ACTOR
        )


async def test_update_cohort_not_found(service: CohortService) -> None:
    """Raise for an unknown cohort id."""
    with pytest.raises(CohortNotFound):
        await service.update_cohort(uuid.uuid4(), CohortUpdate(name="x"), actor=ACTOR)


async def test_delete_cohort(
    service: CohortService,
    session_repository: FakeSessionRepository,
    tag_repository: FakeTagRepository,
    agent: Agent,
) -> None:
    """Delete a cohort with its membership and tag links."""
    session = await create_session(session_repository, agent.id)
    created = await service.create_cohort(
        CohortCreate(name="baseline", agent_id=agent.id, session_ids=[session.id]),
        actor=ACTOR,
    )
    tag = await tag_repository.create(Tag(owner_id=ACTOR.account.id, name="prod"))
    await tag_repository.create_link(
        TagLink(
            tag_id=tag.id,
            resource_type=TagResourceType.COHORT,
            resource_id=created.id,
        )
    )

    await service.delete_cohort(created.id, actor=ACTOR)
    with pytest.raises(CohortNotFound):
        await service.get_cohort(created.id, actor=ACTOR)
    assert tag_repository.linked_resource_ids("prod", TagResourceType.COHORT) == set()
    # The membership is gone, so the session deletes without a conflict.
    await session_repository.delete(session.id)


async def test_delete_cohort_not_found(service: CohortService) -> None:
    """Raise for an unknown cohort id."""
    with pytest.raises(CohortNotFound):
        await service.delete_cohort(uuid.uuid4(), actor=ACTOR)
