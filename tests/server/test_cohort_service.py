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
    create_agent,
    create_session,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.cohort import (
    CohortFilter,
    CohortSessionsFilter,
    CohortUpdate,
)
from kitaru.server.application.services.cohort_service import CohortService
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import AgentNotFound
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.cohort import CohortNotFound, DuplicateCohortName

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))


@pytest.fixture
def agent_repository() -> FakeAgentRepository:
    """Provide a fake agent repository."""
    return FakeAgentRepository()


@pytest.fixture
def session_repository() -> FakeSessionRepository:
    """Provide a fake session repository."""
    return FakeSessionRepository()


@pytest.fixture
def cohort_repository(
    session_repository: FakeSessionRepository,
) -> FakeCohortRepository:
    """Provide a fake cohort repository sharing the session backend."""
    return FakeCohortRepository(session_repository)


@pytest.fixture
def service(
    cohort_repository: FakeCohortRepository,
    agent_repository: FakeAgentRepository,
    session_repository: FakeSessionRepository,
) -> CohortService:
    """Provide a cohort service backed by fake repositories."""
    return CohortService(
        repository=cohort_repository,
        agent_repository=agent_repository,
        session_repository=session_repository,
    )


@pytest.fixture
async def agent_id(agent_repository: FakeAgentRepository) -> uuid.UUID:
    """Provide an agent id owned by the actor."""
    agent = await create_agent(agent_repository, ACTOR.account.id)
    return agent.id


async def _make_session_id(
    session_repository: FakeSessionRepository, agent_id: uuid.UUID
) -> uuid.UUID:
    """Store a session on the given agent and return its id."""
    session = await create_session(session_repository, ACTOR.account.id, agent_id)
    return session.id


async def test_create_cohort(
    service: CohortService,
    session_repository: FakeSessionRepository,
    agent_id: uuid.UUID,
) -> None:
    """Create a cohort as a fixed snapshot of its member sessions."""
    session_ids = [
        await _make_session_id(session_repository, agent_id),
        await _make_session_id(session_repository, agent_id),
    ]
    cohort = await service.create_cohort(
        name="smoke-test",
        description="A cohort",
        agent_id=agent_id,
        session_ids=session_ids,
        actor=ACTOR,
    )
    assert cohort.name == "smoke-test"
    assert cohort.description == "A cohort"
    assert cohort.agent_id == agent_id
    assert cohort.session_count == 2
    assert cohort.owner_id == ACTOR.account.id
    assert cohort.created is not None
    assert cohort.updated is not None


async def test_create_cohort_missing_agent(
    service: CohortService, session_repository: FakeSessionRepository
) -> None:
    """Raise when the agent does not exist."""
    missing_agent_id = uuid.uuid4()
    with pytest.raises(AgentNotFound):
        await service.create_cohort(
            name="cohort",
            description=None,
            agent_id=missing_agent_id,
            session_ids=[uuid.uuid4()],
            actor=ACTOR,
        )


async def test_create_cohort_empty_members(
    service: CohortService, agent_id: uuid.UUID
) -> None:
    """Reject an empty member list."""
    with pytest.raises(ValidationError, match="must have at least one session"):
        await service.create_cohort(
            name="cohort",
            description=None,
            agent_id=agent_id,
            session_ids=[],
            actor=ACTOR,
        )


async def test_create_cohort_duplicate_members(
    service: CohortService,
    session_repository: FakeSessionRepository,
    agent_id: uuid.UUID,
) -> None:
    """Reject a member list with a repeated session id."""
    session_id = await _make_session_id(session_repository, agent_id)
    with pytest.raises(ValidationError, match="contains duplicate sessions"):
        await service.create_cohort(
            name="cohort",
            description=None,
            agent_id=agent_id,
            session_ids=[session_id, session_id],
            actor=ACTOR,
        )


async def test_create_cohort_missing_session(
    service: CohortService, agent_id: uuid.UUID
) -> None:
    """Reject a member list naming a session that does not exist."""
    missing_session_id = uuid.uuid4()
    with pytest.raises(
        ValidationError, match=f"Session {missing_session_id} was not found"
    ):
        await service.create_cohort(
            name="cohort",
            description=None,
            agent_id=agent_id,
            session_ids=[missing_session_id],
            actor=ACTOR,
        )


async def test_create_cohort_session_wrong_agent(
    service: CohortService,
    session_repository: FakeSessionRepository,
    agent_repository: FakeAgentRepository,
    agent_id: uuid.UUID,
) -> None:
    """Reject a member session that belongs to a different agent."""
    other_agent = await create_agent(agent_repository, ACTOR.account.id, name="other")
    foreign_session_id = await _make_session_id(session_repository, other_agent.id)
    with pytest.raises(
        ValidationError, match=f"Session {foreign_session_id} does not belong"
    ):
        await service.create_cohort(
            name="cohort",
            description=None,
            agent_id=agent_id,
            session_ids=[foreign_session_id],
            actor=ACTOR,
        )


async def test_create_cohort_duplicate_name(
    service: CohortService,
    session_repository: FakeSessionRepository,
    agent_id: uuid.UUID,
) -> None:
    """Reject a second cohort with the same name."""
    await service.create_cohort(
        name="cohort",
        description=None,
        agent_id=agent_id,
        session_ids=[await _make_session_id(session_repository, agent_id)],
        actor=ACTOR,
    )
    with pytest.raises(DuplicateCohortName):
        await service.create_cohort(
            name="cohort",
            description=None,
            agent_id=agent_id,
            session_ids=[await _make_session_id(session_repository, agent_id)],
            actor=ACTOR,
        )


async def test_get_cohort(
    service: CohortService,
    session_repository: FakeSessionRepository,
    agent_id: uuid.UUID,
) -> None:
    """Load a stored cohort by id."""
    created = await service.create_cohort(
        name="cohort",
        description=None,
        agent_id=agent_id,
        session_ids=[await _make_session_id(session_repository, agent_id)],
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
    agent_id: uuid.UUID,
) -> None:
    """List cohorts newest-first with a name filter."""
    for name in ["alpha", "beta"]:
        await service.create_cohort(
            name=name,
            description=None,
            agent_id=agent_id,
            session_ids=[await _make_session_id(session_repository, agent_id)],
            actor=ACTOR,
        )

    cohorts, next_cursor = await service.list_cohorts(CohortFilter(), actor=ACTOR)
    assert next_cursor is None
    assert [cohort.name for cohort in cohorts] == ["beta", "alpha"]

    cohorts, next_cursor = await service.list_cohorts(
        CohortFilter(name="alpha"), actor=ACTOR
    )
    assert [cohort.name for cohort in cohorts] == ["alpha"]


async def test_update_cohort_name(
    service: CohortService,
    session_repository: FakeSessionRepository,
    agent_id: uuid.UUID,
) -> None:
    """Update a cohort's name."""
    created = await service.create_cohort(
        name="cohort",
        description=None,
        agent_id=agent_id,
        session_ids=[await _make_session_id(session_repository, agent_id)],
        actor=ACTOR,
    )
    updated = await service.update_cohort(
        created.id, CohortUpdate(name="renamed"), actor=ACTOR
    )
    assert updated.name == "renamed"
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated > created.updated


async def test_update_cohort_description(
    service: CohortService,
    session_repository: FakeSessionRepository,
    agent_id: uuid.UUID,
) -> None:
    """Update a cohort's description without touching its name."""
    created = await service.create_cohort(
        name="cohort",
        description="old",
        agent_id=agent_id,
        session_ids=[await _make_session_id(session_repository, agent_id)],
        actor=ACTOR,
    )
    updated = await service.update_cohort(
        created.id, CohortUpdate(description="new"), actor=ACTOR
    )
    assert updated.name == "cohort"
    assert updated.description == "new"


async def test_update_cohort_omitted_fields_unchanged(
    service: CohortService,
    session_repository: FakeSessionRepository,
    agent_id: uuid.UUID,
) -> None:
    """Leave every field unchanged when the command sets none of it."""
    created = await service.create_cohort(
        name="cohort",
        description="old",
        agent_id=agent_id,
        session_ids=[await _make_session_id(session_repository, agent_id)],
        actor=ACTOR,
    )
    updated = await service.update_cohort(created.id, CohortUpdate(), actor=ACTOR)
    assert updated.name == "cohort"
    assert updated.description == "old"


async def test_update_cohort_cannot_clear_name(
    service: CohortService,
    session_repository: FakeSessionRepository,
    agent_id: uuid.UUID,
) -> None:
    """Reject clearing the cohort name with an explicit null."""
    created = await service.create_cohort(
        name="cohort",
        description=None,
        agent_id=agent_id,
        session_ids=[await _make_session_id(session_repository, agent_id)],
        actor=ACTOR,
    )
    with pytest.raises(ValidationError, match="Cohort name cannot be cleared"):
        await service.update_cohort(created.id, CohortUpdate(name=None), actor=ACTOR)


async def test_update_cohort_not_found(service: CohortService) -> None:
    """Raise for an unknown cohort id."""
    with pytest.raises(CohortNotFound):
        await service.update_cohort(uuid.uuid4(), CohortUpdate(name="x"), actor=ACTOR)


async def test_update_cohort_duplicate_name(
    service: CohortService,
    session_repository: FakeSessionRepository,
    agent_id: uuid.UUID,
) -> None:
    """Reject renaming a cohort to a registered name."""
    await service.create_cohort(
        name="alpha",
        description=None,
        agent_id=agent_id,
        session_ids=[await _make_session_id(session_repository, agent_id)],
        actor=ACTOR,
    )
    other = await service.create_cohort(
        name="beta",
        description=None,
        agent_id=agent_id,
        session_ids=[await _make_session_id(session_repository, agent_id)],
        actor=ACTOR,
    )
    with pytest.raises(DuplicateCohortName):
        await service.update_cohort(other.id, CohortUpdate(name="alpha"), actor=ACTOR)


async def test_delete_cohort(
    service: CohortService,
    session_repository: FakeSessionRepository,
    agent_id: uuid.UUID,
) -> None:
    """Delete a stored cohort."""
    created = await service.create_cohort(
        name="cohort",
        description=None,
        agent_id=agent_id,
        session_ids=[await _make_session_id(session_repository, agent_id)],
        actor=ACTOR,
    )
    await service.delete_cohort(created.id, actor=ACTOR)
    with pytest.raises(CohortNotFound):
        await service.get_cohort(created.id, actor=ACTOR)


async def test_delete_cohort_not_found(service: CohortService) -> None:
    """Raise for an unknown cohort id."""
    with pytest.raises(CohortNotFound):
        await service.delete_cohort(uuid.uuid4(), actor=ACTOR)


async def test_list_cohort_sessions_preserves_order(
    service: CohortService,
    session_repository: FakeSessionRepository,
    agent_id: uuid.UUID,
) -> None:
    """List a cohort's sessions in fixed member order, across pages."""
    session_ids = [
        await _make_session_id(session_repository, agent_id) for _ in range(5)
    ]
    created = await service.create_cohort(
        name="cohort",
        description=None,
        agent_id=agent_id,
        session_ids=session_ids,
        actor=ACTOR,
    )

    collected: list[uuid.UUID] = []
    cursor = None
    while True:
        sessions, next_cursor = await service.list_cohort_sessions(
            CohortSessionsFilter(cohort_id=created.id, cursor=cursor, size=2),
            actor=ACTOR,
        )
        collected.extend(session.id for session in sessions)
        if next_cursor is None:
            break
        cursor = next_cursor

    assert collected == session_ids
