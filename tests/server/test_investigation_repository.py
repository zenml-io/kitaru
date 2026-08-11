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
"""Contract tests for investigation repositories."""

import itertools
import uuid
from collections.abc import AsyncGenerator, Awaitable, Callable
from typing import Any

import pytest

from conftest import (
    FakeInvestigationRepository,
    FakeSessionRepository,
    create_session,
    pg_session_with_engine,
    postgres_available,
)
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.investigation import (
    InvestigationSessionVerdict,
    InvestigationStatus,
    QuestionItem,
)
from kitaru.api_models.v1.session import SessionOrigin
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.agent_repository import SQLAgentRepository
from kitaru.server.adapters.db.repositories.investigation_repository import (
    SQLInvestigationRepository,
)
from kitaru.server.adapters.db.repositories.session_repository import (
    SQLSessionRepository,
)
from kitaru.server.application.interfaces.investigation_repository import (
    InvestigationRepository,
)
from kitaru.server.application.models.investigation import (
    InvestigationFilter,
    InvestigationSessionFilter,
)
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.investigation import (
    Investigation,
    InvestigationNotFound,
    InvestigationSession,
    InvestigationSessionNotFound,
)
from kitaru.server.domain.session import Session
from kitaru.server.filtering import FilterCondition

Setup = tuple[
    InvestigationRepository,
    uuid.UUID,
    uuid.UUID,
    Callable[[], Awaitable[uuid.UUID]],
    Callable[[], Awaitable[uuid.UUID]],
]


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide each investigation repository implementation, an owner id, an
    agent id, a factory for further session ids on that agent, and a factory
    for further agent ids under the same owner."""
    if request.param == "fake":
        investigations = FakeInvestigationRepository()
        sessions = FakeSessionRepository()
        owner_id = uuid.uuid4()
        agent_id = uuid.uuid4()

        async def make_session_id() -> uuid.UUID:
            created = await create_session(sessions, owner_id, agent_id=agent_id)
            return created.id

        async def make_agent_id() -> uuid.UUID:
            return uuid.uuid4()

        yield investigations, owner_id, agent_id, make_session_id, make_agent_id
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session_with_engine() as (session, engine):
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        agents = SQLAgentRepository(session)
        agent = await agents.create(Agent(owner_id=owner.id, name="assistant"))
        sessions_repository = SQLSessionRepository(session, engine)
        session_numbers = itertools.count(1)

        async def make_session_id() -> uuid.UUID:
            created = await sessions_repository.create(
                Session(
                    owner_id=owner.id,
                    agent_id=agent.id,
                    number=next(session_numbers),
                    origin=SessionOrigin.RECORDED,
                )
            )
            return created.id

        async def make_agent_id() -> uuid.UUID:
            created = await agents.create(
                Agent(owner_id=owner.id, name=f"agent-{uuid.uuid4().hex[:8]}")
            )
            return created.id

        yield (
            SQLInvestigationRepository(session),
            owner.id,
            agent.id,
            make_session_id,
            make_agent_id,
        )


def _link(
    investigation_id: uuid.UUID, session_id: uuid.UUID, position: int, **overrides: Any
) -> InvestigationSession:
    """Build a linked session for a create() call.

    Args:
        investigation_id: Id of the owning investigation.
        session_id: Id of the linked session.
        position: Presentation order.
        **overrides: Additional investigation session fields.

    Returns:
        Investigation session ready to pass to create().
    """
    values: dict[str, Any] = {
        "investigation_id": investigation_id,
        "session_id": session_id,
        "position": position,
    }
    values.update(overrides)
    return InvestigationSession(**values)


async def _create_investigation(
    repository: InvestigationRepository,
    owner_id: uuid.UUID,
    agent_id: uuid.UUID,
    **overrides: Any,
) -> Investigation:
    """Store a bare investigation with no linked sessions.

    Args:
        repository: Investigation repository under test.
        owner_id: Id of the owning account.
        agent_id: Id of the agent the investigation's sessions belong to.
        **overrides: Additional investigation fields.

    Returns:
        Stored investigation.
    """
    values: dict[str, Any] = {
        "owner_id": owner_id,
        "agent_id": agent_id,
        "name": "investigation",
        "questions": [],
        "total_sessions": 0,
        "completed_sessions": 0,
    }
    values.update(overrides)
    return await repository.create(Investigation(**values), [])


async def test_create_sets_timestamps_and_counts(setup: Setup) -> None:
    """Persist an investigation and report zero progress with no links."""
    repository, owner_id, agent_id, _, _ = setup
    investigation = await _create_investigation(
        repository,
        owner_id,
        agent_id,
        questions=[QuestionItem(key="root_cause", question="What caused it?")],
    )
    assert investigation.owner_id == owner_id
    assert investigation.agent_id == agent_id
    assert investigation.status is InvestigationStatus.PENDING
    assert investigation.questions == [
        QuestionItem(key="root_cause", question="What caused it?")
    ]
    assert investigation.total_sessions == 0
    assert investigation.completed_sessions == 0
    assert investigation.created is not None
    assert investigation.updated is not None


async def test_create_with_linked_sessions_counts_total(setup: Setup) -> None:
    """Count every linked session as total_sessions regardless of verdict."""
    repository, owner_id, agent_id, make_session_id, _ = setup
    session_ids = [await make_session_id(), await make_session_id()]
    investigation = Investigation(
        owner_id=owner_id,
        agent_id=agent_id,
        name="investigation",
        questions=[],
        total_sessions=0,
        completed_sessions=0,
    )
    created = await repository.create(
        investigation,
        [
            _link(investigation.id, session_ids[0], 0),
            _link(investigation.id, session_ids[1], 1),
        ],
    )
    assert created.total_sessions == 2
    assert created.completed_sessions == 0


async def test_get(setup: Setup) -> None:
    """Load a stored investigation by id."""
    repository, owner_id, agent_id, _, _ = setup
    created = await _create_investigation(repository, owner_id, agent_id)
    loaded = await repository.get(created.id)
    assert loaded == created


async def test_get_not_found(setup: Setup) -> None:
    """Raise for an unknown investigation id."""
    repository, _, _, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(
        InvestigationNotFound, match=f"Investigation {missing_id} was not found"
    ):
        await repository.get(missing_id)


async def test_query_filters_by_agent_id(setup: Setup) -> None:
    """Filter investigations scoped to one agent."""
    repository, owner_id, agent_id, _, make_agent_id = setup
    matching = await _create_investigation(repository, owner_id, agent_id)
    await _create_investigation(repository, owner_id, await make_agent_id())
    investigations, _ = await repository.query(
        InvestigationFilter(
            expression=FilterCondition(field="agent_id", op=FilterOp.EQ, value=agent_id)
        )
    )
    assert [investigation.id for investigation in investigations] == [matching.id]


async def test_query_filters_by_status(setup: Setup) -> None:
    """Filter investigations scoped to one status."""
    repository, owner_id, agent_id, _, _ = setup
    await _create_investigation(repository, owner_id, agent_id)
    investigations, _ = await repository.query(
        InvestigationFilter(
            expression=FilterCondition(
                field="status", op=FilterOp.EQ, value=InvestigationStatus.COMPLETED
            )
        )
    )
    assert investigations == []


async def test_query_walks_pages(setup: Setup) -> None:
    """Walk every page via next_cursor without duplicates or gaps."""
    repository, owner_id, agent_id, _, _ = setup
    created = [
        await _create_investigation(repository, owner_id, agent_id) for _ in range(5)
    ]
    expected_order = list(reversed(created))

    collected: list[Investigation] = []
    cursor = None
    while True:
        investigations, next_cursor = await repository.query(
            InvestigationFilter(cursor=cursor, size=2)
        )
        collected.extend(investigations)
        if next_cursor is None:
            break
        cursor = next_cursor

    assert [investigation.id for investigation in collected] == [
        investigation.id for investigation in expected_order
    ]
    assert len({investigation.id for investigation in collected}) == 5


async def test_query_reports_counts(setup: Setup) -> None:
    """Report progress counts on every investigation returned by query()."""
    repository, owner_id, agent_id, make_session_id, _ = setup
    session_id = await make_session_id()
    investigation = Investigation(
        owner_id=owner_id,
        agent_id=agent_id,
        name="investigation",
        questions=[],
        total_sessions=0,
        completed_sessions=0,
    )
    await repository.create(investigation, [_link(investigation.id, session_id, 0)])
    investigations, _ = await repository.query(InvestigationFilter())
    assert investigations[0].total_sessions == 1
    assert investigations[0].completed_sessions == 0


async def test_update(setup: Setup) -> None:
    """Persist a rename and renew the updated timestamp."""
    repository, owner_id, agent_id, _, _ = setup
    created = await _create_investigation(repository, owner_id, agent_id)
    created.update_name("renamed")
    created.update_description("new rationale")
    updated = await repository.update(created)
    assert updated.name == "renamed"
    assert updated.description == "new rationale"
    assert updated.created == created.created
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated >= created.updated
    loaded = await repository.get(created.id)
    assert loaded == updated


async def test_update_not_found(setup: Setup) -> None:
    """Raise for an unknown investigation id."""
    repository, owner_id, agent_id, _, _ = setup
    investigation = Investigation(
        owner_id=owner_id,
        agent_id=agent_id,
        name="investigation",
        questions=[],
        total_sessions=0,
        completed_sessions=0,
    )
    with pytest.raises(
        InvestigationNotFound,
        match=f"Investigation {investigation.id} was not found",
    ):
        await repository.update(investigation)


async def test_update_preserves_counts(setup: Setup) -> None:
    """Recompute progress counts from link rows rather than trusting update()."""
    repository, owner_id, agent_id, make_session_id, _ = setup
    session_id = await make_session_id()
    investigation = Investigation(
        owner_id=owner_id,
        agent_id=agent_id,
        name="investigation",
        questions=[],
        total_sessions=0,
        completed_sessions=0,
    )
    created = await repository.create(
        investigation, [_link(investigation.id, session_id, 0)]
    )
    created.update_name("renamed")
    updated = await repository.update(created)
    assert updated.total_sessions == 1
    assert updated.completed_sessions == 0


async def test_delete(setup: Setup) -> None:
    """Delete a stored investigation."""
    repository, owner_id, agent_id, _, _ = setup
    created = await _create_investigation(repository, owner_id, agent_id)
    await repository.delete(created.id)
    with pytest.raises(InvestigationNotFound):
        await repository.get(created.id)


async def test_delete_not_found(setup: Setup) -> None:
    """Raise for an unknown investigation id."""
    repository, _, _, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(
        InvestigationNotFound, match=f"Investigation {missing_id} was not found"
    ):
        await repository.delete(missing_id)


async def test_delete_cascades_sessions(setup: Setup) -> None:
    """Delete an investigation's linked sessions along with it."""
    repository, owner_id, agent_id, make_session_id, _ = setup
    session_id = await make_session_id()
    investigation = Investigation(
        owner_id=owner_id,
        agent_id=agent_id,
        name="investigation",
        questions=[],
        total_sessions=0,
        completed_sessions=0,
    )
    created = await repository.create(
        investigation, [_link(investigation.id, session_id, 0)]
    )
    linked = await repository.get_session_by_session_id(created.id, session_id)
    await repository.delete(created.id)
    with pytest.raises(InvestigationSessionNotFound):
        await repository.get_session(linked.id)


async def test_progress_counts_track_links_with_verdicts(setup: Setup) -> None:
    """Count links with a verdict, apart from links without one."""
    repository, owner_id, agent_id, make_session_id, _ = setup
    session_ids = [
        await make_session_id(),
        await make_session_id(),
        await make_session_id(),
    ]
    investigation = Investigation(
        owner_id=owner_id,
        agent_id=agent_id,
        name="investigation",
        questions=[],
        total_sessions=0,
        completed_sessions=0,
    )
    created = await repository.create(
        investigation,
        [
            _link(investigation.id, session_ids[0], 0),
            _link(investigation.id, session_ids[1], 1),
            _link(investigation.id, session_ids[2], 2),
        ],
    )
    first = await repository.get_session_by_session_id(created.id, session_ids[0])
    first.update_verdict(InvestigationSessionVerdict.ACCEPTABLE)
    await repository.update_session(first)
    second = await repository.get_session_by_session_id(created.id, session_ids[1])
    second.update_verdict(InvestigationSessionVerdict.PROBLEMATIC)
    await repository.update_session(second)

    loaded = await repository.get(created.id)
    assert loaded.total_sessions == 3
    assert loaded.completed_sessions == 2


async def test_query_sessions_orders_by_position(setup: Setup) -> None:
    """Return an investigation's sessions ordered by position ascending."""
    repository, owner_id, agent_id, make_session_id, _ = setup
    session_ids = [
        await make_session_id(),
        await make_session_id(),
        await make_session_id(),
    ]
    investigation = Investigation(
        owner_id=owner_id,
        agent_id=agent_id,
        name="investigation",
        questions=[],
        total_sessions=0,
        completed_sessions=0,
    )
    created = await repository.create(
        investigation,
        [
            _link(investigation.id, session_ids[0], 2),
            _link(investigation.id, session_ids[1], 0),
            _link(investigation.id, session_ids[2], 1),
        ],
    )
    sessions, next_cursor = await repository.query_sessions(
        InvestigationSessionFilter(investigation_id=created.id)
    )
    assert next_cursor is None
    assert [session.session_id for session in sessions] == [
        session_ids[1],
        session_ids[2],
        session_ids[0],
    ]


async def test_query_sessions_scoped_to_investigation(setup: Setup) -> None:
    """Return only the sessions linked to the requested investigation."""
    repository, owner_id, agent_id, make_session_id, _ = setup
    session_id = await make_session_id()
    other_session_id = await make_session_id()
    investigation = Investigation(
        owner_id=owner_id,
        agent_id=agent_id,
        name="investigation",
        questions=[],
        total_sessions=0,
        completed_sessions=0,
    )
    other = Investigation(
        owner_id=owner_id,
        agent_id=agent_id,
        name="other",
        questions=[],
        total_sessions=0,
        completed_sessions=0,
    )
    created = await repository.create(
        investigation, [_link(investigation.id, session_id, 0)]
    )
    await repository.create(other, [_link(other.id, other_session_id, 0)])
    sessions, _ = await repository.query_sessions(
        InvestigationSessionFilter(investigation_id=created.id)
    )
    assert [session.session_id for session in sessions] == [session_id]


async def test_query_sessions_filters_by_verdict(setup: Setup) -> None:
    """Filter an investigation's sessions by verdict."""
    repository, owner_id, agent_id, make_session_id, _ = setup
    session_ids = [await make_session_id(), await make_session_id()]
    investigation = Investigation(
        owner_id=owner_id,
        agent_id=agent_id,
        name="investigation",
        questions=[],
        total_sessions=0,
        completed_sessions=0,
    )
    created = await repository.create(
        investigation,
        [
            _link(investigation.id, session_ids[0], 0),
            _link(investigation.id, session_ids[1], 1),
        ],
    )
    target = await repository.get_session_by_session_id(created.id, session_ids[0])
    target.update_verdict(InvestigationSessionVerdict.ACCEPTABLE)
    await repository.update_session(target)

    sessions, _ = await repository.query_sessions(
        InvestigationSessionFilter(
            investigation_id=created.id,
            expression=FilterCondition(
                field="verdict",
                op=FilterOp.EQ,
                value=InvestigationSessionVerdict.ACCEPTABLE,
            ),
        )
    )
    assert [session.session_id for session in sessions] == [session_ids[0]]

    sessions, _ = await repository.query_sessions(
        InvestigationSessionFilter(
            investigation_id=created.id,
            expression=FilterCondition(field="verdict", op=FilterOp.IS_NULL),
        )
    )
    assert [session.session_id for session in sessions] == [session_ids[1]]


async def test_query_sessions_walks_pages(setup: Setup) -> None:
    """Walk every page of an investigation's sessions without gaps."""
    repository, owner_id, agent_id, make_session_id, _ = setup
    session_ids = [await make_session_id() for _ in range(5)]
    investigation = Investigation(
        owner_id=owner_id,
        agent_id=agent_id,
        name="investigation",
        questions=[],
        total_sessions=0,
        completed_sessions=0,
    )
    created = await repository.create(
        investigation,
        [
            _link(investigation.id, session_id, position)
            for position, session_id in enumerate(session_ids)
        ],
    )

    collected: list[InvestigationSession] = []
    cursor = None
    while True:
        sessions, next_cursor = await repository.query_sessions(
            InvestigationSessionFilter(
                investigation_id=created.id, cursor=cursor, size=2
            )
        )
        collected.extend(sessions)
        if next_cursor is None:
            break
        cursor = next_cursor

    assert [session.session_id for session in collected] == session_ids
    assert len({session.id for session in collected}) == 5


async def test_get_session(setup: Setup) -> None:
    """Load a stored investigation session by id."""
    repository, owner_id, agent_id, make_session_id, _ = setup
    session_id = await make_session_id()
    investigation = Investigation(
        owner_id=owner_id,
        agent_id=agent_id,
        name="investigation",
        questions=[],
        total_sessions=0,
        completed_sessions=0,
    )
    created = await repository.create(
        investigation, [_link(investigation.id, session_id, 0)]
    )
    linked = await repository.get_session_by_session_id(created.id, session_id)
    loaded = await repository.get_session(linked.id)
    assert loaded == linked


async def test_get_session_not_found(setup: Setup) -> None:
    """Raise for an unknown investigation session id."""
    repository, _, _, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(
        InvestigationSessionNotFound,
        match=f"Investigation session {missing_id} was not found",
    ):
        await repository.get_session(missing_id)


async def test_get_session_by_session_id_not_found(setup: Setup) -> None:
    """Raise when no link matches the investigation and session pair."""
    repository, owner_id, agent_id, make_session_id, _ = setup
    session_id = await make_session_id()
    created = await _create_investigation(repository, owner_id, agent_id)
    with pytest.raises(InvestigationSessionNotFound):
        await repository.get_session_by_session_id(created.id, session_id)


async def test_update_session(setup: Setup) -> None:
    """Persist a verdict change and renew the updated timestamp."""
    repository, owner_id, agent_id, make_session_id, _ = setup
    session_id = await make_session_id()
    investigation = Investigation(
        owner_id=owner_id,
        agent_id=agent_id,
        name="investigation",
        questions=[],
        total_sessions=0,
        completed_sessions=0,
    )
    created = await repository.create(
        investigation, [_link(investigation.id, session_id, 0)]
    )
    linked = await repository.get_session_by_session_id(created.id, session_id)
    linked.update_verdict(InvestigationSessionVerdict.UNCERTAIN)
    updated = await repository.update_session(linked)
    assert updated.verdict is InvestigationSessionVerdict.UNCERTAIN
    assert updated.created == linked.created
    assert updated.updated is not None
    loaded = await repository.get_session(linked.id)
    assert loaded == updated


async def test_update_session_not_found(setup: Setup) -> None:
    """Raise for an unknown investigation session id."""
    repository, _, _, _, _ = setup
    session = InvestigationSession(
        investigation_id=uuid.uuid4(), session_id=uuid.uuid4(), position=0
    )
    with pytest.raises(
        InvestigationSessionNotFound,
        match=f"Investigation session {session.id} was not found",
    ):
        await repository.update_session(session)
