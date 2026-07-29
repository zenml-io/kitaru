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
"""Contract tests for session repositories."""

import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from conftest import (
    FakeSessionRepository,
    FakeTagRepository,
    pg_session,
    postgres_available,
)
from kitaru.api_models.v1.session import SessionOrigin, SessionStatus
from kitaru.api_models.v1.tag import TagResourceType
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.agent_repository import SQLAgentRepository
from kitaru.server.adapters.db.repositories.session_repository import (
    SQLSessionRepository,
)
from kitaru.server.adapters.db.repositories.tag_repository import SQLTagRepository
from kitaru.server.application.interfaces.session_repository import (
    SessionRepository,
)
from kitaru.server.application.interfaces.tag_repository import TagRepository
from kitaru.server.application.models.session import SessionFilter
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.session import (
    DuplicateSessionExternalId,
    Session,
    SessionNotFound,
    SessionRollups,
)
from kitaru.server.domain.tag import Tag, TagLink

Setup = tuple[SessionRepository, uuid.UUID, uuid.UUID, TagRepository]


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide each session repository implementation, an owner id, an agent
    id to attach sessions to, and a tag repository sharing its backend."""
    if request.param == "fake":
        tags = FakeTagRepository()
        yield FakeSessionRepository(tags=tags), uuid.uuid4(), uuid.uuid4(), tags
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        agents = SQLAgentRepository(session)
        agent = await agents.create(Agent(owner_id=owner.id, name="assistant"))
        yield (
            SQLSessionRepository(session),
            owner.id,
            agent.id,
            SQLTagRepository(session),
        )


async def test_create_sets_timestamps_and_defaults(setup: Setup) -> None:
    """Store a new session with both timestamps and default rollups."""
    repository, owner_id, agent_id, _ = setup
    session = await repository.create(
        Session(owner_id=owner_id, agent_id=agent_id, origin=SessionOrigin.RECORDED)
    )
    assert session.owner_id == owner_id
    assert session.agent_id == agent_id
    assert session.status == SessionStatus.IN_PROGRESS
    assert session.cost is None
    assert session.tokens is None
    assert session.llm_call_count == 0
    assert session.tool_call_count == 0
    assert session.created is not None
    assert session.updated is not None


async def test_create_duplicate_provider_external_id(setup: Setup) -> None:
    """Reject a second session with the same provider and external id."""
    repository, owner_id, agent_id, _ = setup
    await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            origin=SessionOrigin.IMPORTED,
            provider="langsmith",
            external_id="run-1",
        )
    )
    with pytest.raises(DuplicateSessionExternalId):
        await repository.create(
            Session(
                owner_id=owner_id,
                agent_id=agent_id,
                origin=SessionOrigin.IMPORTED,
                provider="langsmith",
                external_id="run-1",
            )
        )


async def test_create_allows_null_provider_and_external_id_repeatedly(
    setup: Setup,
) -> None:
    """Allow many sessions with no provider and external id."""
    repository, owner_id, agent_id, _ = setup
    first = await repository.create(
        Session(owner_id=owner_id, agent_id=agent_id, origin=SessionOrigin.RECORDED)
    )
    second = await repository.create(
        Session(owner_id=owner_id, agent_id=agent_id, origin=SessionOrigin.RECORDED)
    )
    assert first.id != second.id


async def test_get(setup: Setup) -> None:
    """Load a stored session by id."""
    repository, owner_id, agent_id, _ = setup
    created = await repository.create(
        Session(owner_id=owner_id, agent_id=agent_id, origin=SessionOrigin.RECORDED)
    )
    loaded = await repository.get(created.id)
    assert loaded == created


async def test_get_not_found(setup: Setup) -> None:
    """Raise for an unknown session id."""
    repository, _, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(SessionNotFound, match=f"Session {missing_id} was not found"):
        await repository.get(missing_id)


async def test_get_exclusive(setup: Setup) -> None:
    """Load a session with an exclusive lock without error."""
    repository, owner_id, agent_id, _ = setup
    created = await repository.create(
        Session(owner_id=owner_id, agent_id=agent_id, origin=SessionOrigin.RECORDED)
    )
    loaded = await repository.get(created.id, exclusive=True)
    assert loaded == created


async def test_query_filters_by_origin_and_status(setup: Setup) -> None:
    """Filter sessions by origin and status."""
    repository, owner_id, agent_id, _ = setup
    recorded = await repository.create(
        Session(owner_id=owner_id, agent_id=agent_id, origin=SessionOrigin.RECORDED)
    )
    await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            origin=SessionOrigin.IMPORTED,
            status=SessionStatus.COMPLETED,
        )
    )

    sessions, next_cursor = await repository.query(
        SessionFilter(origin=SessionOrigin.RECORDED)
    )
    assert next_cursor is None
    assert [s.id for s in sessions] == [recorded.id]

    sessions, next_cursor = await repository.query(
        SessionFilter(status=SessionStatus.COMPLETED)
    )
    assert next_cursor is None
    assert len(sessions) == 1
    assert sessions[0].status == SessionStatus.COMPLETED


async def test_query_filters_by_provider_and_external_id(setup: Setup) -> None:
    """Filter sessions by provider and external id together."""
    repository, owner_id, agent_id, _ = setup
    await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            origin=SessionOrigin.IMPORTED,
            provider="langsmith",
            external_id="run-1",
        )
    )
    target = await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            origin=SessionOrigin.IMPORTED,
            provider="langsmith",
            external_id="run-2",
        )
    )

    sessions, next_cursor = await repository.query(
        SessionFilter(provider="langsmith", external_id="run-2")
    )
    assert next_cursor is None
    assert [s.id for s in sessions] == [target.id]


async def test_query_filters_by_date_bounds(setup: Setup) -> None:
    """Filter sessions by started_after/before and ended_after/before."""
    repository, owner_id, agent_id, _ = setup
    early = await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            origin=SessionOrigin.RECORDED,
            started_at=datetime(2026, 1, 1, tzinfo=UTC),
            ended_at=datetime(2026, 1, 2, tzinfo=UTC),
        )
    )
    late = await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            origin=SessionOrigin.RECORDED,
            started_at=datetime(2026, 6, 1, tzinfo=UTC),
            ended_at=datetime(2026, 6, 2, tzinfo=UTC),
        )
    )

    sessions, _ = await repository.query(
        SessionFilter(started_after=datetime(2026, 3, 1, tzinfo=UTC))
    )
    assert [s.id for s in sessions] == [late.id]

    sessions, _ = await repository.query(
        SessionFilter(started_before=datetime(2026, 3, 1, tzinfo=UTC))
    )
    assert [s.id for s in sessions] == [early.id]

    sessions, _ = await repository.query(
        SessionFilter(ended_after=datetime(2026, 3, 1, tzinfo=UTC))
    )
    assert [s.id for s in sessions] == [late.id]

    sessions, _ = await repository.query(
        SessionFilter(ended_before=datetime(2026, 3, 1, tzinfo=UTC))
    )
    assert [s.id for s in sessions] == [early.id]


async def test_query_filters_by_cost_bounds(setup: Setup) -> None:
    """Filter sessions by min_cost and max_cost."""
    repository, owner_id, agent_id, _ = setup
    cheap = await repository.create(
        Session(owner_id=owner_id, agent_id=agent_id, origin=SessionOrigin.RECORDED)
    )
    pricey = await repository.create(
        Session(owner_id=owner_id, agent_id=agent_id, origin=SessionOrigin.RECORDED)
    )
    await repository.apply_rollups(cheap.id, SessionRollups(cost=Decimal("1.00")))
    await repository.apply_rollups(pricey.id, SessionRollups(cost=Decimal("9.00")))

    sessions, _ = await repository.query(SessionFilter(min_cost=Decimal("5.00")))
    assert [s.id for s in sessions] == [pricey.id]

    sessions, _ = await repository.query(SessionFilter(max_cost=Decimal("5.00")))
    assert [s.id for s in sessions] == [cheap.id]


async def test_query_has_evaluation_not_implemented(setup: Setup) -> None:
    """Raise when the has_evaluation filter is set."""
    repository, _, _, _ = setup
    with pytest.raises(ValidationError):
        await repository.query(SessionFilter(has_evaluation=True))


async def test_query_walks_pages(setup: Setup) -> None:
    """Walk every page via next_cursor without duplicates or gaps."""
    repository, owner_id, agent_id, _ = setup
    created = [
        await repository.create(
            Session(owner_id=owner_id, agent_id=agent_id, origin=SessionOrigin.RECORDED)
        )
        for _ in range(5)
    ]
    expected_order = list(reversed(created))

    collected: list[Session] = []
    cursor = None
    while True:
        sessions, next_cursor = await repository.query(
            SessionFilter(cursor=cursor, size=2)
        )
        collected.extend(sessions)
        if next_cursor is None:
            break
        cursor = next_cursor

    assert collected == expected_order
    assert len({s.id for s in collected}) == 5


async def test_update(setup: Setup) -> None:
    """Persist field changes and renew the updated timestamp."""
    repository, owner_id, agent_id, _ = setup
    created = await repository.create(
        Session(owner_id=owner_id, agent_id=agent_id, origin=SessionOrigin.RECORDED)
    )
    created.update_name("renamed")
    created.finish(
        status=SessionStatus.COMPLETED, outputs={"a": 1}, error=None, ended_at=None
    )
    updated = await repository.update(created)
    assert updated.name == "renamed"
    assert updated.status == SessionStatus.COMPLETED
    assert updated.outputs == {"a": 1}
    assert updated.created == created.created
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated > created.updated
    loaded = await repository.get(created.id)
    assert loaded == updated


async def test_update_not_found(setup: Setup) -> None:
    """Raise for an unknown session id."""
    repository, owner_id, agent_id, _ = setup
    session = Session(
        owner_id=owner_id, agent_id=agent_id, origin=SessionOrigin.RECORDED
    )
    with pytest.raises(SessionNotFound, match=f"Session {session.id} was not found"):
        await repository.update(session)


async def test_update_duplicate_external_id(setup: Setup) -> None:
    """Reject an update that collides with another session's provider and
    external id."""
    repository, owner_id, agent_id, _ = setup
    await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            origin=SessionOrigin.IMPORTED,
            provider="langsmith",
            external_id="run-1",
        )
    )
    other = await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            origin=SessionOrigin.IMPORTED,
            provider="langsmith",
            external_id="run-2",
        )
    )
    other.external_id = "run-1"
    with pytest.raises(DuplicateSessionExternalId):
        await repository.update(other)


async def test_delete(setup: Setup) -> None:
    """Delete a stored session."""
    repository, owner_id, agent_id, _ = setup
    created = await repository.create(
        Session(owner_id=owner_id, agent_id=agent_id, origin=SessionOrigin.RECORDED)
    )
    await repository.delete(created.id)
    with pytest.raises(SessionNotFound):
        await repository.get(created.id)


async def test_delete_not_found(setup: Setup) -> None:
    """Raise for an unknown session id."""
    repository, _, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(SessionNotFound, match=f"Session {missing_id} was not found"):
        await repository.delete(missing_id)


async def test_apply_rollups_accumulates_deltas(setup: Setup) -> None:
    """Add deltas atomically, coalescing null cost and tokens to zero."""
    repository, owner_id, agent_id, _ = setup
    created = await repository.create(
        Session(owner_id=owner_id, agent_id=agent_id, origin=SessionOrigin.RECORDED)
    )
    await repository.apply_rollups(
        created.id,
        SessionRollups(
            cost=Decimal("1.50"),
            input_tokens=10,
            output_tokens=5,
            llm_call_count=1,
        ),
    )
    await repository.apply_rollups(
        created.id,
        SessionRollups(cost=Decimal("0.50"), tool_call_count=1),
    )
    loaded = await repository.get(created.id)
    assert loaded.cost == Decimal("2.00")
    assert loaded.tokens is not None
    assert loaded.tokens.input_tokens == 10
    assert loaded.tokens.output_tokens == 5
    assert loaded.llm_call_count == 1
    assert loaded.tool_call_count == 1


async def test_apply_rollups_not_found(setup: Setup) -> None:
    """Raise for an unknown session id."""
    repository, _, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(SessionNotFound):
        await repository.apply_rollups(missing_id, SessionRollups())


async def test_query_filters_by_tag(setup: Setup) -> None:
    """Filter sessions linked to a tag through tag_link."""
    repository, owner_id, agent_id, tags = setup
    tagged = await repository.create(
        Session(owner_id=owner_id, agent_id=agent_id, origin=SessionOrigin.RECORDED)
    )
    await repository.create(
        Session(owner_id=owner_id, agent_id=agent_id, origin=SessionOrigin.RECORDED)
    )

    tag = await tags.create(Tag(owner_id=owner_id, name="smoke-test"))
    await tags.create_link(
        TagLink(
            tag_id=tag.id,
            resource_type=TagResourceType.SESSION,
            resource_id=tagged.id,
        )
    )

    sessions, _ = await repository.query(SessionFilter(tag="smoke-test"))
    assert [s.id for s in sessions] == [tagged.id]
