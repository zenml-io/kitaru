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
    FakeAgentRepository,
    FakeAgentVersionRepository,
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
from kitaru.server.adapters.db.repositories.agent_version_repository import (
    SQLAgentVersionRepository,
)
from kitaru.server.adapters.db.repositories.session_repository import (
    SQLSessionRepository,
)
from kitaru.server.adapters.db.repositories.tag_repository import (
    SQLTagRepository,
)
from kitaru.server.application.interfaces.agent_repository import AgentRepository
from kitaru.server.application.interfaces.agent_version_repository import (
    AgentVersionRepository,
)
from kitaru.server.application.interfaces.session_repository import (
    SessionRepository,
)
from kitaru.server.application.interfaces.tag_repository import TagRepository
from kitaru.server.application.models.sessions import SessionFilter
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent, AgentNotFound
from kitaru.server.domain.agent_version import (
    AgentVersion,
    AgentVersionNotFound,
)
from kitaru.server.domain.session import (
    DuplicateSessionExternalId,
    Session,
    SessionNotFound,
    SessionOrigin,
    SessionProvider,
    SessionStatus,
    TokenUsage,
)
from kitaru.server.domain.tag import (
    Tag,
    TagLink,
    TagLinkNotFound,
    TagResourceType,
)

Setup = tuple[
    SessionRepository,
    AgentRepository,
    AgentVersionRepository,
    TagRepository,
    uuid.UUID,
]

STARTED_AT = datetime(2026, 7, 1, 12, 0, tzinfo=UTC)
ENDED_AT = datetime(2026, 7, 1, 12, 5, tzinfo=UTC)


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide each session repository implementation plus an owner id."""
    if request.param == "fake":
        agents = FakeAgentRepository()
        versions = FakeAgentVersionRepository(agents)
        tags = FakeTagRepository()
        sessions = FakeSessionRepository(agents, versions, tags)
        yield sessions, agents, versions, tags, uuid.uuid4()
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        # The owner_id column has a foreign key to the account table, so
        # store the owning account first.
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        yield (
            SQLSessionRepository(session),
            SQLAgentRepository(session),
            SQLAgentVersionRepository(session),
            SQLTagRepository(session),
            owner.id,
        )


async def create_agent(
    repository: AgentRepository, owner_id: uuid.UUID, name: str = "support-bot"
) -> Agent:
    """Store an agent for session tests.

    Args:
        repository: Agent repository.
        owner_id: Id of the owning account.
        name: Agent name.

    Returns:
        Stored agent.
    """
    return await repository.create(Agent(owner_id=owner_id, name=name))


def recorded_session(
    owner_id: uuid.UUID, agent_id: uuid.UUID, **overrides: object
) -> Session:
    """Build a recorded session entity.

    Args:
        owner_id: Id of the owning account.
        agent_id: Id of the agent.
        **overrides: Field overrides.

    Returns:
        Session entity.
    """
    values: dict[str, object] = {
        "owner_id": owner_id,
        "agent_id": agent_id,
        "origin": SessionOrigin.RECORDED,
        **overrides,
    }
    return Session.model_validate(values)


def imported_session(
    owner_id: uuid.UUID,
    agent_id: uuid.UUID,
    external_id: str = "lf-1",
    provider: SessionProvider = SessionProvider.LANGFUSE,
    **overrides: object,
) -> Session:
    """Build an imported session entity.

    Args:
        owner_id: Id of the owning account.
        agent_id: Id of the agent.
        external_id: External session id.
        provider: Session provider.
        **overrides: Field overrides.

    Returns:
        Session entity.
    """
    values: dict[str, object] = {
        "owner_id": owner_id,
        "agent_id": agent_id,
        "origin": SessionOrigin.IMPORTED,
        "status": SessionStatus.COMPLETED,
        "provider": provider,
        "external_id": external_id,
        **overrides,
    }
    return Session.model_validate(values)


async def test_create_sets_timestamps(setup: Setup) -> None:
    """Store a new session with both timestamps set."""
    repository, agents, _, _, owner_id = setup
    agent = await create_agent(agents, owner_id)
    session = await repository.create(recorded_session(owner_id, agent.id))
    assert session.owner_id == owner_id
    assert session.agent_id == agent.id
    assert session.status is SessionStatus.IN_PROGRESS
    assert session.created is not None
    assert session.updated is not None


async def test_create_round_trips_all_fields(setup: Setup) -> None:
    """Store a session and round-trip every field."""
    repository, agents, versions, _, owner_id = setup
    agent = await create_agent(agents, owner_id)
    version = await versions.create(
        AgentVersion(owner_id=owner_id, agent_id=agent.id, version="v1")
    )
    session = recorded_session(
        owner_id,
        agent.id,
        agent_version_id=version.id,
        name="run-1",
        inputs={"prompt": "hi"},
        expected={"answer": "42"},
        started_at=STARTED_AT,
        external_id="conv-1",
        metadata={"env": "prod"},
        framework="pydantic_ai",
        adapter_version="0.1.0",
        cost=Decimal("1.25"),
        tokens=TokenUsage(input_tokens=100, output_tokens=20),
        llm_call_count=2,
        tool_call_count=1,
        scores={"conciseness": 0.5},
    )
    created = await repository.create(session)
    loaded = await repository.get(created.id)
    assert loaded == created
    assert loaded.agent_version_id == version.id
    assert loaded.inputs == {"prompt": "hi"}
    assert loaded.expected == {"answer": "42"}
    assert loaded.started_at == STARTED_AT
    assert loaded.metadata == {"env": "prod"}
    assert loaded.cost == Decimal("1.25")
    assert loaded.tokens == TokenUsage(input_tokens=100, output_tokens=20)
    assert loaded.scores == {"conciseness": 0.5}


async def test_create_duplicate_import(setup: Setup) -> None:
    """Reject a second session with the same provider and external id."""
    repository, agents, _, _, owner_id = setup
    agent = await create_agent(agents, owner_id)
    await repository.create(imported_session(owner_id, agent.id))
    with pytest.raises(
        DuplicateSessionExternalId,
        match="Session external id 'lf-1' is already registered for provider "
        "'langfuse'",
    ):
        await repository.create(imported_session(owner_id, agent.id))


async def test_create_after_duplicate_failure(setup: Setup) -> None:
    """Keep the repository usable after a duplicate import failure."""
    repository, agents, _, _, owner_id = setup
    agent = await create_agent(agents, owner_id)
    await repository.create(imported_session(owner_id, agent.id))
    with pytest.raises(DuplicateSessionExternalId):
        await repository.create(imported_session(owner_id, agent.id))
    session = await repository.create(
        imported_session(owner_id, agent.id, external_id="lf-2")
    )
    assert session.external_id == "lf-2"


async def test_create_same_external_id_other_provider(setup: Setup) -> None:
    """Register the same external id for two providers."""
    repository, agents, _, _, owner_id = setup
    agent = await create_agent(agents, owner_id)
    await repository.create(imported_session(owner_id, agent.id))
    session = await repository.create(
        imported_session(owner_id, agent.id, provider=SessionProvider.BRAINTRUST)
    )
    assert session.provider is SessionProvider.BRAINTRUST


async def test_create_unknown_agent(setup: Setup) -> None:
    """Raise for an unknown agent id."""
    repository, _, _, _, owner_id = setup
    missing_id = uuid.uuid4()
    with pytest.raises(AgentNotFound, match=f"Agent {missing_id} was not found"):
        await repository.create(recorded_session(owner_id, missing_id))


async def test_create_unknown_agent_version(setup: Setup) -> None:
    """Raise for an unknown agent version id."""
    repository, agents, _, _, owner_id = setup
    agent = await create_agent(agents, owner_id)
    missing_id = uuid.uuid4()
    with pytest.raises(
        AgentVersionNotFound, match=f"Agent version {missing_id} was not found"
    ):
        await repository.create(
            recorded_session(owner_id, agent.id, agent_version_id=missing_id)
        )


async def test_get_not_found(setup: Setup) -> None:
    """Raise for an unknown session id."""
    repository, _, _, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(SessionNotFound, match=f"Session {missing_id} was not found"):
        await repository.get(missing_id)


async def test_query_scalar_filters(setup: Setup) -> None:
    """Query sessions by agent, version, origin, status, and identity."""
    repository, agents, versions, _, owner_id = setup
    agent = await create_agent(agents, owner_id)
    other = await create_agent(agents, owner_id, name="triage-bot")
    version = await versions.create(
        AgentVersion(owner_id=owner_id, agent_id=agent.id, version="v1")
    )
    recorded = await repository.create(
        recorded_session(owner_id, agent.id, agent_version_id=version.id, name="run-1")
    )
    finished = recorded_session(owner_id, other.id, name="run-2")
    finished.status = SessionStatus.FAILED
    await repository.create(finished)
    imported = await repository.create(imported_session(owner_id, agent.id))

    sessions, total = await repository.query(SessionFilter())
    assert total == 3

    sessions, total = await repository.query(SessionFilter(agent_id=agent.id))
    assert total == 2

    sessions, total = await repository.query(SessionFilter(agent_version_id=version.id))
    assert total == 1
    assert sessions[0].id == recorded.id

    sessions, total = await repository.query(
        SessionFilter(origin=SessionOrigin.IMPORTED)
    )
    assert total == 1
    assert sessions[0].id == imported.id

    sessions, total = await repository.query(SessionFilter(status=SessionStatus.FAILED))
    assert total == 1
    assert sessions[0].name == "run-2"

    sessions, total = await repository.query(SessionFilter(name="run-1"))
    assert total == 1

    sessions, total = await repository.query(
        SessionFilter(provider=SessionProvider.LANGFUSE, external_id="lf-1")
    )
    assert total == 1
    assert sessions[0].id == imported.id


async def test_query_time_ranges(setup: Setup) -> None:
    """Query sessions by start and end time ranges."""
    repository, agents, _, _, owner_id = setup
    agent = await create_agent(agents, owner_id)
    early = datetime(2026, 7, 1, 8, 0, tzinfo=UTC)
    late = datetime(2026, 7, 1, 18, 0, tzinfo=UTC)
    first = await repository.create(
        recorded_session(owner_id, agent.id, started_at=early, ended_at=STARTED_AT)
    )
    second = await repository.create(
        recorded_session(owner_id, agent.id, started_at=late, ended_at=ENDED_AT)
    )
    await repository.create(recorded_session(owner_id, agent.id))

    sessions, total = await repository.query(SessionFilter(started_after=STARTED_AT))
    assert total == 1
    assert sessions[0].id == second.id

    sessions, total = await repository.query(SessionFilter(started_before=STARTED_AT))
    assert total == 1
    assert sessions[0].id == first.id

    sessions, total = await repository.query(
        SessionFilter(ended_after=early, ended_before=STARTED_AT)
    )
    assert total == 1
    assert sessions[0].id == first.id


async def test_query_score_and_ranges(setup: Setup) -> None:
    """Query sessions by score presence, cost, and token ranges."""
    repository, agents, _, _, owner_id = setup
    agent = await create_agent(agents, owner_id)
    cheap = await repository.create(
        recorded_session(
            owner_id,
            agent.id,
            cost=Decimal("0.5"),
            tokens=TokenUsage(input_tokens=100, output_tokens=50),
            scores={"conciseness": 0.5},
        )
    )
    expensive = await repository.create(
        recorded_session(
            owner_id,
            agent.id,
            cost=Decimal("3"),
            tokens=TokenUsage(input_tokens=900, output_tokens=100),
        )
    )
    bare = await repository.create(recorded_session(owner_id, agent.id))

    sessions, total = await repository.query(SessionFilter(has_score=True))
    assert total == 1
    assert sessions[0].id == cheap.id

    sessions, total = await repository.query(SessionFilter(has_score=False))
    assert total == 2

    sessions, total = await repository.query(SessionFilter(min_cost=Decimal("1")))
    assert total == 1
    assert sessions[0].id == expensive.id

    sessions, total = await repository.query(SessionFilter(max_cost=Decimal("1")))
    assert total == 1
    assert sessions[0].id == cheap.id

    sessions, total = await repository.query(SessionFilter(min_total_tokens=500))
    assert total == 1
    assert sessions[0].id == expensive.id

    sessions, total = await repository.query(
        SessionFilter(min_total_tokens=1, max_total_tokens=500)
    )
    assert total == 1
    assert sessions[0].id == cheap.id

    sessions, total = await repository.query(SessionFilter(max_total_tokens=0))
    assert total == 1
    assert sessions[0].id == bare.id


async def test_query_by_tag(setup: Setup) -> None:
    """Query sessions attached to a tag name."""
    repository, agents, _, tags, owner_id = setup
    agent = await create_agent(agents, owner_id)
    tagged = await repository.create(recorded_session(owner_id, agent.id))
    await repository.create(recorded_session(owner_id, agent.id))
    tag = await tags.create(Tag(owner_id=owner_id, name="prod"))
    await tags.create_link(
        TagLink(
            tag_id=tag.id,
            resource_type=TagResourceType.SESSION,
            resource_id=tagged.id,
        )
    )

    sessions, total = await repository.query(SessionFilter(tag="prod"))
    assert total == 1
    assert sessions[0].id == tagged.id

    sessions, total = await repository.query(SessionFilter(tag="missing"))
    assert total == 0


async def test_query_pagination(setup: Setup) -> None:
    """Query sessions with pagination."""
    repository, agents, _, _, owner_id = setup
    agent = await create_agent(agents, owner_id)
    for name in ["one", "two", "three"]:
        await repository.create(recorded_session(owner_id, agent.id, name=name))

    sessions, total = await repository.query(SessionFilter(page=2, page_size=2))
    assert total == 3
    assert [session.name for session in sessions] == ["three"]


async def test_update(setup: Setup) -> None:
    """Persist field changes and renew the updated timestamp."""
    repository, agents, _, _, owner_id = setup
    agent = await create_agent(agents, owner_id)
    created = await repository.create(recorded_session(owner_id, agent.id))
    created.update_name("run-1")
    created.merge_scores({"conciseness": 0.5})
    updated = await repository.update(created)
    assert updated.name == "run-1"
    assert updated.scores == {"conciseness": 0.5}
    assert updated.created == created.created
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated > created.updated
    loaded = await repository.get(created.id)
    assert loaded == updated


async def test_update_not_found(setup: Setup) -> None:
    """Raise for an unknown session id."""
    repository, agents, _, _, owner_id = setup
    agent = await create_agent(agents, owner_id)
    session = recorded_session(owner_id, agent.id)
    with pytest.raises(SessionNotFound, match=f"Session {session.id} was not found"):
        await repository.update(session)


async def test_delete(setup: Setup) -> None:
    """Delete a stored session."""
    repository, agents, _, _, owner_id = setup
    agent = await create_agent(agents, owner_id)
    created = await repository.create(recorded_session(owner_id, agent.id))
    await repository.delete(created.id)
    with pytest.raises(SessionNotFound):
        await repository.get(created.id)


async def test_delete_removes_tag_links(setup: Setup) -> None:
    """Remove the session's tag links on delete."""
    repository, agents, _, tags, owner_id = setup
    agent = await create_agent(agents, owner_id)
    created = await repository.create(recorded_session(owner_id, agent.id))
    tag = await tags.create(Tag(owner_id=owner_id, name="prod"))
    await tags.create_link(
        TagLink(
            tag_id=tag.id,
            resource_type=TagResourceType.SESSION,
            resource_id=created.id,
        )
    )
    await repository.delete(created.id)
    with pytest.raises(TagLinkNotFound):
        await tags.delete_link(tag.id, TagResourceType.SESSION, created.id)


async def test_delete_not_found(setup: Setup) -> None:
    """Raise for an unknown session id."""
    repository, _, _, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(SessionNotFound, match=f"Session {missing_id} was not found"):
        await repository.delete(missing_id)


async def test_get_many(setup: Setup) -> None:
    """Load sessions by id and omit ids that do not resolve."""
    repository, agents, _, _, owner_id = setup
    agent = await create_agent(agents, owner_id)
    first = await repository.create(recorded_session(owner_id, agent.id))
    second = await repository.create(recorded_session(owner_id, agent.id))
    loaded = await repository.get_many([first.id, second.id, uuid.uuid4()])
    assert set(loaded) == {first.id, second.id}
    assert loaded[first.id] == first
    assert await repository.get_many([]) == {}
