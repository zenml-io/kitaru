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
"""Contract tests for session node repositories."""

import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from conftest import (
    FakeAgentRepository,
    FakeSessionNodeRepository,
    FakeSessionRepository,
    pg_session,
    postgres_available,
)
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.agent_repository import (
    SQLAgentRepository,
)
from kitaru.server.adapters.db.repositories.session_node_repository import (
    SQLSessionNodeRepository,
)
from kitaru.server.adapters.db.repositories.session_repository import (
    SQLSessionRepository,
)
from kitaru.server.application.interfaces.agent_repository import AgentRepository
from kitaru.server.application.interfaces.session_node_repository import (
    SessionNodeRepository,
)
from kitaru.server.application.interfaces.session_repository import (
    SessionRepository,
)
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.session import (
    Session,
    SessionNotFound,
    SessionOrigin,
    TokenUsage,
)
from kitaru.server.domain.session_node import (
    DuplicateNodeExternalId,
    DuplicateNodeKey,
    DuplicateNodeSequence,
    DuplicateSessionNodeId,
    NodeStatus,
    NodeType,
    SessionNode,
)

Setup = tuple[SessionNodeRepository, SessionRepository, AgentRepository, uuid.UUID]

STARTED_AT = datetime(2026, 7, 1, 12, 0, tzinfo=UTC)


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide each session node repository implementation plus an owner id."""
    if request.param == "fake":
        agents = FakeAgentRepository()
        sessions = FakeSessionRepository(agents)
        nodes = FakeSessionNodeRepository(sessions)
        yield nodes, sessions, agents, uuid.uuid4()
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        # The owner_id column has a foreign key to the account table, so
        # store the owning account first.
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        yield (
            SQLSessionNodeRepository(session),
            SQLSessionRepository(session),
            SQLAgentRepository(session),
            owner.id,
        )


async def create_session(
    sessions: SessionRepository, agents: AgentRepository, owner_id: uuid.UUID
) -> Session:
    """Store an agent and an in-progress recorded session.

    Args:
        sessions: Session repository.
        agents: Agent repository.
        owner_id: Id of the owning account.

    Returns:
        Stored session.
    """
    agent = await agents.create(
        Agent(owner_id=owner_id, name=f"bot-{uuid.uuid4().hex[:8]}")
    )
    return await sessions.create(
        Session(owner_id=owner_id, agent_id=agent.id, origin=SessionOrigin.RECORDED)
    )


def node(
    session_id: uuid.UUID,
    key: str = "span:run",
    sequence: int = 0,
    **overrides: object,
) -> SessionNode:
    """Build a session node entity.

    Args:
        session_id: Id of the session.
        key: Node key.
        sequence: Node sequence.
        **overrides: Field overrides.

    Returns:
        Session node entity.
    """
    values: dict[str, object] = {
        "session_id": session_id,
        "key": key,
        "sequence": sequence,
        "node_type": NodeType.SPAN,
        "name": "run",
        "status": NodeStatus.COMPLETED,
        **overrides,
    }
    return SessionNode.model_validate(values)


async def test_upsert_inserts_and_sets_timestamps(setup: Setup) -> None:
    """Insert new nodes with both timestamps set."""
    repository, sessions, agents, owner_id = setup
    session = await create_session(sessions, agents, owner_id)
    stored = await repository.upsert([node(session.id)])
    assert len(stored) == 1
    assert stored[0].session_id == session.id
    assert stored[0].key == "span:run"
    assert stored[0].created is not None
    assert stored[0].updated is not None


async def test_upsert_round_trips_all_fields(setup: Setup) -> None:
    """Store a node and round-trip every field."""
    repository, sessions, agents, owner_id = setup
    session = await create_session(sessions, agents, owner_id)
    parent = node(session.id)
    child = node(
        session.id,
        key="span:run/llm_call:chat",
        sequence=1,
        parent_id=parent.id,
        secondary_parent_ids=[parent.id],
        external_id="obs-1",
        trace_id="trace-1",
        node_type=NodeType.LLM_CALL,
        name="chat",
        status=NodeStatus.FAILED,
        error="rate limited",
        started_at=STARTED_AT,
        ended_at=STARTED_AT,
        inputs={"messages": ["hi"]},
        outputs={"content": "hello"},
        requested_model="gpt-5",
        model="gpt-5-mini",
        provider="openai",
        tokens=TokenUsage(input_tokens=10, output_tokens=2),
        cost=Decimal("0.01"),
        model_params={"temperature": 0.2},
        attributes={"mocked": True},
        metadata={"note": "x"},
    )
    await repository.upsert([parent, child])
    nodes = await repository.list_for_session(session.id, include_payloads=True)
    assert len(nodes) == 2
    loaded = nodes[1]
    assert loaded.parent_id == parent.id
    assert loaded.secondary_parent_ids == [parent.id]
    assert loaded.external_id == "obs-1"
    assert loaded.trace_id == "trace-1"
    assert loaded.node_type is NodeType.LLM_CALL
    assert loaded.status is NodeStatus.FAILED
    assert loaded.error == "rate limited"
    assert loaded.started_at == STARTED_AT
    assert loaded.inputs == {"messages": ["hi"]}
    assert loaded.outputs == {"content": "hello"}
    assert loaded.requested_model == "gpt-5"
    assert loaded.model == "gpt-5-mini"
    assert loaded.provider == "openai"
    assert loaded.tokens == TokenUsage(input_tokens=10, output_tokens=2)
    assert loaded.cost == Decimal("0.01")
    assert loaded.model_params == {"temperature": 0.2}
    assert loaded.attributes == {"mocked": True}
    assert loaded.metadata == {"note": "x"}


async def test_upsert_updates_existing_node(setup: Setup) -> None:
    """Update an existing node by id and renew the updated timestamp."""
    repository, sessions, agents, owner_id = setup
    session = await create_session(sessions, agents, owner_id)
    first = (await repository.upsert([node(session.id)]))[0]
    changed = node(
        session.id,
        id=first.id,
        status=NodeStatus.FAILED,
        error="boom",
        outputs={"partial": True},
    )
    second = (await repository.upsert([changed]))[0]
    assert second.id == first.id
    assert second.status is NodeStatus.FAILED
    assert second.error == "boom"
    assert second.outputs == {"partial": True}
    assert second.created == first.created
    assert second.updated is not None
    assert first.updated is not None
    assert second.updated > first.updated
    nodes = await repository.list_for_session(session.id, include_payloads=True)
    assert len(nodes) == 1


async def test_upsert_duplicate_sequence(setup: Setup) -> None:
    """Reject a second node with the same sequence in the session."""
    repository, sessions, agents, owner_id = setup
    session = await create_session(sessions, agents, owner_id)
    await repository.upsert([node(session.id)])
    with pytest.raises(
        DuplicateNodeSequence,
        match=f"A node sequence is already registered in session {session.id}",
    ):
        await repository.upsert([node(session.id, key="span:other", sequence=0)])
    stored = await repository.upsert([node(session.id, key="span:other", sequence=1)])
    assert stored[0].sequence == 1


async def test_upsert_duplicate_external_id(setup: Setup) -> None:
    """Reject a second node with the same external id in the session."""
    repository, sessions, agents, owner_id = setup
    session = await create_session(sessions, agents, owner_id)
    await repository.upsert([node(session.id, external_id="obs-1")])
    with pytest.raises(
        DuplicateNodeExternalId,
        match=f"A node external id is already registered in session {session.id}",
    ):
        await repository.upsert(
            [node(session.id, key="span:other", sequence=1, external_id="obs-1")]
        )


async def test_upsert_duplicate_key(setup: Setup) -> None:
    """Reject a second node with the same key in the session."""
    repository, sessions, agents, owner_id = setup
    session = await create_session(sessions, agents, owner_id)
    await repository.upsert([node(session.id)])
    with pytest.raises(
        DuplicateNodeKey,
        match=f"A node key is already registered in session {session.id}",
    ):
        await repository.upsert([node(session.id, sequence=1)])


async def test_upsert_same_key_other_session(setup: Setup) -> None:
    """Register the same key and sequence in two sessions."""
    repository, sessions, agents, owner_id = setup
    session = await create_session(sessions, agents, owner_id)
    other = await create_session(sessions, agents, owner_id)
    await repository.upsert([node(session.id, external_id="obs-1")])
    stored = await repository.upsert([node(other.id, external_id="obs-1")])
    assert stored[0].session_id == other.id


async def test_upsert_id_in_another_session(setup: Setup) -> None:
    """Reject a node id that is already registered in another session."""
    repository, sessions, agents, owner_id = setup
    session = await create_session(sessions, agents, owner_id)
    other = await create_session(sessions, agents, owner_id)
    stored = (await repository.upsert([node(session.id)]))[0]
    with pytest.raises(
        DuplicateSessionNodeId,
        match=f"Session node {stored.id} is already registered in another session",
    ):
        await repository.upsert([node(other.id, id=stored.id)])


async def test_upsert_unknown_session(setup: Setup) -> None:
    """Raise for an unknown session id."""
    repository, _, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(SessionNotFound, match=f"Session {missing_id} was not found"):
        await repository.upsert([node(missing_id)])


async def test_upsert_empty_batch(setup: Setup) -> None:
    """Return an empty list for an empty batch."""
    repository, _, _, _ = setup
    assert await repository.upsert([]) == []


async def test_list_ordered_by_sequence(setup: Setup) -> None:
    """List nodes ordered by sequence."""
    repository, sessions, agents, owner_id = setup
    session = await create_session(sessions, agents, owner_id)
    await repository.upsert(
        [
            node(session.id, key="span:late", sequence=5, name="late"),
            node(session.id, key="span:early", sequence=1, name="early"),
        ]
    )
    nodes = await repository.list_for_session(session.id, include_payloads=False)
    assert [stored.name for stored in nodes] == ["early", "late"]


async def test_list_excludes_payloads(setup: Setup) -> None:
    """Exclude inputs, outputs, and attributes unless requested."""
    repository, sessions, agents, owner_id = setup
    session = await create_session(sessions, agents, owner_id)
    await repository.upsert(
        [
            node(
                session.id,
                inputs={"city": "Berlin"},
                outputs={"temp": 21},
                attributes={"mocked": False},
            )
        ]
    )
    nodes = await repository.list_for_session(session.id, include_payloads=False)
    assert nodes[0].inputs is None
    assert nodes[0].outputs is None
    assert nodes[0].attributes == {}
    assert nodes[0].name == "run"

    nodes = await repository.list_for_session(session.id, include_payloads=True)
    assert nodes[0].inputs == {"city": "Berlin"}
    assert nodes[0].outputs == {"temp": 21}
    assert nodes[0].attributes == {"mocked": False}


async def test_session_delete_cascades_nodes(setup: Setup) -> None:
    """Remove a session's nodes when the session is deleted."""
    repository, sessions, agents, owner_id = setup
    session = await create_session(sessions, agents, owner_id)
    parent = node(session.id)
    child = node(session.id, key="span:run/span:step", sequence=1, parent_id=parent.id)
    await repository.upsert([parent, child])
    await sessions.delete(session.id)
    assert await repository.list_for_session(session.id, include_payloads=True) == []
