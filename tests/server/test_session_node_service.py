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
"""Tests for session node use cases."""

import uuid

import pytest

from conftest import (
    FakeAgentRepository,
    FakeSessionNodeRepository,
    FakeSessionRepository,
)
from kitaru.hashing import tool_call_cache_key
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.session_nodes import SessionNodeUpsert
from kitaru.server.application.services.session_node_service import (
    SessionNodeService,
)
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.ids import uuid7
from kitaru.server.domain.session import (
    Session,
    SessionNotFound,
    SessionNotInProgress,
    SessionOrigin,
    SessionProvider,
    SessionStatus,
)
from kitaru.server.domain.session_node import (
    NodeStatus,
    NodeType,
    UnknownParentNode,
)

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))


@pytest.fixture
def agent_repository() -> FakeAgentRepository:
    """Provide a fake agent repository."""
    return FakeAgentRepository()


@pytest.fixture
def session_repository(
    agent_repository: FakeAgentRepository,
) -> FakeSessionRepository:
    """Provide a fake session repository."""
    return FakeSessionRepository(agent_repository)


@pytest.fixture
def repository(
    session_repository: FakeSessionRepository,
) -> FakeSessionNodeRepository:
    """Provide a fake session node repository."""
    return FakeSessionNodeRepository(session_repository)


@pytest.fixture
def service(
    repository: FakeSessionNodeRepository,
    session_repository: FakeSessionRepository,
) -> SessionNodeService:
    """Provide a session node service backed by the fake repositories."""
    return SessionNodeService(
        repository=repository, session_repository=session_repository
    )


@pytest.fixture
async def session(
    agent_repository: FakeAgentRepository,
    session_repository: FakeSessionRepository,
) -> Session:
    """Provide a stored in-progress recorded session."""
    agent = await agent_repository.create(
        Agent(owner_id=ACTOR.account.id, name="support-bot")
    )
    return await session_repository.create(
        Session(
            owner_id=ACTOR.account.id,
            agent_id=agent.id,
            origin=SessionOrigin.RECORDED,
        )
    )


def upsert(
    sequence: int,
    node_type: NodeType = NodeType.SPAN,
    name: str = "run",
    parent_id: uuid.UUID | None = None,
    **overrides: object,
) -> SessionNodeUpsert:
    """Build a session node upsert command.

    Args:
        sequence: Node sequence.
        node_type: Node type.
        name: Node name.
        parent_id: Id of the primary parent.
        **overrides: Field overrides.

    Returns:
        Session node upsert command.
    """
    values: dict[str, object] = {
        "id": uuid7(),
        "sequence": sequence,
        "node_type": node_type,
        "name": name,
        "parent_id": parent_id,
        "status": NodeStatus.COMPLETED,
        **overrides,
    }
    return SessionNodeUpsert.model_validate(values)


async def test_ingest_computes_keys(
    service: SessionNodeService, session: Session
) -> None:
    """Compute path keys with occurrence indexes and nesting."""
    root = upsert(0, NodeType.SPAN, "run")
    chat_one = upsert(1, NodeType.LLM_CALL, "chat", parent_id=root.id)
    chat_two = upsert(2, NodeType.LLM_CALL, "chat", parent_id=root.id)
    weather = upsert(
        3,
        NodeType.TOOL_CALL,
        "get_weather",
        parent_id=chat_two.id,
        tool_name="get_weather",
        inputs={"city": "Berlin"},
    )
    nodes = await service.ingest_nodes(
        session.id, [root, chat_one, chat_two, weather], actor=ACTOR
    )
    assert [node.key for node in nodes] == [
        "span:run",
        "span:run/llm_call:chat",
        "span:run/llm_call:chat#2",
        "span:run/llm_call:chat#2/tool_call:get_weather",
    ]
    assert nodes[0].created is not None
    assert nodes[0].updated is not None


async def test_ingest_occurrence_counts_same_segment_only(
    service: SessionNodeService, session: Session
) -> None:
    """Count occurrences among siblings with the same type and name only."""
    root = upsert(0, NodeType.SPAN, "run")
    span_chat = upsert(1, NodeType.SPAN, "chat", parent_id=root.id)
    llm_chat = upsert(2, NodeType.LLM_CALL, "chat", parent_id=root.id)
    other = upsert(3, NodeType.LLM_CALL, "classify", parent_id=root.id)
    nested_chat = upsert(4, NodeType.SPAN, "chat", parent_id=span_chat.id)
    nodes = await service.ingest_nodes(
        session.id, [root, span_chat, llm_chat, other, nested_chat], actor=ACTOR
    )
    assert [node.key for node in nodes] == [
        "span:run",
        "span:run/span:chat",
        "span:run/llm_call:chat",
        "span:run/llm_call:classify",
        "span:run/span:chat/span:chat",
    ]


async def test_ingest_across_batches(
    service: SessionNodeService, session: Session
) -> None:
    """Resolve parents and occurrences across batches."""
    root = upsert(0, NodeType.SPAN, "run")
    first_chat = upsert(1, NodeType.LLM_CALL, "chat", parent_id=root.id)
    await service.ingest_nodes(session.id, [root, first_chat], actor=ACTOR)

    second_chat = upsert(2, NodeType.LLM_CALL, "chat", parent_id=root.id)
    nodes = await service.ingest_nodes(session.id, [second_chat], actor=ACTOR)
    assert nodes[0].key == "span:run/llm_call:chat#2"


async def test_ingest_computes_cache_key(
    service: SessionNodeService, session: Session
) -> None:
    """Compute the cache key of tool call nodes only."""
    tool = upsert(
        0,
        NodeType.TOOL_CALL,
        "get_weather",
        tool_name="get_weather",
        inputs={"city": "Berlin", "unit": "c"},
    )
    llm = upsert(1, NodeType.LLM_CALL, "chat", inputs={"messages": []})
    nodes = await service.ingest_nodes(session.id, [tool, llm], actor=ACTOR)
    assert nodes[0].cache_key == tool_call_cache_key(
        "get_weather", {"city": "Berlin", "unit": "c"}
    )
    assert nodes[1].cache_key is None


async def test_ingest_unknown_parent(
    service: SessionNodeService, session: Session
) -> None:
    """Reject the whole batch for an unknown parent."""
    missing_id = uuid.uuid4()
    root = upsert(0)
    child = upsert(1, parent_id=missing_id)
    with pytest.raises(
        UnknownParentNode, match=f"Parent node {missing_id} was not found"
    ):
        await service.ingest_nodes(session.id, [root, child], actor=ACTOR)
    assert await service.list_nodes(session.id, True, actor=ACTOR) == []


async def test_ingest_child_before_parent(
    service: SessionNodeService, session: Session
) -> None:
    """Reject a batch where a child arrives before its parent."""
    root = upsert(0)
    child = upsert(1, parent_id=root.id)
    with pytest.raises(UnknownParentNode):
        await service.ingest_nodes(session.id, [child, root], actor=ACTOR)


async def test_ingest_self_parent(
    service: SessionNodeService, session: Session
) -> None:
    """Reject a node referencing itself as parent."""
    node = upsert(0)
    with pytest.raises(UnknownParentNode):
        await service.ingest_nodes(
            session.id, [upsert(0, parent_id=node.id, id=node.id)], actor=ACTOR
        )


async def test_ingest_unknown_secondary_parent(
    service: SessionNodeService, session: Session
) -> None:
    """Reject an unknown secondary parent."""
    missing_id = uuid.uuid4()
    root = upsert(0)
    child = upsert(1, parent_id=root.id, secondary_parent_ids=[missing_id])
    with pytest.raises(UnknownParentNode):
        await service.ingest_nodes(session.id, [root, child], actor=ACTOR)


async def test_ingest_secondary_parent(
    service: SessionNodeService, session: Session
) -> None:
    """Store secondary parent edges."""
    left = upsert(0, name="left")
    right = upsert(1, name="right")
    join = upsert(2, name="join", parent_id=left.id, secondary_parent_ids=[right.id])
    nodes = await service.ingest_nodes(session.id, [left, right, join], actor=ACTOR)
    assert nodes[2].secondary_parent_ids == [right.id]


async def test_ingest_idempotent_retry(
    service: SessionNodeService, session: Session
) -> None:
    """Re-upsert the same batch without conflicts and with stable keys."""
    root = upsert(0, NodeType.SPAN, "run")
    chat = upsert(
        1,
        NodeType.LLM_CALL,
        "chat",
        parent_id=root.id,
        inputs={"messages": ["hi"]},
    )
    first = await service.ingest_nodes(session.id, [root, chat], actor=ACTOR)
    second = await service.ingest_nodes(session.id, [root, chat], actor=ACTOR)
    assert [node.id for node in second] == [node.id for node in first]
    assert [node.key for node in second] == ["span:run", "span:run/llm_call:chat"]
    assert second[0].created == first[0].created
    assert second[0].updated is not None
    assert first[0].updated is not None
    assert second[0].updated > first[0].updated
    nodes = await service.list_nodes(session.id, True, actor=ACTOR)
    assert len(nodes) == 2


async def test_ingest_finished_session(
    service: SessionNodeService,
    session_repository: FakeSessionRepository,
    session: Session,
) -> None:
    """Reject node ingest for a finished recorded session."""
    session.status = SessionStatus.COMPLETED
    await session_repository.update(session)
    with pytest.raises(
        SessionNotInProgress, match=f"Session {session.id} is not in progress"
    ):
        await service.ingest_nodes(session.id, [upsert(0)], actor=ACTOR)


async def test_ingest_imported_session(
    service: SessionNodeService,
    agent_repository: FakeAgentRepository,
    session_repository: FakeSessionRepository,
) -> None:
    """Accept node ingest for a terminal imported session."""
    agent = await agent_repository.create(
        Agent(owner_id=ACTOR.account.id, name="import-bot")
    )
    imported = await session_repository.create(
        Session(
            owner_id=ACTOR.account.id,
            agent_id=agent.id,
            origin=SessionOrigin.IMPORTED,
            status=SessionStatus.COMPLETED,
            provider=SessionProvider.LANGFUSE,
            external_id="lf-1",
        )
    )
    nodes = await service.ingest_nodes(imported.id, [upsert(0)], actor=ACTOR)
    assert nodes[0].key == "span:run"


async def test_ingest_unknown_session(service: SessionNodeService) -> None:
    """Raise for an unknown session id."""
    missing_id = uuid.uuid4()
    with pytest.raises(SessionNotFound, match=f"Session {missing_id} was not found"):
        await service.ingest_nodes(missing_id, [upsert(0)], actor=ACTOR)


async def test_list_nodes_ordered_by_sequence(
    service: SessionNodeService, session: Session
) -> None:
    """List nodes ordered by sequence."""
    root = upsert(0, name="run")
    late = upsert(5, name="late", parent_id=root.id)
    early = upsert(2, name="early", parent_id=root.id)
    await service.ingest_nodes(session.id, [root, late, early], actor=ACTOR)
    nodes = await service.list_nodes(session.id, False, actor=ACTOR)
    assert [node.name for node in nodes] == ["run", "early", "late"]


async def test_list_nodes_excludes_payloads(
    service: SessionNodeService, session: Session
) -> None:
    """Exclude inputs, outputs, and attributes unless requested."""
    node = upsert(
        0,
        NodeType.TOOL_CALL,
        "get_weather",
        tool_name="get_weather",
        inputs={"city": "Berlin"},
        outputs={"temp": 21},
        attributes={"mocked": False},
    )
    await service.ingest_nodes(session.id, [node], actor=ACTOR)

    nodes = await service.list_nodes(session.id, False, actor=ACTOR)
    assert nodes[0].inputs is None
    assert nodes[0].outputs is None
    assert nodes[0].attributes == {}
    assert nodes[0].cache_key is not None

    nodes = await service.list_nodes(session.id, True, actor=ACTOR)
    assert nodes[0].inputs == {"city": "Berlin"}
    assert nodes[0].outputs == {"temp": 21}
    assert nodes[0].attributes == {"mocked": False}


async def test_list_nodes_unknown_session(service: SessionNodeService) -> None:
    """Raise for an unknown session id."""
    with pytest.raises(SessionNotFound):
        await service.list_nodes(uuid.uuid4(), False, actor=ACTOR)
