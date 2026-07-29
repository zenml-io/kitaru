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
from decimal import Decimal
from typing import Any

import pytest

from conftest import FakeSessionNodeRepository, FakeSessionRepository, create_session
from kitaru.api_models.v1.session import SessionOrigin, SessionStatus, TokenUsage
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.session import SessionUpdate
from kitaru.server.application.models.session_node import (
    SessionNodeFilter,
    SessionNodeUpsert,
)
from kitaru.server.application.services.session_node_service import (
    SessionNodeService,
)
from kitaru.server.application.services.session_service import SessionService
from kitaru.server.domain.account import Account
from kitaru.server.domain.session_node import SessionNodeParentNotFound

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))


@pytest.fixture
def session_repository() -> FakeSessionRepository:
    """Provide a fake session repository."""
    return FakeSessionRepository()


@pytest.fixture
def node_repository() -> FakeSessionNodeRepository:
    """Provide a fake session node repository."""
    return FakeSessionNodeRepository()


@pytest.fixture
def service(
    node_repository: FakeSessionNodeRepository,
    session_repository: FakeSessionRepository,
) -> SessionNodeService:
    """Provide a session node service backed by the fake repositories."""
    return SessionNodeService(
        repository=node_repository, session_repository=session_repository
    )


@pytest.fixture
def session_service(session_repository: FakeSessionRepository) -> SessionService:
    """Provide a session service sharing the fake session repository."""
    return SessionService(repository=session_repository)


@pytest.fixture
async def session_id(session_repository: FakeSessionRepository) -> uuid.UUID:
    """Provide the id of an in-progress recorded session."""
    session = await create_session(
        session_repository,
        ACTOR.account.id,
        agent_id=uuid.uuid4(),
        origin=SessionOrigin.RECORDED,
        status=SessionStatus.IN_PROGRESS,
    )
    return session.id


def _llm_node(index: int, **overrides: Any) -> SessionNodeUpsert:
    values: dict[str, Any] = {
        "index": index,
        "node_type": NodeType.LLM_CALL,
        "name": "call",
        "status": NodeStatus.COMPLETED,
    }
    values.update(overrides)
    return SessionNodeUpsert(**values)


async def test_ingest_insert_assigns_ids_and_rollups(
    service: SessionNodeService,
    session_repository: FakeSessionRepository,
    session_id: uuid.UUID,
) -> None:
    """Insert new nodes and roll up their cost, tokens, and call counts."""
    batch = [
        _llm_node(0, cost=Decimal("1.50"), tokens=TokenUsage(input_tokens=10)),
        SessionNodeUpsert(
            index=1,
            parent_index=0,
            node_type=NodeType.TOOL_CALL,
            name="search",
            status=NodeStatus.COMPLETED,
            tool_name="search",
            inputs={"q": "hi"},
        ),
    ]
    stored = await service.ingest_nodes(session_id, batch, actor=ACTOR)

    assert stored[0].parent_id is None
    assert stored[1].parent_id == stored[0].id
    assert stored[1].cache_key is not None

    session = await session_repository.get(session_id)
    assert session.cost == Decimal("1.50")
    assert session.tokens is not None
    assert session.tokens.input_tokens == 10
    assert session.llm_call_count == 1
    assert session.tool_call_count == 1


async def test_ingest_cache_key_null_when_tool_name_missing(
    service: SessionNodeService, session_id: uuid.UUID
) -> None:
    """Leave cache_key null on a tool call node without a tool name."""
    batch = [
        SessionNodeUpsert(
            index=0,
            node_type=NodeType.TOOL_CALL,
            name="unknown-tool",
            status=NodeStatus.COMPLETED,
        )
    ]
    stored = await service.ingest_nodes(session_id, batch, actor=ACTOR)
    assert stored[0].cache_key is None


async def test_ingest_secondary_parents_resolve(
    service: SessionNodeService, session_id: uuid.UUID
) -> None:
    """Resolve secondary_parent_indexes into secondary_parent_ids."""
    batch = [
        _llm_node(0),
        _llm_node(1),
        SessionNodeUpsert(
            index=2,
            parent_index=0,
            secondary_parent_indexes=[1],
            node_type=NodeType.SUBAGENT_CALL,
            name="merge",
            status=NodeStatus.COMPLETED,
        ),
    ]
    stored = await service.ingest_nodes(session_id, batch, actor=ACTOR)
    assert stored[2].parent_id == stored[0].id
    assert stored[2].secondary_parent_ids == [stored[1].id]


async def test_ingest_parent_resolves_against_stored_row(
    service: SessionNodeService, session_id: uuid.UUID
) -> None:
    """Resolve a parent_index against a row stored in an earlier batch."""
    first = await service.ingest_nodes(session_id, [_llm_node(0)], actor=ACTOR)
    second = await service.ingest_nodes(
        session_id,
        [
            SessionNodeUpsert(
                index=1,
                parent_index=0,
                node_type=NodeType.TOOL_CALL,
                name="search",
                status=NodeStatus.COMPLETED,
            )
        ],
        actor=ACTOR,
    )
    assert second[0].parent_id == first[0].id


async def test_ingest_unresolved_parent_index_raises(
    service: SessionNodeService, session_id: uuid.UUID
) -> None:
    """Raise when a parent_index matches no stored or batched node."""
    batch = [
        SessionNodeUpsert(
            index=1,
            parent_index=0,
            node_type=NodeType.TOOL_CALL,
            name="search",
            status=NodeStatus.COMPLETED,
        )
    ]
    with pytest.raises(SessionNodeParentNotFound):
        await service.ingest_nodes(session_id, batch, actor=ACTOR)


async def test_ingest_replace_clears_omitted_fields(
    service: SessionNodeService, session_id: uuid.UUID
) -> None:
    """Replace a node whole, clearing fields the resent version omits."""
    created = await service.ingest_nodes(
        session_id,
        [_llm_node(0, error="boom", requested_model="gpt", tool_name="unused")],
        actor=ACTOR,
    )
    replaced = await service.ingest_nodes(session_id, [_llm_node(0)], actor=ACTOR)
    assert replaced[0].id == created[0].id
    assert replaced[0].error is None
    assert replaced[0].requested_model is None
    assert replaced[0].tool_name is None


async def test_ingest_replace_preserves_id(
    service: SessionNodeService, session_id: uuid.UUID
) -> None:
    """Preserve the row id when replacing an already-stored index."""
    created = await service.ingest_nodes(session_id, [_llm_node(0)], actor=ACTOR)
    replaced = await service.ingest_nodes(
        session_id, [_llm_node(0, name="renamed")], actor=ACTOR
    )
    assert replaced[0].id == created[0].id
    assert replaced[0].name == "renamed"


async def test_ingest_replace_updates_rollup_delta(
    service: SessionNodeService,
    session_repository: FakeSessionRepository,
    session_id: uuid.UUID,
) -> None:
    """Correct the session rollup when a replace changes cost and tokens."""
    await service.ingest_nodes(
        session_id,
        [_llm_node(0, cost=Decimal("1.00"), tokens=TokenUsage(input_tokens=10))],
        actor=ACTOR,
    )
    await service.ingest_nodes(
        session_id,
        [_llm_node(0, cost=Decimal("4.00"), tokens=TokenUsage(input_tokens=30))],
        actor=ACTOR,
    )
    session = await session_repository.get(session_id)
    assert session.cost == Decimal("4.00")
    assert session.tokens is not None
    assert session.tokens.input_tokens == 30
    assert session.llm_call_count == 1


async def test_ingest_replace_changing_node_type_updates_call_counts(
    service: SessionNodeService,
    session_repository: FakeSessionRepository,
    session_id: uuid.UUID,
) -> None:
    """Move the call count from one kind to another when the type changes."""
    await service.ingest_nodes(session_id, [_llm_node(0)], actor=ACTOR)
    await service.ingest_nodes(
        session_id,
        [
            SessionNodeUpsert(
                index=0,
                node_type=NodeType.SPAN,
                name="span",
                status=NodeStatus.COMPLETED,
            )
        ],
        actor=ACTOR,
    )
    session = await session_repository.get(session_id)
    assert session.llm_call_count == 0
    assert session.tool_call_count == 0


async def test_ingest_retry_identical_batch_nets_zero_delta(
    service: SessionNodeService,
    session_repository: FakeSessionRepository,
    session_id: uuid.UUID,
) -> None:
    """Net a zero rollup delta when an identical batch is retried."""
    batch = [_llm_node(0, cost=Decimal("2.00"), tokens=TokenUsage(input_tokens=5))]
    await service.ingest_nodes(session_id, batch, actor=ACTOR)
    before = await session_repository.get(session_id)
    await service.ingest_nodes(session_id, batch, actor=ACTOR)
    after = await session_repository.get(session_id)
    assert after.cost == before.cost
    assert after.tokens == before.tokens
    assert after.llm_call_count == before.llm_call_count


async def test_ingest_into_terminal_imported_session_allowed(
    service: SessionNodeService,
    session_service: SessionService,
    session_repository: FakeSessionRepository,
) -> None:
    """Allow node ingest into an imported session created already terminal."""
    session = await create_session(
        session_repository,
        ACTOR.account.id,
        agent_id=uuid.uuid4(),
        origin=SessionOrigin.IMPORTED,
        status=SessionStatus.COMPLETED,
    )
    stored = await service.ingest_nodes(session.id, [_llm_node(0)], actor=ACTOR)
    assert len(stored) == 1


async def test_ingest_into_terminal_recorded_session_rejected(
    service: SessionNodeService,
    session_service: SessionService,
    session_id: uuid.UUID,
) -> None:
    """Reject node ingest into a terminal recorded session."""
    await session_service.update_session(
        session_id, SessionUpdate(status=SessionStatus.COMPLETED), actor=ACTOR
    )
    with pytest.raises(Exception, match="does not accept node ingestion"):
        await service.ingest_nodes(session_id, [_llm_node(0)], actor=ACTOR)


async def test_list_nodes_include_payloads_true(
    service: SessionNodeService, session_id: uuid.UUID
) -> None:
    """Populate inputs, outputs, and attributes when include_payloads is set."""
    await service.ingest_nodes(
        session_id,
        [_llm_node(0, inputs={"q": "hi"}, outputs={"a": "there"}, attributes={"k": 1})],
        actor=ACTOR,
    )
    nodes, next_cursor = await service.list_nodes(
        SessionNodeFilter(session_id=session_id, include_payloads=True), actor=ACTOR
    )
    assert next_cursor is None
    assert nodes[0].inputs == {"q": "hi"}
    assert nodes[0].outputs == {"a": "there"}
    assert nodes[0].attributes == {"k": 1}


async def test_list_nodes_include_payloads_false(
    service: SessionNodeService, session_id: uuid.UUID
) -> None:
    """Null inputs, outputs, and attributes when include_payloads is unset."""
    await service.ingest_nodes(
        session_id,
        [_llm_node(0, inputs={"q": "hi"}, outputs={"a": "there"}, attributes={"k": 1})],
        actor=ACTOR,
    )
    nodes, _ = await service.list_nodes(
        SessionNodeFilter(session_id=session_id, include_payloads=False), actor=ACTOR
    )
    assert nodes[0].inputs is None
    assert nodes[0].outputs is None
    assert nodes[0].attributes is None


async def test_list_nodes_ordered_by_index_with_pagination(
    service: SessionNodeService, session_id: uuid.UUID
) -> None:
    """Page through nodes in index-ascending order via next_cursor."""
    batch = [_llm_node(index) for index in (2, 0, 1, 4, 3)]
    await service.ingest_nodes(session_id, batch, actor=ACTOR)

    collected: list[int] = []
    cursor = None
    while True:
        nodes, next_cursor = await service.list_nodes(
            SessionNodeFilter(session_id=session_id, cursor=cursor, size=2),
            actor=ACTOR,
        )
        collected.extend(node.index for node in nodes)
        if next_cursor is None:
            break
        cursor = next_cursor

    assert collected == [0, 1, 2, 3, 4]


async def test_ingest_empty_batch_is_a_no_op(
    service: SessionNodeService, session_id: uuid.UUID
) -> None:
    """Return an empty list for an empty batch without touching rollups."""
    stored = await service.ingest_nodes(session_id, [], actor=ACTOR)
    assert stored == []
