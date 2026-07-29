#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
"""Tests for v2 session-node ingestion."""

import uuid
from decimal import Decimal
from typing import Any

import pytest

from kitaru.cache_keys import compute_tool_cache_key
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.session_node import SessionNodeUpsert
from kitaru.server.application.services.session_node_service import (
    SessionNodeService,
)
from kitaru.server.domain.account import Account
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.session import (
    Session,
    SessionOrigin,
    SessionRollups,
    SessionStatus,
    TokenUsage,
)
from kitaru.server.domain.session_node import (
    NodeStatus,
    NodeType,
    SessionNode,
)


def actor() -> AuthContext:
    """Create a caller context."""
    return AuthContext(account=Account(name="tester"))


class SessionRepositoryFake:
    """Session repository fake recording aggregate deltas."""

    def __init__(self, session: Session) -> None:
        self.session = session
        self.rollups: list[SessionRollups] = []

    async def get(self, session_id: uuid.UUID):
        assert session_id == self.session.id
        return self.session

    async def apply_rollups(self, session_id: uuid.UUID, rollups: SessionRollups):
        self.rollups.append(rollups)
        return self.session


class NodeRepositoryFake:
    """Index-addressed node repository fake."""

    def __init__(self, *nodes: SessionNode) -> None:
        self.nodes = {node.index: node for node in nodes}
        self.lookup_indexes: list[int] = []

    async def get_by_indexes(self, session_id: uuid.UUID, indexes: list[int]):
        self.lookup_indexes = indexes
        return {index: self.nodes[index] for index in indexes if index in self.nodes}

    async def get_indexes_by_ids(self, session_id: uuid.UUID, ids: list[uuid.UUID]):
        return {node.id: node.index for node in self.nodes.values() if node.id in ids}

    async def upsert_many(self, nodes: list[SessionNode]):
        self.nodes.update({node.index: node for node in nodes})
        return nodes


class ListingNodeRepositoryFake(NodeRepositoryFake):
    """Node repository returning one configured page."""

    def __init__(self, *nodes: SessionNode, page: list[SessionNode]) -> None:
        super().__init__(*nodes)
        self.page = page
        self.index_lookup_ids: list[uuid.UUID] = []

    async def list_nodes(
        self,
        session_id: uuid.UUID,
        cursor: str | None,
        size: int,
        include_payloads: bool,
    ):
        return self.page, "next"

    async def get_indexes_by_ids(self, session_id: uuid.UUID, ids: list[uuid.UUID]):
        self.index_lookup_ids = ids
        return await super().get_indexes_by_ids(session_id, ids)


async def test_ingest_resolves_parents_cache_keys_and_rollup_deltas() -> None:
    """Resolve stored/in-batch parents and subtract replaced-node aggregates."""
    session = Session(
        owner_id=uuid.uuid4(),
        agent_id=uuid.uuid4(),
        origin=SessionOrigin.RECORDED,
    )
    old = SessionNode(
        session_id=session.id,
        index=0,
        node_type=NodeType.LLM_CALL,
        name="old",
        status=NodeStatus.COMPLETED,
        cost=Decimal("1.5"),
        tokens=TokenUsage(input_tokens=10),
    )
    node_repository = NodeRepositoryFake(old)
    session_repository = SessionRepositoryFake(session)
    node_repository_argument: Any = node_repository
    session_repository_argument: Any = session_repository
    service = SessionNodeService(node_repository_argument, session_repository_argument)
    stored, index_by_id = await service.ingest_nodes(
        session.id,
        [
            SessionNodeUpsert(
                index=0,
                node_type=NodeType.SPAN,
                name="replacement",
                status=NodeStatus.COMPLETED,
            ),
            SessionNodeUpsert(
                index=1,
                parent_index=0,
                node_type=NodeType.TOOL_CALL,
                name="tool",
                status=NodeStatus.COMPLETED,
                tool_name="lookup",
                inputs={"key": "value"},
                cost=Decimal("0.5"),
                tokens=TokenUsage(output_tokens=3),
            ),
        ],
        actor(),
    )
    assert set(node_repository.lookup_indexes) == {0, 1}
    assert stored[0].id == old.id
    assert index_by_id[old.id] == 0
    assert stored[1].parent_id == old.id
    assert stored[1].cache_key == compute_tool_cache_key("lookup", {"key": "value"})
    delta = session_repository.rollups[0]
    assert delta.cost == Decimal("-1.0")
    assert delta.input_tokens == -10
    assert delta.output_tokens == 3
    assert delta.llm_call_count == -1
    assert delta.tool_call_count == 1


async def test_ingest_rejects_unknown_and_forward_parents() -> None:
    """Reject unresolved parents and references that do not precede a child."""
    session = Session(
        owner_id=uuid.uuid4(),
        agent_id=uuid.uuid4(),
        origin=SessionOrigin.RECORDED,
    )
    node_repository: Any = NodeRepositoryFake()
    session_repository: Any = SessionRepositoryFake(session)
    service = SessionNodeService(node_repository, session_repository)
    base: dict[str, Any] = {
        "index": 1,
        "node_type": NodeType.SPAN,
        "name": "child",
        "status": NodeStatus.COMPLETED,
    }
    with pytest.raises(ValidationError, match="Unknown"):
        await service.ingest_nodes(
            session.id,
            [SessionNodeUpsert(parent_index=0, **base)],
            actor(),
        )
    with pytest.raises(ValidationError, match="precede"):
        await service.ingest_nodes(
            session.id,
            [SessionNodeUpsert(parent_index=2, **base)],
            actor(),
        )


async def test_list_resolves_only_referenced_parent_indexes() -> None:
    """Load parent indexes without scanning unrelated session nodes."""
    session = Session(
        owner_id=uuid.uuid4(),
        agent_id=uuid.uuid4(),
        origin=SessionOrigin.RECORDED,
    )
    parent = SessionNode(
        session_id=session.id,
        index=0,
        node_type=NodeType.SPAN,
        name="parent",
        status=NodeStatus.COMPLETED,
    )
    child = SessionNode(
        session_id=session.id,
        parent_id=parent.id,
        index=1,
        node_type=NodeType.SPAN,
        name="child",
        status=NodeStatus.COMPLETED,
    )
    unrelated = SessionNode(
        session_id=session.id,
        index=2,
        node_type=NodeType.SPAN,
        name="unrelated",
        status=NodeStatus.COMPLETED,
    )
    node_repository = ListingNodeRepositoryFake(parent, child, unrelated, page=[child])
    node_repository_argument: Any = node_repository
    session_repository_argument: Any = SessionRepositoryFake(session)
    service = SessionNodeService(node_repository_argument, session_repository_argument)

    nodes, cursor, index_by_id = await service.list_nodes(
        session.id, None, 1, False, actor()
    )

    assert nodes == [child]
    assert cursor == "next"
    assert node_repository.index_lookup_ids == [parent.id]
    assert index_by_id == {child.id: 1, parent.id: 0}


async def test_terminal_non_imported_session_rejects_nodes() -> None:
    """Enforce the session's ingest lifecycle rule."""
    session = Session(
        owner_id=uuid.uuid4(),
        agent_id=uuid.uuid4(),
        origin=SessionOrigin.RECORDED,
        status=SessionStatus.COMPLETED,
    )
    node_repository: Any = NodeRepositoryFake()
    session_repository: Any = SessionRepositoryFake(session)
    service: Any = SessionNodeService(node_repository, session_repository)
    with pytest.raises(ValidationError, match="no longer accepts"):
        await service.ingest_nodes(
            session.id,
            [
                SessionNodeUpsert(
                    index=0,
                    node_type=NodeType.SPAN,
                    name="span",
                    status=NodeStatus.COMPLETED,
                )
            ],
            actor(),
        )
