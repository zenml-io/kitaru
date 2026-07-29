#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
"""Session-node ingest and listing use cases."""

import uuid
from decimal import Decimal

from kitaru.cache_keys import compute_tool_cache_key
from kitaru.server.application.interfaces.session_repository import (
    SessionNodeRepository,
    SessionRepository,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.session_node import SessionNodeUpsert
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.ids import uuid7
from kitaru.server.domain.session import SessionRollups
from kitaru.server.domain.session_node import NodeType, SessionNode


def _rollups(node: SessionNode, sign: int = 1) -> SessionRollups:
    """Build one node's signed aggregate contribution."""
    tokens = node.tokens
    return SessionRollups(
        cost=Decimal(sign) * (node.cost or Decimal(0)),
        input_tokens=sign * ((tokens.input_tokens if tokens else None) or 0),
        output_tokens=sign * ((tokens.output_tokens if tokens else None) or 0),
        cached_input_tokens=sign
        * ((tokens.cached_input_tokens if tokens else None) or 0),
        reasoning_tokens=sign * ((tokens.reasoning_tokens if tokens else None) or 0),
        llm_call_count=sign if node.node_type is NodeType.LLM_CALL else 0,
        tool_call_count=sign if node.node_type is NodeType.TOOL_CALL else 0,
    )


def _add_rollups(left: SessionRollups, right: SessionRollups) -> SessionRollups:
    """Add two immutable rollup deltas."""
    return SessionRollups(
        **{
            field: getattr(left, field) + getattr(right, field)
            for field in SessionRollups.model_fields
        }
    )


class SessionNodeService:
    """Session-node ingest and listing use cases."""

    def __init__(
        self,
        repository: SessionNodeRepository,
        session_repository: SessionRepository,
    ) -> None:
        self._repository = repository
        self._session_repository = session_repository

    async def ingest_nodes(
        self,
        session_id: uuid.UUID,
        upserts: list[SessionNodeUpsert],
        actor: AuthContext,
    ) -> tuple[list[SessionNode], dict[uuid.UUID, int]]:
        """Upsert a parent-first batch and apply aggregate deltas."""
        _ = actor
        if not upserts:
            return [], {}
        session = await self._session_repository.get(session_id)
        session.check_node_ingest()
        indexes = [item.index for item in upserts]
        if len(set(indexes)) != len(indexes):
            raise ValidationError("Session node indexes must be unique in a batch")
        parent_indexes = {
            parent
            for item in upserts
            for parent in [item.parent_index, *item.secondary_parent_indexes]
            if parent is not None
        }
        existing = await self._repository.get_by_indexes(
            session_id, list(set(indexes) | parent_indexes)
        )
        resolved = dict(existing)
        nodes: list[SessionNode] = []
        delta = SessionRollups()
        for item in upserts:
            referenced = [
                parent
                for parent in [item.parent_index, *item.secondary_parent_indexes]
                if parent is not None
            ]
            if any(parent >= item.index for parent in referenced):
                raise ValidationError("Parent indexes must precede their child")
            missing = [parent for parent in referenced if parent not in resolved]
            if missing:
                raise ValidationError(f"Unknown session node parent index {missing[0]}")
            old = existing.get(item.index)
            cache_key = None
            if item.node_type is NodeType.TOOL_CALL and item.tool_name is not None:
                cache_key = compute_tool_cache_key(item.tool_name, item.inputs)
            node = SessionNode(
                id=old.id if old is not None else uuid7(),
                session_id=session_id,
                parent_id=(
                    resolved[item.parent_index].id
                    if item.parent_index is not None
                    else None
                ),
                secondary_parent_ids=[
                    resolved[parent].id for parent in item.secondary_parent_indexes
                ],
                cache_key=cache_key,
                **item.model_dump(exclude={"parent_index", "secondary_parent_indexes"}),
            )
            if old is not None:
                delta = _add_rollups(delta, _rollups(old, -1))
            delta = _add_rollups(delta, _rollups(node))
            nodes.append(node)
            resolved[item.index] = node
        stored = await self._repository.upsert_many(nodes)
        await self._session_repository.apply_rollups(session_id, delta)
        index_by_id = {node.id: node.index for node in resolved.values()}
        index_by_id.update({node.id: node.index for node in stored})
        return stored, index_by_id

    async def list_nodes(
        self,
        session_id: uuid.UUID,
        cursor: str | None,
        size: int,
        include_payloads: bool,
        actor: AuthContext,
    ) -> tuple[list[SessionNode], str | None, dict[uuid.UUID, int]]:
        """List session nodes ordered by index."""
        _ = actor
        await self._session_repository.get(session_id)
        nodes, cursor = await self._repository.list_nodes(
            session_id, cursor, size, include_payloads
        )
        index_by_id = {node.id: node.index for node in nodes}
        parent_ids = {
            parent_id
            for node in nodes
            for parent_id in [node.parent_id, *node.secondary_parent_ids]
            if parent_id is not None
        }
        index_by_id.update(
            await self._repository.get_indexes_by_ids(
                session_id, list(parent_ids - index_by_id.keys())
            )
        )
        return nodes, cursor, index_by_id
