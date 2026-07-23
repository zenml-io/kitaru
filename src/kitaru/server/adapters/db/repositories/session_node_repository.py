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
"""SQL session node repository."""

import uuid

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import defer
from sqlmodel import col

from kitaru.server.adapters.db.errors import violated_constraint
from kitaru.server.adapters.db.schemas.session import SessionSchema
from kitaru.server.adapters.db.schemas.session_node import (
    SESSION_NODE_EXTERNAL_ID_UNIQUE_CONSTRAINT,
    SESSION_NODE_KEY_UNIQUE_CONSTRAINT,
    SESSION_NODE_SEQUENCE_UNIQUE_CONSTRAINT,
    SESSION_NODE_SESSION_ID_FOREIGN_KEY,
    SessionNodeSchema,
)
from kitaru.server.domain.session import SessionNotFound
from kitaru.server.domain.session_node import (
    DuplicateNodeExternalId,
    DuplicateNodeKey,
    DuplicateNodeSequence,
    DuplicateSessionNodeId,
    NodeStatus,
    NodeType,
    SessionNode,
)


class SQLSessionNodeRepository:
    """Session node repository backed by the application database."""

    def __init__(self, session: AsyncSession) -> None:
        """Initialize the repository.

        Args:
            session: Database session for all operations.
        """
        self._session = session

    def _apply(self, row: SessionNodeSchema, node: SessionNode) -> None:
        """Copy domain node fields onto an existing row.

        Args:
            row: Row to update.
            node: Node with modified fields.
        """
        tokens = node.tokens
        row.key = node.key
        row.parent_id = node.parent_id
        row.secondary_parent_ids = [
            str(parent_id) for parent_id in node.secondary_parent_ids
        ]
        row.sequence = node.sequence
        row.external_id = node.external_id
        row.trace_id = node.trace_id
        row.node_type = node.node_type.value
        row.name = node.name
        row.status = node.status.value
        row.error = node.error
        row.started_at = node.started_at
        row.ended_at = node.ended_at
        row.inputs = node.inputs
        row.outputs = node.outputs
        row.requested_model = node.requested_model
        row.model = node.model
        row.provider = node.provider
        row.input_tokens = tokens.input_tokens if tokens else None
        row.output_tokens = tokens.output_tokens if tokens else None
        row.cached_input_tokens = tokens.cached_input_tokens if tokens else None
        row.reasoning_tokens = tokens.reasoning_tokens if tokens else None
        row.cost = node.cost
        row.model_params = node.model_params
        row.tool_name = node.tool_name
        row.cache_key = node.cache_key
        row.subagent_id = node.subagent_id
        row.attributes = node.attributes
        row.metadata_ = node.metadata

    async def upsert(self, nodes: list[SessionNode]) -> list[SessionNode]:
        """Insert or update nodes by id as one atomic batch.

        Callers guarantee that every primary parent exists, either stored
        or earlier in the batch.

        Args:
            nodes: Nodes to store, all belonging to one session.

        Raises:
            SessionNotFound: No session has the nodes' session id.
            DuplicateSessionNodeId: A node id is already registered in
                another session.
            DuplicateNodeSequence: A node sequence is already registered in
                the session.
            DuplicateNodeExternalId: A node external id is already
                registered in the session.
            DuplicateNodeKey: A node key is already registered in the
                session.

        Returns:
            Stored nodes in batch order with timestamps set.
        """
        if not nodes:
            return []
        session_id = nodes[0].session_id
        rows: list[SessionNodeSchema] = []
        try:
            async with self._session.begin_nested():
                for node in nodes:
                    row = await self._session.get(SessionNodeSchema, node.id)
                    if row is None:
                        row = SessionNodeSchema.from_domain(node)
                        self._session.add(row)
                    else:
                        if row.session_id != node.session_id:
                            raise DuplicateSessionNodeId(node.id)
                        self._apply(row, node)
                    rows.append(row)
                await self._session.flush()
        except IntegrityError as exc:
            constraint = violated_constraint(exc)
            if constraint == SESSION_NODE_SEQUENCE_UNIQUE_CONSTRAINT:
                raise DuplicateNodeSequence(session_id) from exc
            if constraint == SESSION_NODE_EXTERNAL_ID_UNIQUE_CONSTRAINT:
                raise DuplicateNodeExternalId(session_id) from exc
            if constraint == SESSION_NODE_KEY_UNIQUE_CONSTRAINT:
                raise DuplicateNodeKey(session_id) from exc
            if constraint == SESSION_NODE_SESSION_ID_FOREIGN_KEY:
                raise SessionNotFound(session_id) from exc
            raise
        return [row.to_domain() for row in rows]

    async def list_for_session(
        self, session_id: uuid.UUID, include_payloads: bool
    ) -> list[SessionNode]:
        """Load the nodes of a session ordered by sequence.

        Args:
            session_id: Id of the session.
            include_payloads: Whether to load inputs, outputs, and
                attributes.

        Returns:
            Nodes ordered by sequence.
        """
        statement = (
            select(SessionNodeSchema)
            .where(col(SessionNodeSchema.session_id) == session_id)
            .order_by(col(SessionNodeSchema.sequence))
        )
        if not include_payloads:
            # col() returns the instrumented attribute at runtime, which
            # defer() accepts, but its declared type is Mapped.
            statement = statement.options(
                defer(col(SessionNodeSchema.inputs)),  # ty: ignore[invalid-argument-type]
                defer(col(SessionNodeSchema.outputs)),  # ty: ignore[invalid-argument-type]
                defer(col(SessionNodeSchema.attributes)),  # ty: ignore[invalid-argument-type]
            )
        rows = (await self._session.scalars(statement)).all()
        return [row.to_domain(include_payloads=include_payloads) for row in rows]

    async def find_tool_result(
        self,
        cache_key: str,
        session_ids: list[uuid.UUID] | None,
        agent_id: uuid.UUID | None,
    ) -> SessionNode | None:
        """Find the most recent completed tool call with a cache key.

        Nodes whose attributes mark them mocked are excluded. Exactly one
        of the scope arguments is set.

        Args:
            cache_key: Cache key to match.
            session_ids: Sessions to search within.
            agent_id: Agent whose sessions to search within.

        Returns:
            Most recent matching node with payloads, ``None`` on a miss.
        """
        statement = (
            select(SessionNodeSchema)
            .where(
                col(SessionNodeSchema.cache_key) == cache_key,
                col(SessionNodeSchema.node_type) == NodeType.TOOL_CALL.value,
                col(SessionNodeSchema.status) == NodeStatus.COMPLETED.value,
                func.jsonb_extract_path_text(
                    col(SessionNodeSchema.attributes), "mocked"
                ).is_distinct_from("true"),
            )
            .order_by(
                col(SessionNodeSchema.started_at).desc().nulls_last(),
                col(SessionNodeSchema.id).desc(),
            )
            .limit(1)
        )
        if session_ids is not None:
            statement = statement.where(
                col(SessionNodeSchema.session_id).in_(session_ids)
            )
        if agent_id is not None:
            statement = statement.where(
                col(SessionNodeSchema.session_id).in_(
                    select(col(SessionSchema.id)).where(
                        col(SessionSchema.agent_id) == agent_id
                    )
                )
            )
        row = (await self._session.scalars(statement)).first()
        if row is None:
            return None
        return row.to_domain(include_payloads=True)
