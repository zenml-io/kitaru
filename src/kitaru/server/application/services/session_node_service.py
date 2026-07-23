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
"""Session node use cases."""

import uuid

from kitaru.hashing import tool_call_cache_key
from kitaru.server.application.interfaces.session_node_repository import (
    SessionNodeRepository,
)
from kitaru.server.application.interfaces.session_repository import (
    SessionRepository,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.session_nodes import SessionNodeUpsert
from kitaru.server.domain.session_node import (
    NodeType,
    SessionNode,
    UnknownParentNode,
    build_node_key,
)

# Sibling record used for key computation: parent id, type, name, sequence.
_NodeRecord = tuple[uuid.UUID | None, NodeType, str, int]


class SessionNodeService:
    """Session node use cases."""

    def __init__(
        self,
        repository: SessionNodeRepository,
        session_repository: SessionRepository,
    ) -> None:
        """Initialize the service.

        Args:
            repository: Session node repository.
            session_repository: Session repository.
        """
        self._repository = repository
        self._session_repository = session_repository

    def _build_nodes(
        self,
        session_id: uuid.UUID,
        existing: list[SessionNode],
        upserts: list[SessionNodeUpsert],
    ) -> list[SessionNode]:
        """Build domain nodes with computed keys and cache keys.

        Args:
            session_id: Id of the session.
            existing: Stored nodes of the session.
            upserts: Upsert commands in batch order.

        Raises:
            UnknownParentNode: A referenced parent is neither stored nor
                earlier in the batch.

        Returns:
            Nodes in batch order.
        """
        keys: dict[uuid.UUID, str] = {node.id: node.key for node in existing}
        records: dict[uuid.UUID, _NodeRecord] = {
            node.id: (node.parent_id, node.node_type, node.name, node.sequence)
            for node in existing
        }
        nodes: list[SessionNode] = []
        for upsert in upserts:
            parent_key = None
            if upsert.parent_id is not None:
                if upsert.parent_id == upsert.id or upsert.parent_id not in keys:
                    raise UnknownParentNode(upsert.parent_id)
                parent_key = keys[upsert.parent_id]
            for secondary_parent_id in upsert.secondary_parent_ids:
                if secondary_parent_id == upsert.id or secondary_parent_id not in keys:
                    raise UnknownParentNode(secondary_parent_id)
            occurrence = 1 + sum(
                1
                for node_id, (parent_id, node_type, name, sequence) in records.items()
                if node_id != upsert.id
                and parent_id == upsert.parent_id
                and node_type is upsert.node_type
                and name == upsert.name
                and sequence < upsert.sequence
            )
            key = build_node_key(parent_key, upsert.node_type, upsert.name, occurrence)
            cache_key = None
            if upsert.node_type is NodeType.TOOL_CALL:
                cache_key = tool_call_cache_key(upsert.tool_name, upsert.inputs)
            keys[upsert.id] = key
            records[upsert.id] = (
                upsert.parent_id,
                upsert.node_type,
                upsert.name,
                upsert.sequence,
            )
            nodes.append(
                SessionNode(
                    id=upsert.id,
                    session_id=session_id,
                    key=key,
                    parent_id=upsert.parent_id,
                    secondary_parent_ids=upsert.secondary_parent_ids,
                    sequence=upsert.sequence,
                    external_id=upsert.external_id,
                    trace_id=upsert.trace_id,
                    node_type=upsert.node_type,
                    name=upsert.name,
                    status=upsert.status,
                    error=upsert.error,
                    started_at=upsert.started_at,
                    ended_at=upsert.ended_at,
                    inputs=upsert.inputs,
                    outputs=upsert.outputs,
                    requested_model=upsert.requested_model,
                    model=upsert.model,
                    provider=upsert.provider,
                    tokens=upsert.tokens,
                    cost=upsert.cost,
                    model_params=upsert.model_params,
                    tool_name=upsert.tool_name,
                    cache_key=cache_key,
                    subagent_id=upsert.subagent_id,
                    attributes=upsert.attributes,
                    metadata=upsert.metadata,
                )
            )
        return nodes

    async def ingest_nodes(
        self,
        session_id: uuid.UUID,
        upserts: list[SessionNodeUpsert],
        actor: AuthContext,
    ) -> list[SessionNode]:
        """Upsert a batch of nodes into a session.

        The server computes each node's path key from its parent and
        sequence, and the cache key of tool call nodes from the tool name
        and inputs. Retries are idempotent, nodes upsert on their
        client-generated id.

        Args:
            session_id: Id of the session.
            upserts: Upsert commands in parent-before-child order.
            actor: Caller context.

        Raises:
            SessionNotFound: No session has this id.
            SessionNotInProgress: The session does not accept node ingest.
            UnknownParentNode: A referenced parent is neither stored nor
                earlier in the batch.
            DuplicateNodeSequence: A node sequence is already registered in
                the session.
            DuplicateNodeExternalId: A node external id is already
                registered in the session.

        Returns:
            Stored nodes in batch order.
        """
        _ = actor
        session = await self._session_repository.get(session_id)
        session.check_node_ingest()
        existing = await self._repository.list_for_session(
            session_id, include_payloads=False
        )
        nodes = self._build_nodes(session_id, existing, upserts)
        return await self._repository.upsert(nodes)

    async def list_nodes(
        self,
        session_id: uuid.UUID,
        include_payloads: bool,
        actor: AuthContext,
    ) -> list[SessionNode]:
        """List the nodes of a session ordered by sequence.

        Args:
            session_id: Id of the session.
            include_payloads: Whether to load inputs, outputs, and
                attributes.
            actor: Caller context.

        Raises:
            SessionNotFound: No session has this id.

        Returns:
            Nodes ordered by sequence.
        """
        _ = actor
        await self._session_repository.get(session_id)
        return await self._repository.list_for_session(session_id, include_payloads)
