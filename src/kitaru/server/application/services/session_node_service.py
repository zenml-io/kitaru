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

from kitaru.api_models.v1.session_node import NodeType
from kitaru.cache_keys import compute_tool_cache_key
from kitaru.server.application.interfaces.session_node_repository import (
    SessionNodeRepository,
)
from kitaru.server.application.interfaces.session_repository import (
    SessionRepository,
)
from kitaru.server.application.interfaces.task_repository import TaskRepository
from kitaru.server.application.models.auth import AuthContext, TaskPrincipal
from kitaru.server.application.models.session_node import (
    SessionNodeFilter,
    SessionNodeUpsert,
)
from kitaru.server.application.services.resource_access import (
    check_task_attempt,
    check_task_session_read,
    check_task_session_write,
)
from kitaru.server.domain.ids import uuid7
from kitaru.server.domain.session import combine_rollups, rollup_delta
from kitaru.server.domain.session_node import (
    SessionNode,
    SessionNodeParentNotFound,
    node_rollup_contribution,
)


class SessionNodeService:
    """Session node use cases."""

    def __init__(
        self,
        repository: SessionNodeRepository,
        session_repository: SessionRepository,
        task_repository: TaskRepository,
    ) -> None:
        """Initialize the service.

        Args:
            repository: Session node repository.
            session_repository: Session repository, for the ingest gate and
                the rollup update.
            task_repository: Task repository, for the attempt fence.
        """
        self._repository = repository
        self._sessions = session_repository
        self._tasks = task_repository

    async def ingest_nodes(
        self,
        session_id: uuid.UUID,
        batch: list[SessionNodeUpsert],
        actor: AuthContext,
    ) -> list[SessionNode]:
        """Upsert a batch of nodes on (session, index).

        An index already stored is replaced whole. ``parent_index`` and
        ``secondary_parent_indexes`` resolve against both stored rows and
        earlier rows in the same batch. The session's cost, tokens, and call
        counts roll up by one atomic delta-based update covering the whole
        batch. A task principal ingests only into a session it owns.

        Args:
            session_id: Id of the session to ingest into.
            batch: Nodes to upsert, parent before child.
            actor: Caller context.

        Raises:
            SessionNotFound: No session has this id.
            SessionAccessDenied: A task principal does not own the session.
            SessionNotIngestable: The session is not in progress and its
                origin is not imported.
            SessionNodeParentNotFound: A parent_index or secondary parent
                index does not match a stored or batched node.

        Returns:
            Stored nodes in batch order.
        """
        # Node ids are minted for indexes this read does not find, so two
        # concurrent batches for one index would both insert and collide on
        # the (session, index) key. The lock also stabilizes the pre-image
        # the rollup deltas are computed against.
        session = await self._sessions.get(session_id, exclusive=True)
        check_task_session_write(session_id, session.task_id, actor)
        await check_task_attempt(actor, self._tasks)
        session.check_node_ingest()
        if not batch:
            return []

        # A parent_index or secondary_parent_index may point at an index
        # already stored from an earlier batch, not just at one in this
        # batch, so the bulk fetch covers every index either batch or
        # parent reference touches.
        referenced_indexes: set[int] = set()
        for item in batch:
            referenced_indexes.add(item.index)
            if item.parent_index is not None:
                referenced_indexes.add(item.parent_index)
            referenced_indexes.update(item.secondary_parent_indexes)

        existing_by_index = await self._repository.get_by_indexes(
            session_id, sorted(referenced_indexes)
        )
        id_by_index = {index: node.id for index, node in existing_by_index.items()}

        resolved: list[SessionNode] = []
        for item in batch:
            parent_id = None
            if item.parent_index is not None:
                parent_id = id_by_index.get(item.parent_index)
                if parent_id is None:
                    raise SessionNodeParentNotFound(item.index, item.parent_index)
            secondary_parent_ids: list[uuid.UUID] = []
            for secondary_index in item.secondary_parent_indexes:
                secondary_id = id_by_index.get(secondary_index)
                if secondary_id is None:
                    raise SessionNodeParentNotFound(item.index, secondary_index)
                secondary_parent_ids.append(secondary_id)

            existing_node = existing_by_index.get(item.index)
            cache_key = None
            if item.node_type == NodeType.TOOL_CALL and item.tool_name is not None:
                cache_key = compute_tool_cache_key(item.tool_name, item.inputs)

            node = SessionNode(
                id=existing_node.id if existing_node is not None else uuid7(),
                session_id=session_id,
                parent_id=parent_id,
                secondary_parent_ids=secondary_parent_ids,
                index=item.index,
                external_id=item.external_id,
                trace_id=item.trace_id,
                node_type=item.node_type,
                name=item.name,
                status=item.status,
                error=item.error,
                started_at=item.started_at,
                ended_at=item.ended_at,
                inputs=item.inputs,
                outputs=item.outputs,
                requested_model=item.requested_model,
                model=item.model,
                provider=item.provider,
                tokens=item.tokens,
                cost=item.cost,
                model_params=item.model_params,
                tool_name=item.tool_name,
                cache_key=cache_key,
                subagent_id=item.subagent_id,
                attributes=item.attributes,
                metadata=item.metadata,
            )
            resolved.append(node)
            id_by_index[item.index] = node.id

        deltas = [
            rollup_delta(
                node_rollup_contribution(existing_by_index.get(node.index)),
                node_rollup_contribution(node),
            )
            for node in resolved
        ]
        stored = await self._repository.upsert_batch(session_id, resolved)
        await self._sessions.apply_rollups(session_id, combine_rollups(deltas))
        return stored

    async def list_nodes(
        self, session_node_filter: SessionNodeFilter, actor: AuthContext
    ) -> tuple[list[SessionNode], str | None]:
        """List the nodes of a session, ordered by index ascending.

        A task principal reads only a session it owns or holds as its
        task's input session.

        Args:
            session_node_filter: Filter and pagination parameters.
            actor: Caller context.

        Raises:
            SessionNotFound: A task principal names a session that does not
                exist.
            SessionAccessDenied: A task principal owns neither the session nor
                holds it as its task's input session.

        Returns:
            Page of matching nodes and the next cursor.
        """
        if isinstance(actor.principal, TaskPrincipal):
            session = await self._sessions.get(session_node_filter.session_id)
            check_task_session_read(session.id, session.task_id, actor)
        return await self._repository.query(session_node_filter)

    async def get_index_by_id(
        self, session_id: uuid.UUID, actor: AuthContext
    ) -> dict[uuid.UUID, int]:
        """Look up the index of every node in a session, keyed by node id.

        A task principal reads only a session it owns or holds as its
        task's input session.

        Args:
            session_id: Id of the session whose nodes to look up.
            actor: Caller context.

        Raises:
            SessionNotFound: A task principal names a session that does not
                exist.
            SessionAccessDenied: A task principal owns neither the session nor
                holds it as its task's input session.

        Returns:
            Every node id in the session mapped to its index.
        """
        if isinstance(actor.principal, TaskPrincipal):
            session = await self._sessions.get(session_id)
            check_task_session_read(session_id, session.task_id, actor)
        return await self._repository.get_index_by_id(session_id)
