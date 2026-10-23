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
"""In-memory fake for session nodes."""

import uuid
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from kitaru.api_models.v1.session import SessionOrigin
from kitaru.api_models.v1.session_node import NodeStatus
from kitaru.server.application.models.session_node import SessionNodeFilter
from kitaru.server.application.pagination import decode_cursor, encode_cursor
from kitaru.server.base import ListFilter
from kitaru.server.domain.session_node import SessionNode
from kitaru_test_support.common import (
    ListItemT,
    _evaluate_filter_expression,
    _renewed_timestamp,
)

# Named only in string annotations; a runtime import could be circular.
if TYPE_CHECKING:
    from kitaru_test_support.cohorts_workers import FakeCohortVersionRepository
    from kitaru_test_support.sessions import FakeSessionRepository


def _paginate_fake_by_index(
    items: list[ListItemT],
    list_filter: ListFilter,
    index: Callable[[ListItemT], int],
) -> tuple[list[ListItemT], str | None]:
    """Apply index-ascending cursor pagination to an in-memory list.

    Args:
        items: Candidate domain objects, already scoped by the caller.
        list_filter: Filter carrying the cursor and size.
        index: Item index accessor.

    Returns:
        Page of matching items and the next cursor.
    """
    filter_hash = list_filter.compute_filter_hash()
    cursor = None
    if list_filter.cursor is not None:
        cursor = decode_cursor(list_filter.cursor, list_filter.sort, filter_hash)

    ordered = sorted(items, key=index)
    if cursor is not None:
        last_index = int(cursor.id)
        ordered = [item for item in ordered if index(item) > last_index]

    page = ordered[: list_filter.size + 1]
    next_cursor = None
    if len(page) > list_filter.size:
        page = page[: list_filter.size]
        next_cursor = encode_cursor(list_filter.sort, str(index(page[-1])), filter_hash)
    return page, next_cursor


def build_session_node(
    session_id: uuid.UUID, external_id: str, **overrides: Any
) -> SessionNode:
    """Build a session node identified by its session and external id.

    Args:
        session_id: Id of the owning session.
        external_id: Id from the source system.
        **overrides: Additional node fields.

    Returns:
        Session node.
    """
    values: dict[str, Any] = {
        "session_id": session_id,
        "external_id": external_id,
    }
    values.update(overrides)
    return SessionNode(**values)


def _start_sort_key(
    started_at: datetime | None, row_id: uuid.UUID
) -> tuple[bool, datetime, uuid.UUID]:
    """Build the sort key placing a node without a start time last.

    Args:
        started_at: Start time of the node, if any.
        row_id: Id of the node.

    Returns:
        Untimed marker, start time, and id.
    """
    if started_at is None:
        return True, datetime.min.replace(tzinfo=UTC), row_id
    return False, started_at, row_id


def _node_position_key(node: SessionNode) -> tuple[bool, datetime, uuid.UUID]:
    """Build the sort key a session node is positioned by.

    Args:
        node: Session node to position.

    Returns:
        Untimed marker, start time, and id of the node.
    """
    return _start_sort_key(node.started_at, node.id)


def _paginate_fake_by_start(
    items: list[SessionNode], list_filter: ListFilter
) -> tuple[list[SessionNode], str | None]:
    """Apply start-ascending cursor pagination to an in-memory list of nodes.

    Args:
        items: Candidate nodes, already scoped by the caller.
        list_filter: Filter carrying the cursor and size.

    Returns:
        Page of matching nodes and the next cursor.
    """
    filter_hash = list_filter.compute_filter_hash()
    cursor = None
    if list_filter.cursor is not None:
        cursor = decode_cursor(list_filter.cursor, list_filter.sort, filter_hash)

    ordered = sorted(items, key=_node_position_key)
    if cursor is not None:
        started_at, _, row_id = cursor.id.rpartition("|")
        last = _start_sort_key(
            datetime.fromisoformat(started_at) if started_at else None,
            uuid.UUID(row_id),
        )
        ordered = [item for item in ordered if _node_position_key(item) > last]

    page = ordered[: list_filter.size + 1]
    next_cursor = None
    if len(page) > list_filter.size:
        page = page[: list_filter.size]
        last_node = page[-1]
        last_started_at = (
            last_node.started_at.isoformat() if last_node.started_at is not None else ""
        )
        next_cursor = encode_cursor(
            list_filter.sort,
            f"{last_started_at}|{last_node.id}",
            filter_hash,
        )
    return page, next_cursor


class FakeSessionNodeRepository:
    """In-memory session node repository."""

    def __init__(
        self,
        sessions: "FakeSessionRepository | None" = None,
        cohort_versions: "FakeCohortVersionRepository | None" = None,
    ) -> None:
        """Initialize the repository.

        Args:
            sessions: Fake session repository, consulted by the agent-scope
                history search.
            cohort_versions: Fake cohort version repository, consulted by
                the cohort-version-scope history search.
        """
        self._nodes: dict[uuid.UUID, SessionNode] = {}
        self._sessions = sessions
        self._cohort_versions = cohort_versions

    async def get_by_external_ids(
        self,
        session_id: uuid.UUID,
        external_ids: Sequence[str],
        include_payloads: bool,
    ) -> dict[str, SessionNode]:
        """Bulk-load the stored nodes of a session under the given external ids.

        Args:
            session_id: Id of the owning session.
            external_ids: External ids to load.
            include_payloads: Whether to read the inputs, outputs, and
                attributes.

        Returns:
            Stored nodes keyed by external id, missing ids omitted.
        """
        wanted = set(external_ids)
        matches = [
            node
            for node in self._nodes.values()
            if node.session_id == session_id and node.external_id in wanted
        ]
        if include_payloads:
            return {node.external_id: node.model_copy() for node in matches}
        return {
            node.external_id: node.model_copy(
                update={"inputs": None, "outputs": None, "attributes": None}
            )
            for node in matches
        }

    async def upsert_batch(
        self, session_id: uuid.UUID, nodes: list[SessionNode]
    ) -> list[SessionNode]:
        """Insert or replace nodes upserted on (session, external id).

        Args:
            session_id: Id of the owning session.
            nodes: Fully resolved nodes to store, in batch order.

        Returns:
            Stored nodes in batch order, without payloads.
        """
        stored: list[SessionNode] = []
        for node in nodes:
            existing = self._nodes.get(node.id)
            now = datetime.now(UTC)
            created = existing.created if existing is not None else now
            updated = (
                _renewed_timestamp(existing.updated) if existing is not None else now
            )
            row = node.model_copy(update={"created": created, "updated": updated})
            self._nodes[node.id] = row
            stored.append(
                row.model_copy(
                    update={
                        "inputs": None,
                        "outputs": None,
                        "attributes": None,
                    }
                )
            )
        return stored

    async def query(
        self, session_node_filter: SessionNodeFilter
    ) -> tuple[list[SessionNode], str | None]:
        """Query the nodes of a session, ordered by position ascending.

        Args:
            session_node_filter: Filter and pagination parameters.

        Returns:
            Page of matching nodes and the next cursor.
        """
        nodes = [
            node
            for node in self._nodes.values()
            if node.session_id == session_node_filter.session_id
            and (
                session_node_filter.expression is None
                or _evaluate_filter_expression(node, session_node_filter.expression)
            )
        ]
        page, next_cursor = _paginate_fake_by_start(nodes, session_node_filter)
        result = []
        for node in page:
            if session_node_filter.include_payloads:
                result.append(node.model_copy())
            else:
                result.append(
                    node.model_copy(
                        update={"inputs": None, "outputs": None, "attributes": None}
                    )
                )
        return result, next_cursor

    async def list_all(
        self, session_id: uuid.UUID, include_payloads: bool
    ) -> list[SessionNode]:
        """Read every node of a session, ordered by position ascending.

        Args:
            session_id: Id of the owning session.
            include_payloads: Whether to read the inputs, outputs, and
                attributes.

        Returns:
            Every node of the session.
        """
        nodes = [node for node in self._nodes.values() if node.session_id == session_id]
        ordered = sorted(nodes, key=_node_position_key)
        if include_payloads:
            return [node.model_copy() for node in ordered]
        return [
            node.model_copy(
                update={"inputs": None, "outputs": None, "attributes": None}
            )
            for node in ordered
        ]

    async def exists_in_session(
        self, session_id: uuid.UUID, node_id: uuid.UUID
    ) -> bool:
        """Report whether a node belongs to a session.

        Args:
            session_id: Id of the owning session.
            node_id: Id of the node.

        Returns:
            Whether the node belongs to the session.
        """
        node = self._nodes.get(node_id)
        return node is not None and node.session_id == session_id

    def _newest_match(self, candidates: list[SessionNode]) -> SessionNode | None:
        """Pick the last node in position order from a candidate list.

        Args:
            candidates: Matching nodes.

        Returns:
            Last node in position order, or ``None`` when the list is empty.
        """
        if not candidates:
            return None
        return max(candidates, key=_node_position_key).model_copy()

    async def find_latest_by_cache_key_in_session(
        self, session_id: uuid.UUID, cache_key: str
    ) -> SessionNode | None:
        """Find the newest completed node with a cache key within one session.

        Args:
            session_id: Id of the session to search.
            cache_key: Tool call cache key to match.

        Returns:
            Last matching node in position order, or ``None`` on a miss.
        """
        return self._newest_match(
            [
                node
                for node in self._nodes.values()
                if node.session_id == session_id
                and node.cache_key == cache_key
                and node.status == NodeStatus.COMPLETED
            ]
        )

    async def find_nth_by_cache_key_in_session(
        self, session_id: uuid.UUID, cache_key: str, occurrence: int
    ) -> SessionNode | None:
        """Find the nth finished node of a session with a cache key, in position order.

        Only completed and failed tool calls are candidates, so the
        occurrence offset counts finished calls only.

        Args:
            session_id: Id of the session to search.
            cache_key: Tool call cache key to match.
            occurrence: Zero-based match position in position order.

        Returns:
            Matching node at the position, or ``None`` on a miss.
        """
        matches = sorted(
            (
                node
                for node in self._nodes.values()
                if node.session_id == session_id
                and node.cache_key == cache_key
                and node.status in (NodeStatus.COMPLETED, NodeStatus.FAILED)
            ),
            key=_node_position_key,
        )
        if occurrence >= len(matches):
            return None
        return matches[occurrence].model_copy()

    async def find_latest_by_cache_key_in_agent(
        self, agent_id: uuid.UUID, cache_key: str
    ) -> SessionNode | None:
        """Find the newest completed node with a cache key in an agent's history.

        Only sessions with a recorded or imported origin are searched, so a
        replay's own result session is never a match.

        Args:
            agent_id: Id of the agent to search.
            cache_key: Tool call cache key to match.

        Returns:
            Last matching node in position order, or ``None`` on a miss.
        """
        assert self._sessions is not None
        session_ids = {
            session.id
            for session in self._sessions._sessions.values()
            if session.agent_id == agent_id
            and session.origin in (SessionOrigin.RECORDED, SessionOrigin.IMPORTED)
        }
        return self._newest_match(
            [
                node
                for node in self._nodes.values()
                if node.session_id in session_ids
                and node.cache_key == cache_key
                and node.status == NodeStatus.COMPLETED
            ]
        )

    async def find_latest_by_cache_key_in_cohort_version(
        self, cohort_version_id: uuid.UUID, cache_key: str
    ) -> SessionNode | None:
        """Find the newest completed node with a cache key in a cohort version.

        Args:
            cohort_version_id: Id of the cohort version to search.
            cache_key: Tool call cache key to match.

        Returns:
            Last matching node in position order, or ``None`` on a miss.
        """
        assert self._cohort_versions is not None
        session_ids = set(self._cohort_versions._members.get(cohort_version_id, []))
        return self._newest_match(
            [
                node
                for node in self._nodes.values()
                if node.session_id in session_ids
                and node.cache_key == cache_key
                and node.status == NodeStatus.COMPLETED
            ]
        )
