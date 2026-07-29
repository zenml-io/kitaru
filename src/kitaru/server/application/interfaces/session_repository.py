"""Session persistence interfaces."""

import uuid
from typing import Protocol

from kitaru.server.application.models.session import SessionFilter
from kitaru.server.domain.session import Session, SessionRollups
from kitaru.server.domain.session_node import SessionNode


class SessionRepository(Protocol):
    """Session persistence operations."""

    async def create(self, session: Session) -> Session: ...
    async def get(self, session_id: uuid.UUID) -> Session: ...
    async def get_many(self, ids: list[uuid.UUID]) -> dict[uuid.UUID, Session]: ...
    async def get_by_external(
        self, provider: str | None, external_id: str
    ) -> Session | None: ...
    async def query(
        self, session_filter: SessionFilter
    ) -> tuple[list[Session], str | None]: ...
    async def update(self, session: Session) -> Session: ...
    async def delete(self, session_id: uuid.UUID) -> None: ...
    async def unlink_task(self, task_id: uuid.UUID) -> None: ...
    async def apply_rollups(
        self, session_id: uuid.UUID, rollups: SessionRollups
    ) -> Session: ...


class SessionNodeRepository(Protocol):
    """Session-node persistence operations."""

    async def get_by_indexes(
        self, session_id: uuid.UUID, indexes: list[int]
    ) -> dict[int, SessionNode]: ...
    async def get_indexes_by_ids(
        self, session_id: uuid.UUID, ids: list[uuid.UUID]
    ) -> dict[uuid.UUID, int]: ...
    async def upsert_many(self, nodes: list[SessionNode]) -> list[SessionNode]: ...
    async def list_nodes(
        self,
        session_id: uuid.UUID,
        cursor: str | None,
        size: int,
        include_payloads: bool,
    ) -> tuple[list[SessionNode], str | None]: ...
    async def find_tool_result(
        self,
        cache_key: str,
        *,
        session_ids: list[uuid.UUID] | None = None,
        agent_id: uuid.UUID | None = None,
        cohort_id: uuid.UUID | None = None,
    ) -> SessionNode | None: ...
