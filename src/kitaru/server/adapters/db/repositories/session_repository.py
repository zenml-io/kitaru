"""SQL session repositories."""

import hashlib
import uuid

from sqlalchemy import exists, func, select, update

from kitaru.server.adapters.db.orm.cohort import CohortSessionORM
from kitaru.server.adapters.db.orm.evaluation import EvaluationORM
from kitaru.server.adapters.db.orm.session import (
    SESSION_EXTERNAL_UNIQUE_CONSTRAINT,
    SessionORM,
)
from kitaru.server.adapters.db.orm.session_node import SessionNodeORM
from kitaru.server.adapters.db.orm.tag import TagLinkORM, TagORM
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.application.models.session import SessionFilter
from kitaru.server.application.pagination import decode_cursor, encode_cursor
from kitaru.server.domain.base import NotFoundError
from kitaru.server.domain.session import (
    DuplicateExternalSession,
    Session,
    SessionNotFound,
    SessionRollups,
)
from kitaru.server.domain.session_node import SessionNode


class SQLSessionRepository(BaseSQLRepository[SessionORM]):
    """Session repository backed by PostgreSQL."""

    orm_class = SessionORM

    def _not_found(self, entity_id: uuid.UUID) -> NotFoundError:
        return SessionNotFound(entity_id)

    async def create(self, session: Session) -> Session:
        row = SessionORM.from_domain(session)
        await self._add(
            row,
            {
                SESSION_EXTERNAL_UNIQUE_CONSTRAINT: lambda: DuplicateExternalSession(
                    session.provider, session.external_id or ""
                )
            },
        )
        return row.to_domain()

    async def get(self, session_id: uuid.UUID) -> Session:
        return (await self._get_row(session_id)).to_domain()

    async def get_many(self, ids: list[uuid.UUID]) -> dict[uuid.UUID, Session]:
        rows = await self._load_by_ids(ids)
        return {row_id: row.to_domain() for row_id, row in rows.items()}

    async def get_by_external(
        self, provider: str | None, external_id: str
    ) -> Session | None:
        row = (
            await self._session.scalars(
                select(SessionORM).where(
                    SessionORM.provider == provider,
                    SessionORM.external_id == external_id,
                )
            )
        ).one_or_none()
        return row.to_domain() if row is not None else None

    async def query(
        self, session_filter: SessionFilter
    ) -> tuple[list[Session], str | None]:
        statement = select(SessionORM)
        fields = {
            "agent_id": SessionORM.agent_id,
            "agent_version_id": SessionORM.agent_version_id,
            "task_id": SessionORM.task_id,
            "provider": SessionORM.provider,
            "external_id": SessionORM.external_id,
            "name": SessionORM.name,
        }
        for name, column in fields.items():
            value = getattr(session_filter, name)
            if value is not None:
                statement = statement.where(column == value)
        if session_filter.origin is not None:
            statement = statement.where(
                SessionORM.origin == session_filter.origin.value
            )
        if session_filter.status is not None:
            statement = statement.where(
                SessionORM.status == session_filter.status.value
            )
        for name, column, operator in (
            ("started_after", SessionORM.started_at, "ge"),
            ("started_before", SessionORM.started_at, "le"),
            ("ended_after", SessionORM.ended_at, "ge"),
            ("ended_before", SessionORM.ended_at, "le"),
            ("min_cost", SessionORM.cost, "ge"),
            ("max_cost", SessionORM.cost, "le"),
        ):
            value = getattr(session_filter, name)
            if value is not None:
                statement = statement.where(
                    column >= value if operator == "ge" else column <= value
                )
        if session_filter.has_evaluation is not None:
            predicate = exists().where(EvaluationORM.session_id == SessionORM.id)
            statement = statement.where(
                predicate if session_filter.has_evaluation else ~predicate
            )
        if session_filter.tag is not None:
            statement = (
                statement.join(
                    TagLinkORM,
                    (TagLinkORM.resource_id == SessionORM.id)
                    & (TagLinkORM.resource_type == "session"),
                )
                .join(TagORM, TagORM.id == TagLinkORM.tag_id)
                .where(TagORM.name == session_filter.tag)
            )
        rows, cursor = await paginate(
            self._session, statement, session_filter, SessionORM.id
        )
        return [row.to_domain() for row in rows], cursor

    async def update(self, session: Session) -> Session:
        row = await self._get_row(session.id)
        row.copy_from_domain(session)
        await self._session.flush()
        return row.to_domain()

    async def delete(self, session_id: uuid.UUID) -> None:
        await self._delete_row(session_id)

    async def unlink_task(self, task_id: uuid.UUID) -> None:
        await self._session.execute(
            update(SessionORM).where(SessionORM.task_id == task_id).values(task_id=None)
        )

    async def apply_rollups(
        self, session_id: uuid.UUID, rollups: SessionRollups
    ) -> Session:
        await self._get_row(session_id)
        values = {}
        for name, column, delta in (
            ("cost", SessionORM.cost, rollups.cost),
            ("input_tokens", SessionORM.input_tokens, rollups.input_tokens),
            ("output_tokens", SessionORM.output_tokens, rollups.output_tokens),
            (
                "cached_input_tokens",
                SessionORM.cached_input_tokens,
                rollups.cached_input_tokens,
            ),
            (
                "reasoning_tokens",
                SessionORM.reasoning_tokens,
                rollups.reasoning_tokens,
            ),
            (
                "llm_call_count",
                SessionORM.llm_call_count,
                rollups.llm_call_count,
            ),
            (
                "tool_call_count",
                SessionORM.tool_call_count,
                rollups.tool_call_count,
            ),
        ):
            if delta:
                values[name] = func.coalesce(column, 0) + delta
        if values:
            await self._session.execute(
                update(SessionORM).where(SessionORM.id == session_id).values(**values)
            )
            await self._session.flush()
        return (await self._get_row(session_id)).to_domain()


class SQLSessionNodeRepository(BaseSQLRepository[SessionNodeORM]):
    """Session-node repository backed by PostgreSQL."""

    orm_class = SessionNodeORM

    def _not_found(self, entity_id: uuid.UUID) -> NotFoundError:
        return SessionNotFound(entity_id)

    async def get_by_indexes(
        self, session_id: uuid.UUID, indexes: list[int]
    ) -> dict[int, SessionNode]:
        if not indexes:
            return {}
        rows = (
            await self._session.scalars(
                select(SessionNodeORM)
                .where(
                    SessionNodeORM.session_id == session_id,
                    SessionNodeORM.index.in_(indexes),
                )
                .with_for_update()
            )
        ).all()
        return {row.index: row.to_domain() for row in rows}

    async def get_indexes_by_ids(
        self, session_id: uuid.UUID, ids: list[uuid.UUID]
    ) -> dict[uuid.UUID, int]:
        if not ids:
            return {}
        rows = (
            await self._session.execute(
                select(SessionNodeORM.id, SessionNodeORM.index).where(
                    SessionNodeORM.session_id == session_id,
                    SessionNodeORM.id.in_(ids),
                )
            )
        ).all()
        return {node_id: index for node_id, index in rows}

    async def upsert_many(self, nodes: list[SessionNode]) -> list[SessionNode]:
        if not nodes:
            return []
        existing_rows = {
            row.index: row
            for row in (
                await self._session.scalars(
                    select(SessionNodeORM)
                    .where(
                        SessionNodeORM.session_id == nodes[0].session_id,
                        SessionNodeORM.index.in_([node.index for node in nodes]),
                    )
                    .with_for_update()
                )
            ).all()
        }
        stored: list[SessionNodeORM] = []
        for node in nodes:
            source = SessionNodeORM.from_domain(node)
            row = existing_rows.get(node.index)
            if row is None:
                self._session.add(source)
                row = source
            else:
                for column in SessionNodeORM.__table__.columns:
                    if column.name in {"id", "created", "updated"}:
                        continue
                    attribute = (
                        "metadata_" if column.name == "metadata" else column.name
                    )
                    setattr(row, attribute, getattr(source, attribute))
            stored.append(row)
        await self._session.flush()
        return [row.to_domain() for row in stored]

    async def list_nodes(
        self,
        session_id: uuid.UUID,
        cursor: str | None,
        size: int,
        include_payloads: bool,
    ) -> tuple[list[SessionNode], str | None]:
        filter_hash = hashlib.sha256(str(session_id).encode()).hexdigest()[:16]
        last_index = None
        if cursor is not None:
            last_index = int(decode_cursor(cursor, "index:asc", filter_hash).id)
        statement = (
            select(SessionNodeORM)
            .where(SessionNodeORM.session_id == session_id)
            .order_by(SessionNodeORM.index)
            .limit(size + 1)
        )
        if last_index is not None:
            statement = statement.where(SessionNodeORM.index > last_index)
        rows = list((await self._session.scalars(statement)).all())
        next_cursor = None
        if len(rows) > size:
            rows = rows[:size]
            next_cursor = encode_cursor("index:asc", str(rows[-1].index), filter_hash)
        return [
            row.to_domain(include_payloads=include_payloads) for row in rows
        ], next_cursor

    async def find_tool_result(
        self,
        cache_key: str,
        *,
        session_ids: list[uuid.UUID] | None = None,
        agent_id: uuid.UUID | None = None,
        cohort_id: uuid.UUID | None = None,
    ) -> SessionNode | None:
        scope_count = sum(
            value is not None for value in (session_ids, agent_id, cohort_id)
        )
        if scope_count != 1:
            raise ValueError("Exactly one tool-history scope is required")
        if session_ids == []:
            return None
        statement = select(SessionNodeORM).where(
            SessionNodeORM.cache_key == cache_key,
            SessionNodeORM.node_type == "tool_call",
        )
        if session_ids is not None:
            statement = statement.where(SessionNodeORM.session_id.in_(session_ids))
        elif agent_id is not None:
            statement = statement.join(
                SessionORM, SessionORM.id == SessionNodeORM.session_id
            ).where(SessionORM.agent_id == agent_id)
        else:
            assert cohort_id is not None
            statement = statement.join(
                CohortSessionORM,
                CohortSessionORM.session_id == SessionNodeORM.session_id,
            ).where(CohortSessionORM.cohort_id == cohort_id)
        row = (
            await self._session.scalars(
                statement.order_by(SessionNodeORM.id.desc()).limit(1)
            )
        ).one_or_none()
        return row.to_domain() if row is not None else None
