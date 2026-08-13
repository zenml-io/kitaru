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
"""SQL session repository."""

import uuid
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime

from sqlalchemy import ColumnElement, CursorResult, func, select, update
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.tag import TagResourceType
from kitaru.server.adapters.db.filtering import (
    FilterBinding,
    build_scope_condition_binding,
    build_tag_condition_binding,
    compile_filter_expression,
)
from kitaru.server.adapters.db.orm.agent import AgentORM
from kitaru.server.adapters.db.orm.cohort_version_session import (
    COHORT_VERSION_SESSION_SESSION_ID_FOREIGN_KEY,
    CohortVersionSessionORM,
)
from kitaru.server.adapters.db.orm.evaluation import EvaluationORM
from kitaru.server.adapters.db.orm.replay import ReplayORM
from kitaru.server.adapters.db.orm.session import (
    SESSION_IMPORTED_FROM_EXTERNAL_ID_UNIQUE_CONSTRAINT,
    SessionORM,
)
from kitaru.server.adapters.db.orm.task import (
    TASK_INPUT_SESSION_ID_FOREIGN_KEY,
    TASK_RESULT_SESSION_ID_FOREIGN_KEY,
    TaskORM,
)
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.application.models.session import SessionFilter
from kitaru.server.domain.agent import AgentNotFound
from kitaru.server.domain.base import NotFoundError
from kitaru.server.domain.session import (
    DuplicateSessionExternalId,
    Session,
    SessionInUse,
    SessionInUseByTask,
    SessionNotFound,
    SessionRollups,
)
from kitaru.server.filtering import FilterCondition

# A replay's tasks share its job, and the run owns the replay.
_scopes_to_experiment_run = build_scope_condition_binding(
    local_column=TaskORM.job_id,
    related_key=ReplayORM.job_id,
    scope_column=ReplayORM.experiment_run_id,
)


def _compile_experiment_run_condition(
    condition: FilterCondition,
) -> ColumnElement[bool]:
    """Compile an experiment run scope condition into a task predicate.

    Args:
        condition: Validated experiment run condition.

    Returns:
        SQL predicate.
    """
    # Matched through the task's result session, so a run's baseline sessions
    # stay out of the result set.
    run_tasks = (
        select(TaskORM.id)
        .where(
            TaskORM.result_session_id == SessionORM.id,
            _scopes_to_experiment_run(condition),
        )
        .correlate(SessionORM)
    )
    return run_tasks.exists()


def _compile_cohort_version_condition(
    condition: FilterCondition,
) -> ColumnElement[bool]:
    """Compile a cohort version condition into an EXISTS predicate.

    Args:
        condition: Validated cohort version condition.

    Returns:
        SQL predicate.
    """
    ids = condition.value if condition.op is FilterOp.IN else (condition.value,)
    membership_exists = (
        select(CohortVersionSessionORM.session_id)
        .where(
            CohortVersionSessionORM.session_id == SessionORM.id,
            CohortVersionSessionORM.cohort_version_id.in_(ids),
        )
        .correlate(SessionORM)
    )
    return membership_exists.exists()


def _compile_has_evaluation_condition(
    condition: FilterCondition,
) -> ColumnElement[bool]:
    """Compile a has-evaluation condition into an EXISTS predicate.

    Args:
        condition: Validated has-evaluation condition.

    Returns:
        SQL predicate.
    """
    expected = condition.value if condition.op is FilterOp.EQ else not condition.value
    evaluation_exists = (
        select(EvaluationORM.id)
        .where(EvaluationORM.session_id == SessionORM.id)
        .correlate(SessionORM)
    )
    return evaluation_exists.exists() if expected else ~evaluation_exists.exists()


SESSION_FILTER_BINDINGS: Mapping[str, FilterBinding] = {
    "id": SessionORM.id,
    "agent_id": SessionORM.agent_id,
    "agent_version_id": SessionORM.agent_version_id,
    "task_id": SessionORM.task_id,
    "origin": SessionORM.origin,
    "status": SessionORM.status,
    "imported_from": SessionORM.imported_from,
    "framework": SessionORM.framework,
    "external_id": SessionORM.external_id,
    "name": SessionORM.name,
    "tag": build_tag_condition_binding(TagResourceType.SESSION, SessionORM.id),
    "cohort_version_id": _compile_cohort_version_condition,
    "experiment_run_id": _compile_experiment_run_condition,
    "has_evaluation": _compile_has_evaluation_condition,
    "started_at": SessionORM.started_at,
    "ended_at": SessionORM.ended_at,
    "cost": SessionORM.cost,
    "llm_call_count": SessionORM.llm_call_count,
    "tool_call_count": SessionORM.tool_call_count,
    "created": SessionORM.created,
}


class SQLSessionRepository(BaseSQLRepository[SessionORM]):
    """Session repository backed by the application database."""

    orm_class = SessionORM

    def __init__(self, session: AsyncSession, engine: AsyncEngine) -> None:
        """Initialize the repository.

        Args:
            session: Database session for all operations.
            engine: Engine used for the session number allocations that
                commit outside the request transaction.
        """
        super().__init__(session)
        self._engine = engine

    def _not_found(self, entity_id: uuid.UUID) -> NotFoundError:
        """Build the not-found error for an id.

        Args:
            entity_id: Id of the missing row.

        Returns:
            Not-found error.
        """
        return SessionNotFound(entity_id)

    def _duplicate_external_id(self, session: Session) -> DuplicateSessionExternalId:
        """Build the conflict error for a duplicated imported_from and external id.

        Args:
            session: Session whose imported_from and external id collided.

        Returns:
            Conflict error.
        """
        return DuplicateSessionExternalId(session.imported_from, session.external_id)

    async def allocate_session_number(self, agent_id: uuid.UUID) -> int:
        """Bump the agent's session counter and return the new value.

        The bump commits in its own transaction, so the agent row lock is
        held for the bump alone and a rolled back create leaves a gap.

        Args:
            agent_id: Id of the agent to bump.

        Raises:
            AgentNotFound: No agent has this id.

        Returns:
            New session number.
        """
        statement = (
            update(AgentORM)
            .where(AgentORM.id == agent_id)
            .values(latest_session_number=AgentORM.latest_session_number + 1)
            .returning(AgentORM.latest_session_number)
        )
        async with self._engine.begin() as connection:
            result = await connection.execute(statement)
            row = result.first()
        if row is None:
            raise AgentNotFound(agent_id)
        return row[0]

    async def create(self, session: Session) -> Session:
        """Persist a new session.

        Args:
            session: Session to store.

        Raises:
            DuplicateSessionExternalId: The imported_from and external id pair is
                already registered.

        Returns:
            Stored session with timestamps set.
        """
        row = SessionORM.from_domain(session)
        await self._add(
            row,
            {
                SESSION_IMPORTED_FROM_EXTERNAL_ID_UNIQUE_CONSTRAINT: lambda: (
                    self._duplicate_external_id(session)
                )
            },
        )
        return row.to_domain()

    async def get(self, session_id: uuid.UUID, exclusive: bool = False) -> Session:
        """Load a session by id.

        Args:
            session_id: Id of the session.
            exclusive: Whether to lock the row for the duration of the
                transaction.

        Raises:
            SessionNotFound: No session has this id.

        Returns:
            Stored session.
        """
        row = await self._get_row(session_id, exclusive=exclusive)
        return row.to_domain()

    async def query(
        self, session_filter: SessionFilter
    ) -> tuple[list[Session], str | None]:
        """Query sessions matching a filter.

        Args:
            session_filter: Filter and pagination parameters.

        Returns:
            Page of matching sessions and the next cursor.
        """
        statement = select(SessionORM)
        if session_filter.expression is not None:
            statement = statement.where(
                compile_filter_expression(
                    session_filter.expression, SESSION_FILTER_BINDINGS
                )
            )

        rows, next_cursor = await paginate(
            self._session, statement, session_filter, id_column=SessionORM.id
        )
        return [row.to_domain() for row in rows], next_cursor

    async def get_many(
        self, session_ids: Sequence[uuid.UUID]
    ) -> dict[uuid.UUID, Session]:
        """Bulk-load sessions by id, keyed by id, missing ids omitted.

        Args:
            session_ids: Ids of the sessions to load.

        Returns:
            Stored sessions keyed by id.
        """
        rows = await self._load_by_ids(list(session_ids))
        return {session_id: row.to_domain() for session_id, row in rows.items()}

    async def update(self, session: Session) -> Session:
        """Persist changes to an existing session.

        Args:
            session: Session with modified fields.

        Raises:
            SessionNotFound: No session has this id.
            DuplicateSessionExternalId: The imported_from and external id pair is
                already registered.

        Returns:
            Stored session with the updated timestamp renewed.
        """
        row = await self._get_row(session.id)
        tokens = session.tokens
        row.owner_id = session.owner_id
        row.agent_id = session.agent_id
        row.agent_version_id = session.agent_version_id
        row.task_id = session.task_id
        row.origin = session.origin.value
        row.status = session.status.value
        row.name = session.name
        row.inputs = session.inputs
        row.outputs = session.outputs
        row.error = session.error
        row.started_at = session.started_at
        row.ended_at = session.ended_at
        row.external_id = session.external_id
        row.metadata_ = session.metadata
        row.imported_from = session.imported_from
        row.framework = session.framework
        row.adapter_version = session.adapter_version
        row.cost = session.cost
        row.input_tokens = tokens.input_tokens if tokens is not None else None
        row.output_tokens = tokens.output_tokens if tokens is not None else None
        row.cached_input_tokens = (
            tokens.cached_input_tokens if tokens is not None else None
        )
        row.reasoning_tokens = tokens.reasoning_tokens if tokens is not None else None
        row.llm_call_count = session.llm_call_count
        row.tool_call_count = session.tool_call_count
        await self._flush(
            {
                SESSION_IMPORTED_FROM_EXTERNAL_ID_UNIQUE_CONSTRAINT: lambda: (
                    self._duplicate_external_id(session)
                )
            }
        )
        return row.to_domain()

    async def delete(self, session_id: uuid.UUID) -> None:
        """Delete a session by id.

        Deleting a session cascades its nodes.

        Args:
            session_id: Id of the session.

        Raises:
            SessionNotFound: No session has this id.
            SessionInUse: The session belongs to a cohort version and
                cannot be deleted.
            SessionInUseByTask: The session is a task's input or result
                session and cannot be deleted.
        """
        await self._delete_row(
            session_id,
            {
                COHORT_VERSION_SESSION_SESSION_ID_FOREIGN_KEY: lambda: SessionInUse(
                    session_id
                ),
                TASK_INPUT_SESSION_ID_FOREIGN_KEY: lambda: SessionInUseByTask(
                    session_id
                ),
                TASK_RESULT_SESSION_ID_FOREIGN_KEY: lambda: SessionInUseByTask(
                    session_id
                ),
            },
        )

    async def apply_rollups(
        self, session_id: uuid.UUID, deltas: SessionRollups
    ) -> None:
        """Apply rollup deltas to a session's cost, tokens, and call counts.

        This is a Core-level bulk statement rather than an ORM attribute
        mutation, so the ``updated`` timestamp needs to be stamped here
        explicitly, the ``onupdate`` client-side default never fires for it.

        Args:
            session_id: Id of the session.
            deltas: Rollup deltas to add.

        Raises:
            SessionNotFound: No session has this id.
        """
        statement = (
            update(SessionORM)
            .where(SessionORM.id == session_id)
            .values(
                cost=func.coalesce(SessionORM.cost, 0) + deltas.cost,
                input_tokens=func.coalesce(SessionORM.input_tokens, 0)
                + deltas.input_tokens,
                output_tokens=func.coalesce(SessionORM.output_tokens, 0)
                + deltas.output_tokens,
                cached_input_tokens=func.coalesce(SessionORM.cached_input_tokens, 0)
                + deltas.cached_input_tokens,
                reasoning_tokens=func.coalesce(SessionORM.reasoning_tokens, 0)
                + deltas.reasoning_tokens,
                llm_call_count=SessionORM.llm_call_count + deltas.llm_call_count,
                tool_call_count=SessionORM.tool_call_count + deltas.tool_call_count,
                updated=datetime.now(UTC),
            )
        )
        result = await self._session.execute(statement)
        rowcount = result.rowcount if isinstance(result, CursorResult) else 0
        if rowcount == 0:
            raise SessionNotFound(session_id)
        await self._session.flush()
