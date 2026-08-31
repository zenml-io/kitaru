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
"""SQL evaluation repository."""

import uuid
from collections.abc import Callable, Mapping, Sequence

from sqlalchemy import ColumnElement, func, select

from kitaru.server.adapters.db.filtering import (
    FilterBinding,
    build_scope_condition_binding,
    compile_filter_expression,
)
from kitaru.server.adapters.db.orm.cohort_version import CohortVersionORM
from kitaru.server.adapters.db.orm.cohort_version_session import (
    CohortVersionSessionORM,
)
from kitaru.server.adapters.db.orm.evaluation import (
    EVALUATION_SESSION_ID_NAME_UNIQUE_INDEX,
    EvaluationORM,
)
from kitaru.server.adapters.db.orm.plugin import PluginORM, PluginVersionORM
from kitaru.server.adapters.db.orm.replay import ReplayORM
from kitaru.server.adapters.db.orm.replay_evaluation import ReplayEvaluationORM
from kitaru.server.adapters.db.orm.session import SessionORM
from kitaru.server.adapters.db.orm.task import TaskORM
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.application.interfaces.evaluation_repository import (
    EvaluationIdentity,
    EvaluationWithEvaluator,
)
from kitaru.server.application.models.evaluation import EvaluationFilter
from kitaru.server.domain.base import DomainError, NotFoundError
from kitaru.server.domain.evaluation import (
    Evaluation,
    EvaluationNameConflict,
    EvaluationNotFound,
)
from kitaru.server.domain.plugin import PluginVersionIdNotFound
from kitaru.server.filtering import FilterCondition

EvaluatorInfo = tuple[str, int]


# Sessions hang off a cohort version, so a cohort scope spans every version of
# the cohort rather than only its latest.
_scopes_to_cohort = build_scope_condition_binding(
    local_column=CohortVersionSessionORM.cohort_version_id,
    related_key=CohortVersionORM.id,
    scope_column=CohortVersionORM.cohort_id,
)

# A replay's tasks share its job, and the run owns the replay.
_scopes_to_experiment_run = build_scope_condition_binding(
    local_column=TaskORM.job_id,
    related_key=ReplayORM.job_id,
    scope_column=ReplayORM.experiment_run_id,
)


def _compile_cohort_condition(condition: FilterCondition) -> ColumnElement[bool]:
    """Compile a cohort scope condition into a session membership predicate.

    Args:
        condition: Validated cohort condition.

    Returns:
        SQL predicate.
    """
    memberships = (
        select(CohortVersionSessionORM.session_id)
        .where(
            CohortVersionSessionORM.session_id == EvaluationORM.session_id,
            _scopes_to_cohort(condition),
        )
        .correlate(EvaluationORM)
    )
    return memberships.exists()


def _compile_experiment_run_condition(
    condition: FilterCondition,
) -> ColumnElement[bool]:
    """Compile an experiment run scope condition into a task predicate.

    Args:
        condition: Validated experiment run condition.

    Returns:
        SQL predicate.
    """
    # Scoped by producing task rather than by session, so a run's baseline and
    # result evaluations both reach the same result set.
    run_tasks = (
        select(TaskORM.id)
        .where(
            TaskORM.id == EvaluationORM.task_id,
            _scopes_to_experiment_run(condition),
        )
        .correlate(EvaluationORM)
    )
    return run_tasks.exists()


EVALUATION_FILTER_BINDINGS: Mapping[str, FilterBinding] = {
    "id": EvaluationORM.id,
    "session_id": EvaluationORM.session_id,
    "task_id": EvaluationORM.task_id,
    "evaluator_version_id": EvaluationORM.evaluator_version_id,
    "name": EvaluationORM.name,
    "data_type": EvaluationORM.data_type,
    "agent_id": build_scope_condition_binding(
        local_column=EvaluationORM.session_id,
        related_key=SessionORM.id,
        scope_column=SessionORM.agent_id,
    ),
    "cohort_id": _compile_cohort_condition,
    "experiment_run_id": _compile_experiment_run_condition,
}


class SQLEvaluationRepository(BaseSQLRepository[EvaluationORM]):
    """Evaluation repository backed by the application database."""

    orm_class = EvaluationORM

    def _not_found(self, entity_id: uuid.UUID) -> NotFoundError:
        """Build the not-found error for an id.

        Args:
            entity_id: Id of the missing row.

        Returns:
            Not-found error.
        """
        return EvaluationNotFound(entity_id)

    async def _load_evaluators(
        self, version_ids: set[uuid.UUID]
    ) -> dict[uuid.UUID, EvaluatorInfo]:
        """Bulk-load evaluator name and version for a set of plugin version ids.

        Args:
            version_ids: Ids of the referenced plugin versions.

        Returns:
            Evaluator (name, version) pairs keyed by plugin version id,
            missing ids omitted.
        """
        if not version_ids:
            return {}
        statement = (
            select(PluginVersionORM.id, PluginORM.name, PluginVersionORM.version)
            .join(PluginORM, PluginORM.id == PluginVersionORM.plugin_id)
            .where(PluginVersionORM.id.in_(version_ids))
        )
        rows = (await self._session.execute(statement)).all()
        return {row.id: (row.name, row.version) for row in rows}

    async def get(self, evaluation_id: uuid.UUID) -> EvaluationWithEvaluator:
        """Load an evaluation by id, joined with its evaluator name and version.

        One statement carries both, unlike the bulk lookup ``query()`` uses,
        since there is exactly one row to join here.

        Args:
            evaluation_id: Id of the evaluation.

        Raises:
            EvaluationNotFound: No evaluation has this id.

        Returns:
            Stored evaluation paired with its evaluator name and version,
            both ``None`` on a manual evaluation.
        """
        statement = (
            select(EvaluationORM, PluginORM.name, PluginVersionORM.version)
            .outerjoin(
                PluginVersionORM,
                PluginVersionORM.id == EvaluationORM.evaluator_version_id,
            )
            .outerjoin(PluginORM, PluginORM.id == PluginVersionORM.plugin_id)
            .where(EvaluationORM.id == evaluation_id)
        )
        row = (await self._session.execute(statement)).one_or_none()
        if row is None:
            raise EvaluationNotFound(evaluation_id)
        evaluation_row, evaluator_name, evaluator_version = row
        return EvaluationWithEvaluator(
            evaluation_row.to_domain(), evaluator_name, evaluator_version
        )

    async def query(
        self, evaluation_filter: EvaluationFilter
    ) -> tuple[list[EvaluationWithEvaluator], str | None]:
        """Query evaluations matching a filter.

        Args:
            evaluation_filter: Filter and pagination parameters.

        Returns:
            Page of matching evaluations, each paired with its evaluator name
            and version, and the next cursor.
        """
        statement = select(EvaluationORM)
        if evaluation_filter.expression is not None:
            statement = statement.where(
                compile_filter_expression(
                    evaluation_filter.expression, EVALUATION_FILTER_BINDINGS
                )
            )
        rows, next_cursor = await paginate(
            self._session, statement, evaluation_filter, id_column=EvaluationORM.id
        )
        version_ids = {
            row.evaluator_version_id
            for row in rows
            if row.evaluator_version_id is not None
        }
        evaluators = await self._load_evaluators(version_ids)
        items = [
            EvaluationWithEvaluator(
                row.to_domain(),
                *evaluators.get(row.evaluator_version_id, (None, None)),
            )
            for row in rows
        ]
        return items, next_cursor

    async def create_session_evaluations(
        self, session_id: uuid.UUID, evaluations: list[Evaluation]
    ) -> list[Evaluation]:
        """Insert manual evaluations into a session.

        ``evaluator_version_id`` and ``task_id`` stay null for every row this
        writes.

        Args:
            session_id: Id of the session the evaluations belong to.
            evaluations: Fully resolved evaluations to store, in request
                order.

        Raises:
            EvaluationNameConflict: A name in the batch already exists for
                the session.

        Returns:
            Stored evaluations in request order.
        """
        if not evaluations:
            return []
        rows = [
            EvaluationORM(**EvaluationORM.column_values(evaluation))
            for evaluation in evaluations
        ]
        constraints: dict[str, Callable[[], DomainError]] = {
            EVALUATION_SESSION_ID_NAME_UNIQUE_INDEX: lambda: EvaluationNameConflict(
                evaluations[0].name, session_id
            ),
        }
        await self._add_all(rows, constraints)
        return [row.to_domain() for row in rows]

    async def create_task_evaluations(
        self, evaluations: list[Evaluation], replay_id: uuid.UUID | None
    ) -> list[Evaluation]:
        """Insert evaluation rows produced by a completed evaluator task.

        Args:
            evaluations: Fully resolved evaluations to store, in result order.
            replay_id: Replay to link each stored row to, ``None`` for a
                standalone evaluation batch.

        Raises:
            PluginVersionIdNotFound: No plugin version has the evaluator
                version id, including one deleted concurrently with the task
                it scored.

        Returns:
            Stored evaluations in result order.
        """
        if not evaluations:
            return []
        evaluator_version_id = evaluations[0].evaluator_version_id
        if evaluator_version_id is not None:
            exists = await self._session.scalar(
                select(PluginVersionORM.id).where(
                    PluginVersionORM.id == evaluator_version_id
                )
            )
            if exists is None:
                raise PluginVersionIdNotFound(evaluator_version_id)
        rows = [
            EvaluationORM(**EvaluationORM.column_values(evaluation))
            for evaluation in evaluations
        ]
        await self._add_all(rows)
        if replay_id is not None:
            self._session.add_all(
                ReplayEvaluationORM(replay_id=replay_id, evaluation_id=row.id)
                for row in rows
            )
            await self._flush()
        return [row.to_domain() for row in rows]

    async def get_latest_evaluation_ids_by_identity(
        self, session_ids: Sequence[uuid.UUID]
    ) -> dict[EvaluationIdentity, uuid.UUID]:
        """Read the latest evaluation id per (session, evaluator version, params hash).

        Only rows carrying both an evaluator version id and a params hash
        are considered.

        Args:
            session_ids: Ids of the candidate sessions.

        Returns:
            Latest evaluation id keyed by (session_id, evaluator_version_id,
            params_hash), identities without a match omitted.
        """
        if not session_ids:
            return {}
        ranked = (
            select(
                EvaluationORM.session_id,
                EvaluationORM.evaluator_version_id,
                EvaluationORM.params_hash,
                EvaluationORM.id,
                func.row_number()
                .over(
                    partition_by=(
                        EvaluationORM.session_id,
                        EvaluationORM.evaluator_version_id,
                        EvaluationORM.params_hash,
                    ),
                    order_by=(EvaluationORM.created.desc(), EvaluationORM.id.desc()),
                )
                .label("rank"),
            )
            .where(
                EvaluationORM.session_id.in_(session_ids),
                EvaluationORM.evaluator_version_id.is_not(None),
                EvaluationORM.params_hash.is_not(None),
            )
            .subquery()
        )
        statement = select(
            ranked.c.session_id,
            ranked.c.evaluator_version_id,
            ranked.c.params_hash,
            ranked.c.id,
        ).where(ranked.c.rank == 1)
        rows = (await self._session.execute(statement)).all()
        return {
            (session_id, evaluator_version_id, params_hash): evaluation_id
            for session_id, evaluator_version_id, params_hash, evaluation_id in rows
        }

    async def add_replay_links(
        self, links: Sequence[tuple[uuid.UUID, uuid.UUID]]
    ) -> None:
        """Link replays to evaluations they adopted instead of re-running.

        Args:
            links: (replay_id, evaluation_id) pairs to link.
        """
        if not links:
            return
        self._session.add_all(
            ReplayEvaluationORM(replay_id=replay_id, evaluation_id=evaluation_id)
            for replay_id, evaluation_id in links
        )
        await self._flush()
