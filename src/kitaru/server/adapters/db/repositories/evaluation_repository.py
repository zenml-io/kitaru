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
from collections.abc import Mapping
from datetime import UTC, datetime

from sqlalchemy import ColumnElement, select, text
from sqlalchemy.dialects.postgresql import insert

from kitaru.server.adapters.db.filtering import (
    FilterBinding,
    build_scope_condition_binding,
    compile_filter_expression,
)
from kitaru.server.adapters.db.orm.cohort_version import CohortVersionORM
from kitaru.server.adapters.db.orm.cohort_version_session import (
    CohortVersionSessionORM,
)
from kitaru.server.adapters.db.orm.evaluation import EvaluationORM
from kitaru.server.adapters.db.orm.plugin import PluginORM, PluginVersionORM
from kitaru.server.adapters.db.orm.replay import ReplayORM
from kitaru.server.adapters.db.orm.session import SessionORM
from kitaru.server.adapters.db.orm.task import TaskORM
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.application.interfaces.evaluation_repository import (
    EvaluationWithEvaluator,
)
from kitaru.server.application.models.evaluation import EvaluationFilter
from kitaru.server.domain.base import NotFoundError
from kitaru.server.domain.evaluation import Evaluation, EvaluationNotFound
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

    async def merge_session_evaluations(
        self, session_id: uuid.UUID, evaluations: list[Evaluation]
    ) -> list[Evaluation]:
        """Insert or replace manual evaluations upserted on (session, name).

        One statement carries the whole batch. The conflict target is the
        partial unique index on (session_id, name) where task_id is null, so
        a resent name overwrites its data type, score, value, explanation,
        and pass flag while leaving its id, owner, and creation time in
        place.

        Args:
            session_id: Id of the session the evaluations belong to.
            evaluations: Fully resolved evaluations to store, in request
                order.

        Returns:
            Stored evaluations in request order.
        """
        _ = session_id
        if not evaluations:
            return []
        values = [EvaluationORM.column_values(evaluation) for evaluation in evaluations]
        statement = insert(EvaluationORM).values(values)
        statement = statement.on_conflict_do_update(
            index_elements=[EvaluationORM.session_id, EvaluationORM.name],
            index_where=text("task_id IS NULL"),
            set_={
                "data_type": statement.excluded.data_type,
                "numerical_value": statement.excluded.numerical_value,
                "string_value": statement.excluded.string_value,
                "explanation": statement.excluded.explanation,
                "passed": statement.excluded.passed,
                "updated": datetime.now(UTC),
            },
        ).returning(EvaluationORM)
        rows = (
            await self._session.scalars(
                statement, execution_options={"populate_existing": True}
            )
        ).all()
        by_name = {row.name: row for row in rows}
        return [by_name[evaluation.name].to_domain() for evaluation in evaluations]

    async def create_task_evaluations(
        self, evaluations: list[Evaluation]
    ) -> list[Evaluation]:
        """Insert evaluation rows produced by a completed evaluator task.

        Args:
            evaluations: Fully resolved evaluations to store, in result order.

        Returns:
            Stored evaluations in result order.
        """
        if not evaluations:
            return []
        rows = [
            EvaluationORM(**EvaluationORM.column_values(evaluation))
            for evaluation in evaluations
        ]
        self._session.add_all(rows)
        await self._flush()
        return [row.to_domain() for row in rows]
