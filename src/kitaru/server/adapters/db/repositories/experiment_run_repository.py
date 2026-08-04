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
"""SQL experiment run repository."""

import uuid
from collections.abc import Mapping

from sqlalchemy import func, select

from kitaru.api_models.v1.tag import TagResourceType
from kitaru.server.adapters.db.filtering import (
    FilterBinding,
    build_tag_condition_binding,
    compile_filter_expression,
)
from kitaru.server.adapters.db.orm.experiment_run import (
    EXPERIMENT_RUN_NUMBER_UNIQUE_CONSTRAINT,
    ExperimentRunORM,
)
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.application.models.experiment_run import ExperimentRunFilter
from kitaru.server.domain.base import NotFoundError
from kitaru.server.domain.experiment_run import (
    DuplicateExperimentRunNumber,
    ExperimentRun,
    ExperimentRunNotFound,
)

EXPERIMENT_RUN_FILTER_BINDINGS: Mapping[str, FilterBinding] = {
    "experiment_id": ExperimentRunORM.experiment_id,
    "cohort_version_id": ExperimentRunORM.cohort_version_id,
    "agent_version_id": ExperimentRunORM.agent_version_id,
    "status": ExperimentRunORM.status,
    "tag": build_tag_condition_binding(
        TagResourceType.EXPERIMENT_RUN, ExperimentRunORM.id
    ),
}


class SQLExperimentRunRepository(BaseSQLRepository[ExperimentRunORM]):
    """Experiment run repository backed by the application database."""

    orm_class = ExperimentRunORM

    def _not_found(self, entity_id: uuid.UUID) -> NotFoundError:
        """Build the not-found error for an id.

        Args:
            entity_id: Id of the missing row.

        Returns:
            Not-found error.
        """
        return ExperimentRunNotFound(entity_id)

    async def create(self, run: ExperimentRun) -> ExperimentRun:
        """Persist a new experiment run.

        Args:
            run: Experiment run to store.

        Raises:
            DuplicateExperimentRunNumber: The experiment already has a run
                with this number.

        Returns:
            Stored experiment run with timestamps set.
        """
        row = ExperimentRunORM.from_domain(run)
        await self._add(
            row,
            {
                EXPERIMENT_RUN_NUMBER_UNIQUE_CONSTRAINT: lambda: (
                    DuplicateExperimentRunNumber(run.experiment_id, run.number)
                )
            },
        )
        return row.to_domain()

    async def get(
        self, experiment_run_id: uuid.UUID, exclusive: bool = False
    ) -> ExperimentRun:
        """Load an experiment run by id.

        Args:
            experiment_run_id: Id of the run.
            exclusive: Whether to lock the row for the duration of the
                transaction.

        Raises:
            ExperimentRunNotFound: No run has this id.

        Returns:
            Stored experiment run.
        """
        row = await self._get_row(experiment_run_id, exclusive=exclusive)
        return row.to_domain()

    async def query(
        self, run_filter: ExperimentRunFilter
    ) -> tuple[list[ExperimentRun], str | None]:
        """Query experiment runs matching a filter.

        Args:
            run_filter: Filter and pagination parameters.

        Returns:
            Page of matching runs and the next cursor.
        """
        statement = select(ExperimentRunORM)
        if run_filter.expression is not None:
            statement = statement.where(
                compile_filter_expression(
                    run_filter.expression, EXPERIMENT_RUN_FILTER_BINDINGS
                )
            )
        rows, next_cursor = await paginate(
            self._session, statement, run_filter, id_column=ExperimentRunORM.id
        )
        return [row.to_domain() for row in rows], next_cursor

    async def update(self, run: ExperimentRun) -> ExperimentRun:
        """Persist changes to an existing experiment run.

        Args:
            run: Experiment run with modified fields.

        Raises:
            ExperimentRunNotFound: No run has this id.

        Returns:
            Stored experiment run with the updated timestamp renewed.
        """
        row = await self._get_row(run.id)
        row.apply(run)
        await self._flush()
        return row.to_domain()

    async def delete(self, experiment_run_id: uuid.UUID) -> None:
        """Delete an experiment run by id.

        Args:
            experiment_run_id: Id of the run.

        Raises:
            ExperimentRunNotFound: No run has this id.
        """
        await self._delete_row(experiment_run_id)

    async def get_max_number(self, experiment_id: uuid.UUID) -> int:
        """Read the highest run number an experiment has assigned.

        Args:
            experiment_id: Id of the experiment.

        Returns:
            Highest assigned run number, or 0 when the experiment has no runs.
        """
        statement = select(func.max(ExperimentRunORM.number)).where(
            ExperimentRunORM.experiment_id == experiment_id
        )
        result = await self._session.scalar(statement)
        return result if result is not None else 0

    async def exists_for_experiment(self, experiment_id: uuid.UUID) -> bool:
        """Report whether an experiment has any run.

        Args:
            experiment_id: Id of the experiment.

        Returns:
            Whether the experiment has any run.
        """
        statement = select(
            select(ExperimentRunORM.id)
            .where(ExperimentRunORM.experiment_id == experiment_id)
            .exists()
        )
        return bool(await self._session.scalar(statement))
