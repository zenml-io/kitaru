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
"""SQL experiment repository."""

import uuid

from sqlalchemy import delete, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import col

from kitaru.server.adapters.db.errors import violated_constraint
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.schemas.experiment import (
    EXPERIMENT_COHORT_ID_FOREIGN_KEY,
    EXPERIMENT_NAME_UNIQUE_CONSTRAINT,
    EXPERIMENT_REPLAY_CONFIG_ID_FOREIGN_KEY,
    ExperimentSchema,
)
from kitaru.server.adapters.db.schemas.experiment_run import (
    EXPERIMENT_RUN_EXPERIMENT_ID_FOREIGN_KEY,
)
from kitaru.server.adapters.db.schemas.tag import TagLinkSchema
from kitaru.server.adapters.db.tag_filtering import tagged_resource_ids
from kitaru.server.application.models.experiments import ExperimentFilter
from kitaru.server.domain.cohort import CohortNotFound
from kitaru.server.domain.experiment import (
    DuplicateExperimentName,
    Experiment,
    ExperimentInUse,
    ExperimentNotFound,
)
from kitaru.server.domain.replay_config import ReplayConfigNotFound
from kitaru.server.domain.tag import TagResourceType


class SQLExperimentRepository:
    """Experiment repository backed by the application database."""

    def __init__(self, session: AsyncSession) -> None:
        """Initialize the repository.

        Args:
            session: Database session for all operations.
        """
        self._session = session

    def _translate_integrity_error(
        self, exc: IntegrityError, experiment: Experiment
    ) -> None:
        """Translate an integrity error into the matching domain error.

        Args:
            exc: Integrity error raised by a flush.
            experiment: Experiment that was written.

        Raises:
            DuplicateExperimentName: The experiment name is already
                registered.
            CohortNotFound: No cohort has the experiment's cohort id.
            ReplayConfigNotFound: No replay config has the experiment's
                replay config id.
        """
        constraint = violated_constraint(exc)
        if constraint == EXPERIMENT_NAME_UNIQUE_CONSTRAINT:
            raise DuplicateExperimentName(experiment.name) from exc
        if constraint == EXPERIMENT_COHORT_ID_FOREIGN_KEY:
            raise CohortNotFound(experiment.cohort_id) from exc
        if constraint == EXPERIMENT_REPLAY_CONFIG_ID_FOREIGN_KEY:
            raise ReplayConfigNotFound(experiment.replay_config_id) from exc

    async def create(self, experiment: Experiment) -> Experiment:
        """Persist a new experiment.

        Args:
            experiment: Experiment to store.

        Raises:
            DuplicateExperimentName: The experiment name is already
                registered.
            CohortNotFound: No cohort has the experiment's cohort id.
            ReplayConfigNotFound: No replay config has the experiment's
                replay config id.

        Returns:
            Stored experiment with timestamps set.
        """
        row = ExperimentSchema.from_domain(experiment)
        try:
            async with self._session.begin_nested():
                self._session.add(row)
                await self._session.flush()
        except IntegrityError as exc:
            self._translate_integrity_error(exc, experiment)
            raise
        return row.to_domain()

    async def get(self, experiment_id: uuid.UUID) -> Experiment:
        """Load an experiment by id.

        Args:
            experiment_id: Id of the experiment.

        Raises:
            ExperimentNotFound: No experiment has this id.

        Returns:
            Stored experiment.
        """
        row = await self._session.get(ExperimentSchema, experiment_id)
        if row is None:
            raise ExperimentNotFound(experiment_id)
        return row.to_domain()

    async def query(
        self, experiment_filter: ExperimentFilter
    ) -> tuple[list[Experiment], int]:
        """Query experiments matching a filter.

        Args:
            experiment_filter: Filter and pagination parameters.

        Returns:
            Page of matching experiments and the total match count.
        """
        statement = select(ExperimentSchema)
        if experiment_filter.name is not None:
            statement = statement.where(
                col(ExperimentSchema.name) == experiment_filter.name
            )
        if experiment_filter.tag is not None:
            statement = statement.where(
                col(ExperimentSchema.id).in_(
                    tagged_resource_ids(
                        experiment_filter.tag, TagResourceType.EXPERIMENT
                    )
                )
            )
        rows, total = await paginate(
            self._session,
            statement,
            order_by=col(ExperimentSchema.id),
            page=experiment_filter.page,
            page_size=experiment_filter.page_size,
        )
        return [row.to_domain() for row in rows], total

    async def update(self, experiment: Experiment) -> Experiment:
        """Persist changes to an existing experiment.

        Args:
            experiment: Experiment with modified fields.

        Raises:
            ExperimentNotFound: No experiment has this id.
            DuplicateExperimentName: The experiment name is already
                registered.
            CohortNotFound: No cohort has the experiment's cohort id.
            ReplayConfigNotFound: No replay config has the experiment's
                replay config id.

        Returns:
            Stored experiment with the updated timestamp renewed.
        """
        row = await self._session.get(ExperimentSchema, experiment.id)
        if row is None:
            raise ExperimentNotFound(experiment.id)
        row.owner_id = experiment.owner_id
        row.name = experiment.name
        row.description = experiment.description
        row.cohort_id = experiment.cohort_id
        row.replay_config_id = experiment.replay_config_id
        try:
            async with self._session.begin_nested():
                await self._session.flush()
        except IntegrityError as exc:
            self._translate_integrity_error(exc, experiment)
            raise
        return row.to_domain()

    async def delete(self, experiment_id: uuid.UUID) -> None:
        """Delete an experiment by id, including its tag links.

        Tag links carry no foreign key and are removed here.

        Args:
            experiment_id: Id of the experiment.

        Raises:
            ExperimentNotFound: No experiment has this id.
            ExperimentInUse: The experiment has runs.
        """
        row = await self._session.get(ExperimentSchema, experiment_id)
        if row is None:
            raise ExperimentNotFound(experiment_id)
        try:
            async with self._session.begin_nested():
                await self._session.execute(
                    delete(TagLinkSchema).where(
                        col(TagLinkSchema.resource_type)
                        == TagResourceType.EXPERIMENT.value,
                        col(TagLinkSchema.resource_id) == experiment_id,
                    )
                )
                await self._session.delete(row)
                await self._session.flush()
        except IntegrityError as exc:
            if violated_constraint(exc) == EXPERIMENT_RUN_EXPERIMENT_ID_FOREIGN_KEY:
                raise ExperimentInUse(experiment_id) from exc
            raise
