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

from sqlalchemy import delete, exists, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import col

from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.repositories.replay_repository import (
    translate_replay_integrity_error,
)
from kitaru.server.adapters.db.schemas.experiment import ExperimentSchema
from kitaru.server.adapters.db.schemas.experiment_run import (
    ExperimentRunSchema,
)
from kitaru.server.adapters.db.schemas.replay import ReplaySchema
from kitaru.server.adapters.db.schemas.tag import TagLinkSchema
from kitaru.server.adapters.db.tag_filtering import tagged_resource_ids
from kitaru.server.application.models.experiment_runs import ExperimentRunFilter
from kitaru.server.domain.experiment import ExperimentNotFound
from kitaru.server.domain.experiment_run import (
    ExperimentRun,
    ExperimentRunNotFound,
)
from kitaru.server.domain.replay import Replay
from kitaru.server.domain.tag import TagResourceType


class SQLExperimentRunRepository:
    """Experiment run repository backed by the application database."""

    def __init__(self, session: AsyncSession) -> None:
        """Initialize the repository.

        Args:
            session: Database session for all operations.
        """
        self._session = session

    async def create(self, run: ExperimentRun, replays: list[Replay]) -> ExperimentRun:
        """Persist a new experiment run with its replays as one batch.

        Locks the experiment row to serialize the per-experiment number
        counter against concurrent run creation.

        Args:
            run: Experiment run to store.
            replays: Replays to store with the run.

        Raises:
            ExperimentNotFound: No experiment has the run's experiment id.

        Returns:
            Stored experiment run with the number and timestamps set.
        """
        experiment_row = (
            await self._session.execute(
                select(ExperimentSchema)
                .where(col(ExperimentSchema.id) == run.experiment_id)
                .with_for_update()
            )
        ).scalar_one_or_none()
        if experiment_row is None:
            raise ExperimentNotFound(run.experiment_id)
        max_number = (
            await self._session.execute(
                select(func.max(col(ExperimentRunSchema.number))).where(
                    col(ExperimentRunSchema.experiment_id) == run.experiment_id
                )
            )
        ).scalar_one()
        row = ExperimentRunSchema.from_domain(run)
        row.number = (max_number or 0) + 1
        replay_rows = [ReplaySchema.from_domain(replay) for replay in replays]
        try:
            async with self._session.begin_nested():
                self._session.add(row)
                self._session.add_all(replay_rows)
                await self._session.flush()
        except IntegrityError as exc:
            for replay in replays:
                translate_replay_integrity_error(exc, replay)
            raise
        return row.to_domain()

    async def get(self, run_id: uuid.UUID) -> ExperimentRun:
        """Load an experiment run by id.

        Args:
            run_id: Id of the experiment run.

        Raises:
            ExperimentRunNotFound: No experiment run has this id.

        Returns:
            Stored experiment run.
        """
        row = await self._session.get(ExperimentRunSchema, run_id)
        if row is None:
            raise ExperimentRunNotFound(run_id)
        return row.to_domain()

    async def query(
        self, run_filter: ExperimentRunFilter
    ) -> tuple[list[ExperimentRun], int]:
        """Query experiment runs matching a filter.

        Args:
            run_filter: Filter and pagination parameters.

        Returns:
            Page of matching experiment runs and the total match count.
        """
        statement = select(ExperimentRunSchema)
        if run_filter.experiment_id is not None:
            statement = statement.where(
                col(ExperimentRunSchema.experiment_id) == run_filter.experiment_id
            )
        if run_filter.status is not None:
            statement = statement.where(
                col(ExperimentRunSchema.status) == run_filter.status.value
            )
        if run_filter.tag is not None:
            statement = statement.where(
                col(ExperimentRunSchema.id).in_(
                    tagged_resource_ids(run_filter.tag, TagResourceType.EXPERIMENT_RUN)
                )
            )
        rows, total = await paginate(
            self._session,
            statement,
            order_by=col(ExperimentRunSchema.id),
            page=run_filter.page,
            page_size=run_filter.page_size,
        )
        return [row.to_domain() for row in rows], total

    async def update(self, run: ExperimentRun) -> ExperimentRun:
        """Persist changes to an existing experiment run.

        Args:
            run: Experiment run with modified fields.

        Raises:
            ExperimentRunNotFound: No experiment run has this id.

        Returns:
            Stored experiment run with the updated timestamp renewed.
        """
        row = await self._session.get(ExperimentRunSchema, run.id)
        if row is None:
            raise ExperimentRunNotFound(run.id)
        row.owner_id = run.owner_id
        row.experiment_id = run.experiment_id
        row.number = run.number
        row.status = run.status.value
        row.agent_version_id = run.agent_version_id
        row.score_baselines = run.score_baselines
        row.execution_target = run.execution_target.value
        row.executor_handle = run.executor_handle
        row.started_at = run.started_at
        row.ended_at = run.ended_at
        row.summary = run.summary
        row.error = run.error
        await self._session.flush()
        return row.to_domain()

    async def delete(self, run_id: uuid.UUID) -> None:
        """Delete an experiment run by id, including its replays and tag links.

        Replays cascade through their foreign key, tag links carry no
        foreign key and are removed here.

        Args:
            run_id: Id of the experiment run.

        Raises:
            ExperimentRunNotFound: No experiment run has this id.
        """
        row = await self._session.get(ExperimentRunSchema, run_id)
        if row is None:
            raise ExperimentRunNotFound(run_id)
        await self._session.execute(
            delete(TagLinkSchema).where(
                col(TagLinkSchema.resource_type)
                == TagResourceType.EXPERIMENT_RUN.value,
                col(TagLinkSchema.resource_id) == run_id,
            )
        )
        await self._session.delete(row)
        await self._session.flush()

    async def has_runs(self, experiment_id: uuid.UUID) -> bool:
        """Report whether an experiment has stored runs.

        Args:
            experiment_id: Id of the experiment.

        Returns:
            ``True`` when a stored run belongs to the experiment.
        """
        statement = select(
            exists().where(col(ExperimentRunSchema.experiment_id) == experiment_id)
        )
        return bool((await self._session.execute(statement)).scalar())
