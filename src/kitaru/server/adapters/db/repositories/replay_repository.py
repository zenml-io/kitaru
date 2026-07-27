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
"""SQL replay repository."""

import uuid

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import col

from kitaru.server.adapters.db.errors import violated_constraint
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.schemas.replay import (
    REPLAY_INPUT_SESSION_ID_FOREIGN_KEY,
    REPLAY_JOB_ID_FOREIGN_KEY,
    REPLAY_JOB_ID_UNIQUE_CONSTRAINT,
    REPLAY_REPLAY_CONFIG_ID_FOREIGN_KEY,
    ReplaySchema,
)
from kitaru.server.application.models.replays import ReplayFilter
from kitaru.server.domain.job import JobNotFound
from kitaru.server.domain.replay import (
    DuplicateReplayJob,
    Replay,
    ReplayJobNotFound,
    ReplayNotFound,
)
from kitaru.server.domain.replay_config import ReplayConfigNotFound
from kitaru.server.domain.session import SessionNotFound


class SQLReplayRepository:
    """Replay repository backed by the application database."""

    def __init__(self, session: AsyncSession) -> None:
        """Initialize the repository.

        Args:
            session: Database session for all operations.
        """
        self._session = session

    async def create(self, replay: Replay) -> Replay:
        """Persist a new replay.

        Args:
            replay: Replay to store.

        Raises:
            JobNotFound: No job has the replay's job id.
            ReplayConfigNotFound: No replay config has the replay's replay
                config id.
            SessionNotFound: No session has the replay's input session id.
            DuplicateReplayJob: The job already has a replay.

        Returns:
            Stored replay with timestamps set.
        """
        row = ReplaySchema.from_domain(replay)
        try:
            async with self._session.begin_nested():
                self._session.add(row)
                await self._session.flush()
        except IntegrityError as exc:
            constraint = violated_constraint(exc)
            if constraint == REPLAY_JOB_ID_UNIQUE_CONSTRAINT:
                raise DuplicateReplayJob(replay.job_id) from exc
            if constraint == REPLAY_JOB_ID_FOREIGN_KEY:
                raise JobNotFound(replay.job_id) from exc
            if constraint == REPLAY_REPLAY_CONFIG_ID_FOREIGN_KEY:
                raise ReplayConfigNotFound(replay.replay_config_id) from exc
            if constraint == REPLAY_INPUT_SESSION_ID_FOREIGN_KEY:
                raise SessionNotFound(replay.input_session_id) from exc
            raise
        return row.to_domain()

    async def get(self, replay_id: uuid.UUID) -> Replay:
        """Load a replay by id.

        Args:
            replay_id: Id of the replay.

        Raises:
            ReplayNotFound: No replay has this id.

        Returns:
            Stored replay.
        """
        row = await self._session.get(ReplaySchema, replay_id)
        if row is None:
            raise ReplayNotFound(replay_id)
        return row.to_domain()

    async def get_by_job(self, job_id: uuid.UUID) -> Replay:
        """Load the replay of a job.

        Args:
            job_id: Id of the job.

        Raises:
            ReplayJobNotFound: The job has no replay.

        Returns:
            Stored replay.
        """
        statement = select(ReplaySchema).where(col(ReplaySchema.job_id) == job_id)
        row = (await self._session.scalars(statement)).one_or_none()
        if row is None:
            raise ReplayJobNotFound(job_id)
        return row.to_domain()

    async def get_many_by_jobs(
        self, job_ids: list[uuid.UUID]
    ) -> dict[uuid.UUID, Replay]:
        """Load replays by job id.

        Args:
            job_ids: Ids of the jobs.

        Returns:
            Stored replays keyed by job id, jobs without a replay omitted.
        """
        if not job_ids:
            return {}
        statement = select(ReplaySchema).where(col(ReplaySchema.job_id).in_(job_ids))
        rows = (await self._session.scalars(statement)).all()
        return {row.job_id: row.to_domain() for row in rows}

    async def query(self, replay_filter: ReplayFilter) -> tuple[list[Replay], int]:
        """Query replays matching a filter.

        Args:
            replay_filter: Filter and pagination parameters.

        Returns:
            Page of matching replays and the total match count.
        """
        statement = select(ReplaySchema)
        if replay_filter.experiment_run_id is not None:
            statement = statement.where(
                col(ReplaySchema.experiment_run_id) == replay_filter.experiment_run_id
            )
        if replay_filter.input_session_id is not None:
            statement = statement.where(
                col(ReplaySchema.input_session_id) == replay_filter.input_session_id
            )
        if replay_filter.passed is not None:
            statement = statement.where(
                col(ReplaySchema.passed).is_(replay_filter.passed)
            )
        rows, total = await paginate(
            self._session,
            statement,
            order_by=col(ReplaySchema.id),
            page=replay_filter.page,
            page_size=replay_filter.page_size,
        )
        return [row.to_domain() for row in rows], total

    async def update(self, replay: Replay) -> Replay:
        """Persist changes to an existing replay.

        Args:
            replay: Replay with modified fields.

        Raises:
            ReplayNotFound: No replay has this id.

        Returns:
            Stored replay with the updated timestamp renewed.
        """
        row = await self._session.get(ReplaySchema, replay.id)
        if row is None:
            raise ReplayNotFound(replay.id)
        row.passed = replay.passed
        row.score = replay.score
        row.scores = replay.scores
        row.diff = replay.diff
        row.error = replay.error
        await self._session.flush()
        return row.to_domain()
