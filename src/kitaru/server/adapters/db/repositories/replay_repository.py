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
from collections.abc import Callable, Mapping, Sequence

from sqlalchemy import func, select

from kitaru.api_models.v1.replay import ReplayStatus
from kitaru.server.adapters.db.filtering import (
    FilterBinding,
    compile_filter_expression,
)
from kitaru.server.adapters.db.orm.replay import (
    REPLAY_BASELINE_SESSION_ID_FOREIGN_KEY,
    REPLAY_EXPERIMENT_RUN_ID_FOREIGN_KEY,
    REPLAY_JOB_ID_UNIQUE_CONSTRAINT,
    REPLAY_RUN_BASELINE_UNIQUE_CONSTRAINT,
    ReplayORM,
)
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.application.models.replay import ReplayFilter, ReplayStatusCounts
from kitaru.server.domain.base import DomainError, NotFoundError
from kitaru.server.domain.experiment_run import ExperimentRunNotFound
from kitaru.server.domain.replay import (
    DuplicateReplayForBaseline,
    Replay,
    ReplayAlreadyExistsForJob,
    ReplayNotFound,
)
from kitaru.server.domain.session import SessionNotFound

REPLAY_FILTER_BINDINGS: Mapping[str, FilterBinding] = {
    "id": ReplayORM.id,
    "experiment_run_id": ReplayORM.experiment_run_id,
    "baseline_session_id": ReplayORM.baseline_session_id,
    "result_session_id": ReplayORM.result_session_id,
    "status": ReplayORM.status,
}


class SQLReplayRepository(BaseSQLRepository[ReplayORM]):
    """Replay repository backed by the application database."""

    orm_class = ReplayORM

    def _not_found(self, entity_id: uuid.UUID) -> NotFoundError:
        """Build the not-found error for an id.

        Args:
            entity_id: Id of the missing row.

        Returns:
            Not-found error.
        """
        return ReplayNotFound(entity_id)

    async def create(self, replay: Replay) -> Replay:
        """Persist a new replay.

        Args:
            replay: Replay to store.

        Raises:
            DuplicateReplayForBaseline: The run already holds a replay for
                this baseline session.
            ReplayAlreadyExistsForJob: The job already has a replay.
            SessionNotFound: No session has the baseline session id.
            ExperimentRunNotFound: No experiment run has the replay's run id.

        Returns:
            Stored replay with timestamps set.
        """
        row = ReplayORM.from_domain(replay)
        constraints: dict[str, Callable[[], DomainError]] = {
            REPLAY_RUN_BASELINE_UNIQUE_CONSTRAINT: lambda: DuplicateReplayForBaseline(
                replay.experiment_run_id, replay.baseline_session_id
            ),
            REPLAY_JOB_ID_UNIQUE_CONSTRAINT: lambda: ReplayAlreadyExistsForJob(
                replay.job_id
            ),
            REPLAY_BASELINE_SESSION_ID_FOREIGN_KEY: lambda: SessionNotFound(
                replay.baseline_session_id
            ),
        }
        if (experiment_run_id := replay.experiment_run_id) is not None:
            constraints[REPLAY_EXPERIMENT_RUN_ID_FOREIGN_KEY] = lambda: (
                ExperimentRunNotFound(experiment_run_id)
            )
        await self._add(row, constraints)
        return row.to_domain()

    async def create_many(self, replays: list[Replay]) -> list[Replay]:
        """Persist many new replays in one round trip, skipping constraint translation.

        Args:
            replays: Replays to store.

        Raises:
            IntegrityError: The batch collides on (job_id) or
                (experiment_run_id, baseline_session_id).

        Returns:
            Stored replays with timestamps set, in the same order.
        """
        if not replays:
            return []
        rows = [ReplayORM.from_domain(replay) for replay in replays]
        self._session.add_all(rows)
        await self._flush()
        return [row.to_domain() for row in rows]

    async def get(self, replay_id: uuid.UUID) -> Replay:
        """Load a replay by id.

        Args:
            replay_id: Id of the replay.

        Raises:
            ReplayNotFound: No replay has this id.

        Returns:
            Stored replay.
        """
        row = await self._get_row(replay_id)
        return row.to_domain()

    async def get_by_job_id(self, job_id: uuid.UUID) -> Replay | None:
        """Load the replay owning a job, if any.

        Args:
            job_id: Id of the job.

        Returns:
            Stored replay, or ``None`` when the job holds no replay.
        """
        statement = select(ReplayORM).where(ReplayORM.job_id == job_id)
        row = (await self._session.scalars(statement)).one_or_none()
        return row.to_domain() if row is not None else None

    async def get_by_result_session_id(self, session_id: uuid.UUID) -> Replay | None:
        """Load the replay that produced a session, if any.

        Args:
            session_id: Id of the produced session.

        Returns:
            Stored replay, or ``None`` when no replay produced the session.
        """
        statement = select(ReplayORM).where(ReplayORM.result_session_id == session_id)
        row = (await self._session.scalars(statement)).one_or_none()
        return row.to_domain() if row is not None else None

    async def get_many_by_job_ids(
        self, job_ids: Sequence[uuid.UUID]
    ) -> dict[uuid.UUID, Replay]:
        """Bulk-load the replay of each job, keyed by job id.

        Args:
            job_ids: Ids of the jobs.

        Returns:
            Replays keyed by job id, jobs without a replay omitted.
        """
        if not job_ids:
            return {}
        statement = select(ReplayORM).where(ReplayORM.job_id.in_(list(job_ids)))
        rows = (await self._session.scalars(statement)).all()
        return {row.job_id: row.to_domain() for row in rows}

    async def query(
        self, replay_filter: ReplayFilter
    ) -> tuple[list[Replay], str | None]:
        """Query replays matching a filter.

        Args:
            replay_filter: Filter and pagination parameters.

        Returns:
            Page of matching replays and the next cursor.
        """
        statement = select(ReplayORM)
        if replay_filter.expression is not None:
            statement = statement.where(
                compile_filter_expression(
                    replay_filter.expression, REPLAY_FILTER_BINDINGS
                )
            )
        rows, next_cursor = await paginate(
            self._session, statement, replay_filter, id_column=ReplayORM.id
        )
        return [row.to_domain() for row in rows], next_cursor

    async def list_by_experiment_run(
        self, experiment_run_id: uuid.UUID
    ) -> list[Replay]:
        """Load every replay of an experiment run.

        Args:
            experiment_run_id: Id of the run.

        Returns:
            Replays of the run, in creation order.
        """
        statement = (
            select(ReplayORM)
            .where(ReplayORM.experiment_run_id == experiment_run_id)
            .order_by(ReplayORM.id.asc())
        )
        rows = (await self._session.scalars(statement)).all()
        return [row.to_domain() for row in rows]

    async def update(self, replay: Replay) -> Replay:
        """Persist changes to an existing replay.

        Args:
            replay: Replay with modified fields.

        Raises:
            ReplayNotFound: No replay has this id.

        Returns:
            Stored replay with the updated timestamp renewed.
        """
        row = await self._get_row(replay.id)
        row.apply(replay)
        await self._flush()
        return row.to_domain()

    async def update_many(self, replays: list[Replay]) -> list[Replay]:
        """Persist changes to many existing replays in one round trip.

        No replay row is locked ahead of this call, so the rows are applied
        in id order, the first lock each row's update takes is acquired in
        the same order every caller touching many replay rows takes.

        Args:
            replays: Replays with modified fields.

        Raises:
            ReplayNotFound: A replay id matches no replay.

        Returns:
            Stored replays with the updated timestamp renewed, in id order.
        """
        if not replays:
            return []
        rows = []
        for replay in sorted(replays, key=lambda replay: replay.id):
            row = await self._get_row(replay.id)
            row.apply(replay)
            rows.append(row)
        await self._flush()
        return [row.to_domain() for row in rows]

    async def count_by_status(self, experiment_run_id: uuid.UUID) -> ReplayStatusCounts:
        """Count an experiment run's replays by status.

        Args:
            experiment_run_id: Id of the run.

        Returns:
            Replay counts by status.
        """
        counts = await self.count_by_status_many([experiment_run_id])
        return counts.get(experiment_run_id, ReplayStatusCounts())

    async def count_by_status_many(
        self, experiment_run_ids: Sequence[uuid.UUID]
    ) -> dict[uuid.UUID, ReplayStatusCounts]:
        """Bulk-count replays by status for many experiment runs.

        Args:
            experiment_run_ids: Ids of the runs.

        Returns:
            Replay status counts keyed by run id, missing ids holding zero
            counts.
        """
        if not experiment_run_ids:
            return {}
        statement = (
            select(
                ReplayORM.experiment_run_id, ReplayORM.status, func.count(ReplayORM.id)
            )
            .where(ReplayORM.experiment_run_id.in_(experiment_run_ids))
            .group_by(ReplayORM.experiment_run_id, ReplayORM.status)
        )
        rows = (await self._session.execute(statement)).all()
        tallies: dict[uuid.UUID, dict[str, int]] = {}
        for run_id, status, count in rows:
            tallies.setdefault(run_id, {})[status] = count
        return {
            run_id: ReplayStatusCounts(
                pending=tally.get(ReplayStatus.PENDING.value, 0),
                evaluating=tally.get(ReplayStatus.EVALUATING.value, 0),
                completed=tally.get(ReplayStatus.COMPLETED.value, 0),
                failed=tally.get(ReplayStatus.FAILED.value, 0),
                canceled=tally.get(ReplayStatus.CANCELED.value, 0),
            )
            for run_id, tally in tallies.items()
        }

    async def exists_for_replay_config(self, replay_config_id: uuid.UUID) -> bool:
        """Report whether any replay references a replay config.

        Args:
            replay_config_id: Id of the replay config.

        Returns:
            Whether a replay references the replay config.
        """
        statement = select(
            select(ReplayORM.id)
            .where(ReplayORM.replay_config_id == replay_config_id)
            .exists()
        )
        return bool(await self._session.scalar(statement))
