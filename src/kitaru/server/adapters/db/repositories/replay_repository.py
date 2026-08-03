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
from collections.abc import Mapping, Sequence

from sqlalchemy import ColumnElement, func, select

from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.replay import ReplayStatus
from kitaru.api_models.v1.task import TaskKind
from kitaru.server.adapters.db.filtering import (
    FilterBinding,
    compile_column_condition,
    compile_filter_expression,
)
from kitaru.server.adapters.db.orm.replay import (
    REPLAY_JOB_ID_UNIQUE_CONSTRAINT,
    REPLAY_RUN_BASELINE_UNIQUE_CONSTRAINT,
    ReplayORM,
)
from kitaru.server.adapters.db.orm.task import TaskORM
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.application.models.replay import ReplayFilter, ReplayStatusCounts
from kitaru.server.domain.base import NotFoundError
from kitaru.server.domain.replay import (
    DuplicateReplayForBaseline,
    Replay,
    ReplayAlreadyExistsForJob,
    ReplayNotFound,
)
from kitaru.server.filtering import FilterCondition


def _compile_result_session_condition(
    condition: FilterCondition,
) -> ColumnElement[bool]:
    """Compile a result session condition into an agent task predicate.

    Args:
        condition: Validated result session condition.

    Returns:
        SQL predicate.
    """
    # The result session belongs to the replay's agent task, not to the replay
    # row, so this resolves through the job the two share.
    agent_tasks = select(TaskORM.job_id).where(TaskORM.kind == TaskKind.AGENT.value)
    if condition.op is FilterOp.IS_NULL:
        # A replay whose agent task has not produced a session yet, which
        # includes one that never will because the task failed. Written as a
        # correlated NOT EXISTS: Postgres cannot turn NOT IN into an anti-join,
        # so the IN form would hash the whole task table on every call.
        linked = (
            select(TaskORM.id)
            .where(
                TaskORM.job_id == ReplayORM.job_id,
                TaskORM.kind == TaskKind.AGENT.value,
                TaskORM.result_session_id.is_not(None),
            )
            .correlate(ReplayORM)
        )
        return ~linked.exists()
    return ReplayORM.job_id.in_(
        agent_tasks.where(
            compile_column_condition(TaskORM.result_session_id, condition)
        )
    )


REPLAY_FILTER_BINDINGS: Mapping[str, FilterBinding] = {
    "experiment_run_id": ReplayORM.experiment_run_id,
    "baseline_session_id": ReplayORM.baseline_session_id,
    "result_session_id": _compile_result_session_condition,
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

        Returns:
            Stored replay with timestamps set.
        """
        row = ReplayORM.from_domain(replay)
        await self._add(
            row,
            {
                REPLAY_RUN_BASELINE_UNIQUE_CONSTRAINT: lambda: (
                    DuplicateReplayForBaseline(
                        replay.experiment_run_id, replay.baseline_session_id
                    )
                ),
                REPLAY_JOB_ID_UNIQUE_CONSTRAINT: lambda: ReplayAlreadyExistsForJob(
                    replay.job_id
                ),
            },
        )
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
