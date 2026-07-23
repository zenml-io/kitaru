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
from datetime import datetime

from sqlalchemy import ColumnElement, and_, case, exists, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import col

from kitaru.server.adapters.db.errors import violated_constraint
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.schemas.replay import (
    REPLAY_AGENT_VERSION_ID_FOREIGN_KEY,
    REPLAY_EXPERIMENT_RUN_ID_FOREIGN_KEY,
    REPLAY_ORIGINAL_SESSION_ID_FOREIGN_KEY,
    REPLAY_REPLAY_CONFIG_ID_FOREIGN_KEY,
    REPLAY_RESULT_SESSION_ID_FOREIGN_KEY,
    REPLAY_SESSION_UNIQUE_CONSTRAINT,
    ReplaySchema,
)
from kitaru.server.application.models.replays import ReplayFilter
from kitaru.server.domain.agent_version import AgentVersionNotFound
from kitaru.server.domain.experiment_run import ExperimentRunNotFound
from kitaru.server.domain.replay import (
    DuplicateReplaySession,
    Replay,
    ReplayNotFound,
    ReplayStatus,
)
from kitaru.server.domain.replay_config import ReplayConfigNotFound
from kitaru.server.domain.session import SessionNotFound


def translate_replay_integrity_error(exc: IntegrityError, replay: Replay) -> None:
    """Translate a replay write integrity error into the domain error.

    Args:
        exc: Integrity error raised by a flush.
        replay: Replay that was written.

    Raises:
        DuplicateReplaySession: The run already replays the original
            session.
        ExperimentRunNotFound: No experiment run has the replay's
            experiment run id.
        ReplayConfigNotFound: No replay config has the replay's replay
            config id.
        AgentVersionNotFound: No agent version has the replay's agent
            version id.
        SessionNotFound: No session has the replay's original session id.
    """
    constraint = violated_constraint(exc)
    if constraint == REPLAY_SESSION_UNIQUE_CONSTRAINT:
        assert replay.experiment_run_id is not None
        raise DuplicateReplaySession(
            replay.experiment_run_id, replay.original_session_id
        ) from exc
    if constraint == REPLAY_EXPERIMENT_RUN_ID_FOREIGN_KEY:
        assert replay.experiment_run_id is not None
        raise ExperimentRunNotFound(replay.experiment_run_id) from exc
    if constraint == REPLAY_REPLAY_CONFIG_ID_FOREIGN_KEY:
        raise ReplayConfigNotFound(replay.replay_config_id) from exc
    if constraint == REPLAY_AGENT_VERSION_ID_FOREIGN_KEY:
        raise AgentVersionNotFound(replay.agent_version_id) from exc
    if constraint == REPLAY_ORIGINAL_SESSION_ID_FOREIGN_KEY:
        raise SessionNotFound(replay.original_session_id) from exc


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
            ExperimentRunNotFound: No experiment run has the replay's
                experiment run id.
            ReplayConfigNotFound: No replay config has the replay's replay
                config id.
            AgentVersionNotFound: No agent version has the replay's agent
                version id.
            SessionNotFound: No session has the replay's original session
                id.
            DuplicateReplaySession: The run already replays the original
                session.

        Returns:
            Stored replay with timestamps set.
        """
        row = ReplaySchema.from_domain(replay)
        try:
            async with self._session.begin_nested():
                self._session.add(row)
                await self._session.flush()
        except IntegrityError as exc:
            translate_replay_integrity_error(exc, replay)
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
        if replay_filter.original_session_id is not None:
            statement = statement.where(
                col(ReplaySchema.original_session_id)
                == replay_filter.original_session_id
            )
        if replay_filter.status is not None:
            statement = statement.where(
                col(ReplaySchema.status) == replay_filter.status.value
            )
        if replay_filter.standalone is not None:
            if replay_filter.standalone:
                statement = statement.where(
                    col(ReplaySchema.experiment_run_id).is_(None)
                )
            else:
                statement = statement.where(
                    col(ReplaySchema.experiment_run_id).is_not(None)
                )
        rows, total = await paginate(
            self._session,
            statement,
            order_by=col(ReplaySchema.id),
            page=replay_filter.page,
            page_size=replay_filter.page_size,
        )
        return [row.to_domain() for row in rows], total

    def _apply(self, row: ReplaySchema, replay: Replay) -> None:
        """Copy domain replay fields onto an existing row.

        Args:
            row: Row to update.
            replay: Replay with modified fields.
        """
        row.experiment_run_id = replay.experiment_run_id
        row.replay_config_id = replay.replay_config_id
        row.agent_version_id = replay.agent_version_id
        row.original_session_id = replay.original_session_id
        row.result_session_id = replay.result_session_id
        row.status = replay.status.value
        row.attempt = replay.attempt
        row.worker_id = replay.worker_id
        row.claimed_at = replay.claimed_at
        row.heartbeat_at = replay.heartbeat_at
        row.started_at = replay.started_at
        row.ended_at = replay.ended_at
        row.error = replay.error
        row.passed = replay.passed
        row.score = replay.score
        row.scores = replay.scores
        row.diff = replay.diff

    async def update(self, replay: Replay) -> Replay:
        """Persist changes to an existing replay.

        Args:
            replay: Replay with modified fields.

        Raises:
            ReplayNotFound: No replay has this id.
            SessionNotFound: No session has the replay's result session id.

        Returns:
            Stored replay with the updated timestamp renewed.
        """
        row = await self._session.get(ReplaySchema, replay.id)
        if row is None:
            raise ReplayNotFound(replay.id)
        try:
            async with self._session.begin_nested():
                self._apply(row, replay)
                await self._session.flush()
        except IntegrityError as exc:
            constraint = violated_constraint(exc)
            if constraint == REPLAY_RESULT_SESSION_ID_FOREIGN_KEY:
                assert replay.result_session_id is not None
                raise SessionNotFound(replay.result_session_id) from exc
            translate_replay_integrity_error(exc, replay)
            raise
        return row.to_domain()

    def _stale_condition(self, stale_before: datetime) -> ColumnElement[bool]:
        """Build the lost-heartbeat condition on claimed or running rows.

        Args:
            stale_before: Heartbeats older than this time count as lost.

        Returns:
            SQL condition.
        """
        return and_(
            col(ReplaySchema.status).in_(
                [ReplayStatus.CLAIMED.value, ReplayStatus.RUNNING.value]
            ),
            func.coalesce(col(ReplaySchema.heartbeat_at), col(ReplaySchema.claimed_at))
            < stale_before,
        )

    async def requeue_stale(
        self, run_id: uuid.UUID, stale_before: datetime, max_attempts: int
    ) -> None:
        """Requeue or time out a run's replays with lost heartbeats.

        Args:
            run_id: Id of the experiment run.
            stale_before: Heartbeats older than this time count as lost.
            max_attempts: Attempt count at which a stale replay times out.
        """
        statement = (
            select(ReplaySchema)
            .where(
                col(ReplaySchema.experiment_run_id) == run_id,
                self._stale_condition(stale_before),
            )
            .with_for_update(skip_locked=True)
        )
        rows = (await self._session.scalars(statement)).all()
        for row in rows:
            replay = row.to_domain()
            self._apply(row, replay.with_staleness(stale_before, max_attempts))
        await self._session.flush()

    async def claim_pending(
        self, run_id: uuid.UUID, worker_id: str, limit: int
    ) -> list[Replay]:
        """Atomically claim pending replays of a run for a worker.

        Rows locked by a concurrent claim are skipped via
        ``FOR UPDATE SKIP LOCKED``, so parallel workers never double-claim.

        Args:
            run_id: Id of the experiment run.
            worker_id: Id of the claiming worker.
            limit: Maximum number of replays to claim.

        Returns:
            Claimed replays.
        """
        statement = (
            select(ReplaySchema)
            .where(
                col(ReplaySchema.experiment_run_id) == run_id,
                col(ReplaySchema.status) == ReplayStatus.PENDING.value,
            )
            .order_by(col(ReplaySchema.id))
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        rows = (await self._session.scalars(statement)).all()
        for row in rows:
            replay = row.to_domain()
            replay.claim(worker_id)
            self._apply(row, replay)
        await self._session.flush()
        return [row.to_domain() for row in rows]

    async def count_by_status(
        self, run_ids: list[uuid.UUID], stale_before: datetime, max_attempts: int
    ) -> dict[uuid.UUID, dict[ReplayStatus, int]]:
        """Count replays by status for a set of experiment runs.

        Claimed or running replays with lost heartbeats count as pending,
        or as timed out once the attempt count reached the maximum, without
        writing.

        Args:
            run_ids: Ids of the experiment runs.
            stale_before: Heartbeats older than this time count as lost.
            max_attempts: Attempt count at which a stale replay times out.

        Returns:
            Replay counts by status, keyed by experiment run id.
        """
        if not run_ids:
            return {}
        stale = self._stale_condition(stale_before)
        effective_status = case(
            (
                and_(stale, col(ReplaySchema.attempt) >= max_attempts),
                ReplayStatus.TIMED_OUT.value,
            ),
            (stale, ReplayStatus.PENDING.value),
            else_=col(ReplaySchema.status),
        )
        statement = (
            select(
                col(ReplaySchema.experiment_run_id),
                effective_status,
                func.count(),
            )
            .where(col(ReplaySchema.experiment_run_id).in_(run_ids))
            .group_by(col(ReplaySchema.experiment_run_id), effective_status)
        )
        counts: dict[uuid.UUID, dict[ReplayStatus, int]] = {}
        for run_id, status, count in (await self._session.execute(statement)).all():
            counts.setdefault(run_id, {})[ReplayStatus(status)] = count
        return counts

    async def references_agent_version(self, version_id: uuid.UUID) -> bool:
        """Report whether a stored replay references an agent version.

        Args:
            version_id: Id of the agent version.

        Returns:
            ``True`` when a stored replay references the version.
        """
        statement = select(
            exists().where(col(ReplaySchema.agent_version_id) == version_id)
        )
        return bool((await self._session.execute(statement)).scalar())
