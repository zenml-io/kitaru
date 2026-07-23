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
"""Experiment run use cases."""

import uuid
from datetime import UTC, datetime, timedelta

from kitaru.server.application.interfaces.experiment_repository import (
    ExperimentRepository,
)
from kitaru.server.application.interfaces.experiment_run_repository import (
    ExperimentRunRepository,
)
from kitaru.server.application.interfaces.replay_config_repository import (
    ReplayConfigRepository,
)
from kitaru.server.application.interfaces.replay_repository import (
    ReplayRepository,
)
from kitaru.server.application.interfaces.session_repository import (
    SessionRepository,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.experiment_runs import (
    ExperimentRunFilter,
    ExperimentRunReplaysFilter,
)
from kitaru.server.application.models.replays import ReplayFilter
from kitaru.server.application.services.run_finalization import (
    finalize_run_if_drained,
    load_run_replays,
)
from kitaru.server.domain.experiment_run import (
    TERMINAL_RUN_STATUSES,
    ExperimentRun,
    ExperimentRunProgress,
    ExperimentRunStatus,
)
from kitaru.server.domain.replay import Replay, ReplayStatus
from kitaru.server.domain.replay_config import ReplayConfig


class ExperimentRunService:
    """Experiment run use cases."""

    def __init__(
        self,
        repository: ExperimentRunRepository,
        replay_repository: ReplayRepository,
        replay_config_repository: ReplayConfigRepository,
        experiment_repository: ExperimentRepository,
        session_repository: SessionRepository,
        heartbeat_timeout_seconds: int,
        max_attempts: int,
    ) -> None:
        """Initialize the service.

        Args:
            repository: Experiment run repository.
            replay_repository: Replay repository.
            replay_config_repository: Replay config repository.
            experiment_repository: Experiment repository.
            session_repository: Session repository.
            heartbeat_timeout_seconds: Seconds after which a heartbeat
                counts as lost.
            max_attempts: Attempt count at which a stale replay times out.
        """
        self._repository = repository
        self._replay_repository = replay_repository
        self._replay_config_repository = replay_config_repository
        self._experiment_repository = experiment_repository
        self._session_repository = session_repository
        self._heartbeat_timeout_seconds = heartbeat_timeout_seconds
        self._max_attempts = max_attempts

    def _stale_before(self) -> datetime:
        """Compute the heartbeat staleness threshold.

        Returns:
            Time before which a heartbeat counts as lost.
        """
        return datetime.now(UTC) - timedelta(seconds=self._heartbeat_timeout_seconds)

    async def get_run(
        self, run_id: uuid.UUID, actor: AuthContext
    ) -> tuple[ExperimentRun, ExperimentRunProgress]:
        """Get an experiment run by id.

        Args:
            run_id: Id of the experiment run.
            actor: Caller context.

        Raises:
            ExperimentRunNotFound: No experiment run has this id.

        Returns:
            Stored experiment run and its progress.
        """
        _ = actor
        run = await self._repository.get(run_id)
        counts = await self._replay_repository.count_by_status(
            [run_id], self._stale_before(), self._max_attempts
        )
        return run, ExperimentRunProgress.from_counts(counts.get(run_id, {}))

    async def list_runs(
        self, run_filter: ExperimentRunFilter, actor: AuthContext
    ) -> tuple[list[tuple[ExperimentRun, ExperimentRunProgress]], int]:
        """List experiment runs matching a filter.

        Args:
            run_filter: Filter and pagination parameters.
            actor: Caller context.

        Raises:
            ExperimentNotFound: No experiment has the filtered experiment
                id.

        Returns:
            Page of matching experiment runs with their progress and the
            total match count.
        """
        _ = actor
        if run_filter.experiment_id is not None:
            await self._experiment_repository.get(run_filter.experiment_id)
        runs, total = await self._repository.query(run_filter)
        counts = await self._replay_repository.count_by_status(
            [run.id for run in runs], self._stale_before(), self._max_attempts
        )
        return [
            (run, ExperimentRunProgress.from_counts(counts.get(run.id, {})))
            for run in runs
        ], total

    async def list_run_replays(
        self,
        run_id: uuid.UUID,
        replays_filter: ExperimentRunReplaysFilter,
        actor: AuthContext,
    ) -> tuple[list[tuple[Replay, ReplayConfig]], int]:
        """List the replays of an experiment run.

        Args:
            run_id: Id of the experiment run.
            replays_filter: Pagination parameters.
            actor: Caller context.

        Raises:
            ExperimentRunNotFound: No experiment run has this id.

        Returns:
            Page of replays with their replay configs and the total match
            count.
        """
        _ = actor
        await self._repository.get(run_id)
        replays, total = await self._replay_repository.query(
            ReplayFilter(
                experiment_run_id=run_id,
                page=replays_filter.page,
                page_size=replays_filter.page_size,
            )
        )
        stale_before = self._stale_before()
        replays = [
            replay.with_staleness(stale_before, self._max_attempts)
            for replay in replays
        ]
        configs = await self._replay_config_repository.get_many(
            [replay.replay_config_id for replay in replays]
        )
        return [(replay, configs[replay.replay_config_id]) for replay in replays], total

    async def claim_replays(
        self,
        run_id: uuid.UUID,
        worker_id: str,
        max_replays: int,
        actor: AuthContext,
    ) -> list[tuple[Replay, ReplayConfig]]:
        """Atomically claim pending replays of a run for a worker.

        Stale claimed or running replays are requeued or timed out first.
        The first claim moves a pending run to running. Canceling and
        terminal runs yield no replays. An empty claim finalizes the run
        when every replay is already terminal.

        Args:
            run_id: Id of the experiment run.
            worker_id: Id of the claiming worker.
            max_replays: Maximum number of replays to claim.
            actor: Caller context.

        Raises:
            ExperimentRunNotFound: No experiment run has this id.

        Returns:
            Claimed replays with their replay configs.
        """
        _ = actor
        run = await self._repository.get(run_id)
        if (
            run.status is ExperimentRunStatus.CANCELING
            or run.status in TERMINAL_RUN_STATUSES
        ):
            return []
        await self._replay_repository.requeue_stale(
            run_id, self._stale_before(), self._max_attempts
        )
        replays = await self._replay_repository.claim_pending(
            run_id, worker_id, max_replays
        )
        if replays and run.status is ExperimentRunStatus.PENDING:
            run.start()
            await self._repository.update(run)
        if not replays:
            # The requeue may have timed out the run's last replay, which
            # leaves no transition that would finalize the run.
            await finalize_run_if_drained(
                self._repository,
                self._replay_repository,
                self._session_repository,
                run_id,
            )
        configs = await self._replay_config_repository.get_many(
            [replay.replay_config_id for replay in replays]
        )
        return [(replay, configs[replay.replay_config_id]) for replay in replays]

    async def cancel_run(
        self, run_id: uuid.UUID, actor: AuthContext
    ) -> tuple[ExperimentRun, ExperimentRunProgress]:
        """Cancel an experiment run.

        Pending and claimed replays are canceled immediately, running ones
        drain through the heartbeat path. The run lands on canceled right
        away when no running replay remains.

        Args:
            run_id: Id of the experiment run.
            actor: Caller context.

        Raises:
            ExperimentRunNotFound: No experiment run has this id.
            InvalidExperimentRunTransition: The run is already terminal.

        Returns:
            Updated experiment run and its progress.
        """
        _ = actor
        run = await self._repository.get(run_id)
        run.cancel()
        run = await self._repository.update(run)
        replays = await load_run_replays(self._replay_repository, run_id)
        for replay in replays:
            if replay.status in (ReplayStatus.PENDING, ReplayStatus.CLAIMED):
                replay.cancel()
                await self._replay_repository.update(replay)
        await finalize_run_if_drained(
            self._repository,
            self._replay_repository,
            self._session_repository,
            run_id,
        )
        return await self.get_run(run_id, actor)
