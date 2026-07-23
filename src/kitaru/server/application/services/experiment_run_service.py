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
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.experiment_runs import (
    ExperimentRunFilter,
    ExperimentRunReplaysFilter,
)
from kitaru.server.application.models.replays import ReplayFilter
from kitaru.server.domain.experiment_run import (
    ExperimentRun,
    ExperimentRunProgress,
)
from kitaru.server.domain.replay import Replay
from kitaru.server.domain.replay_config import ReplayConfig


class ExperimentRunService:
    """Experiment run use cases."""

    def __init__(
        self,
        repository: ExperimentRunRepository,
        replay_repository: ReplayRepository,
        replay_config_repository: ReplayConfigRepository,
        experiment_repository: ExperimentRepository,
    ) -> None:
        """Initialize the service.

        Args:
            repository: Experiment run repository.
            replay_repository: Replay repository.
            replay_config_repository: Replay config repository.
            experiment_repository: Experiment repository.
        """
        self._repository = repository
        self._replay_repository = replay_repository
        self._replay_config_repository = replay_config_repository
        self._experiment_repository = experiment_repository

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
        counts = await self._replay_repository.count_by_status([run_id])
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
        counts = await self._replay_repository.count_by_status([run.id for run in runs])
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
        configs = await self._replay_config_repository.get_many(
            [replay.replay_config_id for replay in replays]
        )
        return [(replay, configs[replay.replay_config_id]) for replay in replays], total
