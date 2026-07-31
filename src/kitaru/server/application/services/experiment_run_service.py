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

from kitaru.server.application.interfaces.experiment_run_repository import (
    ExperimentRunRepository,
)
from kitaru.server.application.interfaces.job_repository import JobRepository
from kitaru.server.application.interfaces.replay_repository import ReplayRepository
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.experiment_run import (
    ExperimentRunFilter,
    ExperimentRunJobsFilter,
)
from kitaru.server.application.models.job import JobFilter
from kitaru.server.application.models.replay import ReplayStatusCounts
from kitaru.server.application.services.server_analytics import ServerAnalytics
from kitaru.server.application.services.task_transitions import TaskTransitions
from kitaru.server.domain.experiment_run import ExperimentRun
from kitaru.server.domain.job import Job


class ExperimentRunService:
    """Experiment run use cases."""

    def __init__(
        self,
        repository: ExperimentRunRepository,
        replay_repository: ReplayRepository,
        job_repository: JobRepository,
        transitions: TaskTransitions,
        analytics: ServerAnalytics | None = None,
    ) -> None:
        """Initialize the service.

        Args:
            repository: Experiment run repository.
            replay_repository: Replay repository, for progress and job
                listing.
            job_repository: Job repository.
            transitions: Task transition dispatch, for job cancellation.
            analytics: Analytics tracker, None skips tracking.
        """
        self._repository = repository
        self._replays = replay_repository
        self._jobs = job_repository
        self._transitions = transitions
        self._analytics = analytics

    async def get_run(
        self, experiment_run_id: uuid.UUID, actor: AuthContext
    ) -> tuple[ExperimentRun, ReplayStatusCounts]:
        """Get an experiment run and its replay progress by id.

        Args:
            experiment_run_id: Id of the run.
            actor: Caller context.

        Raises:
            ExperimentRunNotFound: No run has this id.

        Returns:
            Stored run and its replay counts by status.
        """
        _ = actor
        run = await self._repository.get(experiment_run_id)
        counts = await self._replays.count_by_status(experiment_run_id)
        return run, counts

    async def list_runs(
        self, run_filter: ExperimentRunFilter, actor: AuthContext
    ) -> tuple[list[tuple[ExperimentRun, ReplayStatusCounts]], str | None]:
        """List experiment runs matching a filter, each with its replay progress.

        Args:
            run_filter: Filter and pagination parameters.
            actor: Caller context.

        Returns:
            Page of matching runs paired with their replay counts, and the
            next cursor.
        """
        _ = actor
        runs, next_cursor = await self._repository.query(run_filter)
        counts = await self._replays.count_by_status_many([run.id for run in runs])
        pairs = [(run, counts.get(run.id, ReplayStatusCounts())) for run in runs]
        return pairs, next_cursor

    async def list_run_jobs(
        self,
        experiment_run_id: uuid.UUID,
        job_filter: ExperimentRunJobsFilter,
        actor: AuthContext,
    ) -> tuple[list[Job], str | None]:
        """List the jobs backing an experiment run's replays.

        Args:
            experiment_run_id: Id of the run.
            job_filter: Filter and pagination parameters.
            actor: Caller context.

        Raises:
            ExperimentRunNotFound: No run has this id.

        Returns:
            Page of matching jobs and the next cursor.
        """
        _ = actor
        await self._repository.get(experiment_run_id)
        replays = await self._replays.list_by_experiment_run(experiment_run_id)
        job_ids = [replay.job_id for replay in replays]
        return await self._jobs.query(
            JobFilter(
                expression=job_filter.expression,
                job_ids=job_ids,
                cursor=job_filter.cursor,
                size=job_filter.size,
                sort=job_filter.sort,
            )
        )

    async def cancel_run(
        self, experiment_run_id: uuid.UUID, actor: AuthContext
    ) -> tuple[ExperimentRun, ReplayStatusCounts]:
        """Request cancellation of a running experiment run.

        Every non-settled replay's job is canceled the way job cancellation
        works: pending tasks move straight to canceled, claimed and running
        tasks are stamped for their worker or the sweep to settle. A run
        whose jobs are already fully drained finalizes within this call.

        Args:
            experiment_run_id: Id of the run.
            actor: Caller context.

        Raises:
            ExperimentRunNotFound: No run has this id.
            IllegalExperimentRunStatusTransition: The run is not running.

        Returns:
            Run carrying the cancel request, and its replay counts by status.
        """
        _ = actor
        run = await self._repository.get(experiment_run_id, exclusive=True)
        run.cancel()
        await self._repository.update(run)
        for replay in await self._replays.list_by_experiment_run(experiment_run_id):
            if replay.settled:
                continue
            job = await self._jobs.get(replay.job_id, exclusive=True)
            if job.settled:
                continue
            await self._transitions.cancel_job(job)
        run = await self._repository.get(experiment_run_id)
        counts = await self._replays.count_by_status(experiment_run_id)
        return run, counts

    async def delete_run(
        self, experiment_run_id: uuid.UUID, actor: AuthContext
    ) -> None:
        """Delete an experiment run and its jobs.

        Each job delete cascades its tasks and replay row, so the run row's
        own delete has nothing left to cascade.

        Args:
            experiment_run_id: Id of the run.
            actor: Caller context.

        Raises:
            ExperimentRunNotFound: No run has this id.
        """
        _ = actor
        await self._repository.get(experiment_run_id)
        for replay in await self._replays.list_by_experiment_run(experiment_run_id):
            await self._jobs.delete(replay.job_id)
        await self._repository.delete(experiment_run_id)
