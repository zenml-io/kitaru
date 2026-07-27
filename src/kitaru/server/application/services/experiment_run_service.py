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
from kitaru.server.application.interfaces.job_repository import (
    JobRepository,
)
from kitaru.server.application.interfaces.replay_config_repository import (
    ReplayConfigRepository,
)
from kitaru.server.application.interfaces.session_repository import (
    SessionRepository,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.experiment_runs import (
    ExperimentRunFilter,
    ExperimentRunJobsFilter,
)
from kitaru.server.application.models.jobs import JobFilter
from kitaru.server.application.services.run_finalization import (
    finalize_run_if_drained,
    load_run_jobs,
)
from kitaru.server.domain.experiment_run import (
    TERMINAL_RUN_STATUSES,
    ExperimentRun,
    ExperimentRunActive,
    ExperimentRunProgress,
)
from kitaru.server.domain.job import TERMINAL_JOB_STATUSES, JobStatus, Replay
from kitaru.server.domain.replay_config import ReplayConfig


class ExperimentRunService:
    """Experiment run use cases."""

    def __init__(
        self,
        repository: ExperimentRunRepository,
        job_repository: JobRepository,
        replay_config_repository: ReplayConfigRepository,
        experiment_repository: ExperimentRepository,
        session_repository: SessionRepository,
        heartbeat_timeout_seconds: int,
        max_attempts: int,
    ) -> None:
        """Initialize the service.

        Args:
            repository: Experiment run repository.
            job_repository: Job repository.
            replay_config_repository: Replay config repository.
            experiment_repository: Experiment repository.
            session_repository: Session repository.
            heartbeat_timeout_seconds: Seconds after which a heartbeat
                counts as lost.
            max_attempts: Attempt count at which a stale job times out.
        """
        self._repository = repository
        self._job_repository = job_repository
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
        counts = await self._job_repository.count_by_status(
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
        counts = await self._job_repository.count_by_status(
            [run.id for run in runs], self._stale_before(), self._max_attempts
        )
        return [
            (run, ExperimentRunProgress.from_counts(counts.get(run.id, {})))
            for run in runs
        ], total

    async def list_run_jobs(
        self,
        run_id: uuid.UUID,
        jobs_filter: ExperimentRunJobsFilter,
        actor: AuthContext,
    ) -> tuple[list[tuple[Replay, ReplayConfig]], int]:
        """List the jobs of an experiment run.

        Args:
            run_id: Id of the experiment run.
            jobs_filter: Filter and pagination parameters.
            actor: Caller context.

        Raises:
            ExperimentRunNotFound: No experiment run has this id.

        Returns:
            Page of jobs with their replay configs and the total match
            count.
        """
        _ = actor
        await self._repository.get(run_id)
        stale_before = self._stale_before()
        jobs, total = await self._job_repository.query(
            JobFilter(
                experiment_run_id=run_id,
                status=jobs_filter.status,
                stale_before=stale_before,
                max_attempts=self._max_attempts,
                page=jobs_filter.page,
                page_size=jobs_filter.page_size,
            )
        )
        replays = [
            job.with_staleness(stale_before, self._max_attempts)
            for job in jobs
            if isinstance(job, Replay)
        ]
        configs = await self._replay_config_repository.get_many(
            [replay.replay_config_id for replay in replays]
        )
        return [(replay, configs[replay.replay_config_id]) for replay in replays], total

    async def cancel_run(
        self, run_id: uuid.UUID, actor: AuthContext
    ) -> tuple[ExperimentRun, ExperimentRunProgress]:
        """Cancel an experiment run.

        Pending, claimed, and scoring jobs are canceled immediately,
        together with the score jobs of the scoring ones. Running jobs
        drain through the heartbeat path. The run lands on canceled right
        away when no running job remains.

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
        jobs = await load_run_jobs(self._job_repository, run_id)
        for job in jobs:
            if job.status not in (
                JobStatus.PENDING,
                JobStatus.CLAIMED,
                JobStatus.SCORING,
            ):
                continue
            if job.status is JobStatus.SCORING:
                for child in await self._job_repository.list_children(job.id):
                    if child.status not in TERMINAL_JOB_STATUSES:
                        child.cancel()
                        await self._job_repository.update(child)
            job.cancel()
            await self._job_repository.update(job)
        await finalize_run_if_drained(
            self._repository,
            self._job_repository,
            self._session_repository,
            run_id,
        )
        return await self.get_run(run_id, actor)

    async def delete_run(self, run_id: uuid.UUID, actor: AuthContext) -> None:
        """Delete a terminal experiment run, including its jobs.

        Deletes each job's config when nothing else references it.

        Args:
            run_id: Id of the experiment run.
            actor: Caller context.

        Raises:
            ExperimentRunNotFound: No experiment run has this id.
            ExperimentRunActive: The run is not terminal.
        """
        _ = actor
        run = await self._repository.get(run_id)
        if run.status not in TERMINAL_RUN_STATUSES:
            raise ExperimentRunActive(run.id)
        jobs = await load_run_jobs(self._job_repository, run_id)
        config_ids = {job.replay_config_id for job in jobs}
        await self._repository.delete(run_id)
        for config_id in config_ids:
            await self._replay_config_repository.delete_if_unreferenced(config_id)
