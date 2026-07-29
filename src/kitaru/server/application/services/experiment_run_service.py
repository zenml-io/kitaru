"""Experiment-run use cases."""

import uuid

from kitaru.server.application.interfaces.experiment_repository import (
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
from kitaru.server.application.services.job_service import JobService
from kitaru.server.domain.base import ConflictError
from kitaru.server.domain.experiment_run import (
    ExperimentRun,
    ExperimentRunProgress,
)
from kitaru.server.domain.job import Job


class ExperimentRunService:
    """Read, cancel, and delete experiment runs."""

    def __init__(
        self,
        repository: ExperimentRunRepository,
        replay_repository: ReplayRepository,
        job_repository: JobRepository,
        job_service: JobService,
    ) -> None:
        """Initialize the service."""
        self._repository = repository
        self._replay_repository = replay_repository
        self._job_repository = job_repository
        self._job_service = job_service

    async def get_run(
        self, run_id: uuid.UUID, actor: AuthContext
    ) -> tuple[ExperimentRun, ExperimentRunProgress]:
        """Get a run and its replay progress."""
        _ = actor
        run = await self._repository.get(run_id)
        return run, await self._repository.progress(run.id)

    async def list_runs(
        self, run_filter: ExperimentRunFilter, actor: AuthContext
    ) -> tuple[
        list[tuple[ExperimentRun, ExperimentRunProgress]],
        str | None,
    ]:
        """List runs and their replay progress."""
        _ = actor
        runs, cursor = await self._repository.query(run_filter)
        items = [(run, await self._repository.progress(run.id)) for run in runs]
        return items, cursor

    async def list_run_jobs(
        self,
        run_id: uuid.UUID,
        jobs_filter: ExperimentRunJobsFilter,
        actor: AuthContext,
    ) -> tuple[list[Job], str | None]:
        """List the jobs connected to a run through its replays."""
        _ = actor
        await self._repository.get(run_id)
        job_ids = await self._replay_repository.list_job_ids(run_id)
        return await self._job_repository.query_ids(
            job_ids,
            JobFilter(
                status=jobs_filter.status,
                cursor=jobs_filter.cursor,
                size=jobs_filter.size,
                sort=jobs_filter.sort,
            ),
        )

    async def cancel_run(
        self, run_id: uuid.UUID, actor: AuthContext
    ) -> tuple[ExperimentRun, ExperimentRunProgress]:
        """Request cancellation of every unsettled replay job."""
        run = await self._repository.get(run_id, exclusive=True)
        if run.status.terminal:
            return run, await self._repository.progress(run.id)
        run.cancel()
        await self._repository.update(run)
        for job_id in await self._replay_repository.list_job_ids(run.id):
            job = await self._job_repository.get(job_id)
            if not job.status.terminal:
                await self._job_service.cancel_job(job_id, actor)
        run = await self._repository.get(run.id)
        return run, await self._repository.progress(run.id)

    async def delete_run(self, run_id: uuid.UUID, actor: AuthContext) -> None:
        """Delete a settled run after explicitly deleting its jobs."""
        run = await self._repository.get(run_id)
        if not run.status.terminal:
            raise ConflictError(f"Experiment run {run.id} is still active")
        for job_id in await self._replay_repository.list_job_ids(run.id):
            await self._job_service.delete_job(job_id, actor)
        await self._repository.delete(run.id)
