"""Job composition, cancellation, and settlement."""

import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from kitaru.server.application.agent_version_resolution import (
    resolve_agent_version,
)
from kitaru.server.application.evaluator_resolution import validate_evaluators
from kitaru.server.application.events import EventRegistry, JobSettled
from kitaru.server.application.interfaces.agent_repository import (
    AgentRepository,
    AgentVersionRepository,
)
from kitaru.server.application.interfaces.blob_repository import BlobRepository
from kitaru.server.application.interfaces.job_repository import JobRepository
from kitaru.server.application.interfaces.plugin_repository import PluginRepository
from kitaru.server.application.interfaces.session_repository import SessionRepository
from kitaru.server.application.interfaces.task_repository import TaskRepository
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.job import (
    EvaluationBatchCreate,
    ImportCreate,
    JobFilter,
    JobTasksFilter,
    SessionRunCreate,
)
from kitaru.server.application.models.task import TaskFilter
from kitaru.server.application.plugin_resolution import (
    resolve_plugin,
    resolve_plugin_version,
)
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.job import Job, JobSettledConflict, JobStatus
from kitaru.server.domain.plugin import PluginKind
from kitaru.server.domain.task import (
    AgentTask,
    EvaluationTask,
    ImportTask,
    Task,
    TaskOnFailure,
    TaskStatus,
)

if TYPE_CHECKING:
    from kitaru.server.application.services.task_service import TaskService


class JobService:
    """Job reads, composition, cancellation, and task-driven settlement."""

    def __init__(
        self,
        job_repository: JobRepository,
        task_repository: TaskRepository,
        agent_repository: AgentRepository,
        agent_version_repository: AgentVersionRepository,
        plugin_repository: PluginRepository,
        blob_repository: BlobRepository,
        session_repository: SessionRepository,
        events: EventRegistry,
        *,
        evaluation_pair_limit: int = 100,
        task_service: "TaskService | None" = None,
    ) -> None:
        self._jobs = job_repository
        self._tasks = task_repository
        self._agents = agent_repository
        self._agent_versions = agent_version_repository
        self._plugins = plugin_repository
        self._blobs = blob_repository
        self._sessions = session_repository
        self._events = events
        self._evaluation_pair_limit = evaluation_pair_limit
        self._task_service = task_service

    def set_task_service(self, task_service: "TaskService") -> None:
        """Complete the circular job/task service wiring."""
        self._task_service = task_service

    async def get_job(self, job_id: uuid.UUID, actor: AuthContext) -> Job:
        """Get a job."""
        _ = actor
        return await self._jobs.get(job_id)

    async def list_jobs(
        self, job_filter: JobFilter, actor: AuthContext
    ) -> tuple[list[Job], str | None]:
        """List jobs."""
        _ = actor
        return await self._jobs.query(job_filter)

    async def list_job_tasks(
        self, task_filter: JobTasksFilter, actor: AuthContext
    ) -> tuple[list[Task], str | None]:
        """List tasks belonging to a job."""
        _ = actor
        return await self._tasks.query(
            TaskFilter(
                job_id=task_filter.job_id,
                kind=task_filter.kind,
                status=task_filter.status,
                cursor=task_filter.cursor,
                size=task_filter.size,
                sort=task_filter.sort,
            )
        )

    async def cancel_job(self, job_id: uuid.UUID, actor: AuthContext) -> Job:
        """Request cancellation of every nonterminal task in a job."""
        _ = actor
        return await self.request_cancel(job_id)

    async def request_cancel(self, job_id: uuid.UUID) -> Job:
        """Internal cancellation path shared by jobs and run propagation."""
        if self._task_service is None:
            raise RuntimeError("JobService is not wired to TaskService")
        now = datetime.now(UTC)
        job = await self._jobs.get(job_id, exclusive=True)
        if job.status.terminal:
            return job
        job.request_cancel(now)
        job = await self._jobs.update(job)
        tasks = await self._tasks.list_job_tasks(job_id, exclusive=True)
        for task in tasks:
            if not task.status.terminal:
                await self._task_service.request_cancel(task, now)
        return await self._jobs.get(job_id)

    async def delete_job(self, job_id: uuid.UUID, actor: AuthContext) -> None:
        """Delete a job and cascading tasks."""
        _ = actor
        await self._jobs.delete(job_id)

    async def create_session_run(
        self, command: SessionRunCreate, actor: AuthContext
    ) -> Job:
        """Create one job containing one agent task."""
        version = await resolve_agent_version(
            command.agent_version_id, self._agent_versions
        )
        job = await self.create_job(actor.account.id)
        env = {"KITARU_SESSION_NAME": command.name} if command.name is not None else {}
        await self.add_task(
            job.id,
            AgentTask(
                job_id=job.id,
                agent_version_id=version.id,
                inputs=command.inputs,
                labels={"agent_version": str(version.id)},
                env=env,
            ),
        )
        return job

    async def create_import(self, command: ImportCreate, actor: AuthContext) -> Job:
        """Create one job containing one importer task."""
        await self._agents.get(command.agent_id)
        await self._blobs.get(command.payload_blob_id)
        plugin = await resolve_plugin(
            command.importer, PluginKind.IMPORTER, self._plugins
        )
        version = await resolve_plugin_version(plugin, command.version, self._plugins)
        job = await self.create_job(actor.account.id)
        await self.add_task(
            job.id,
            ImportTask(
                job_id=job.id,
                plugin_version_id=version.id,
                payload_blob_id=command.payload_blob_id,
                agent_id=command.agent_id,
                params=command.params,
            ),
        )
        return job

    async def create_evaluations(
        self, command: EvaluationBatchCreate, actor: AuthContext
    ) -> Job:
        """Create one continue-on-failure evaluator task per input/config pair."""
        pair_count = len(command.input_session_ids) * len(command.evaluators)
        if not command.input_session_ids or not command.evaluators:
            raise ValidationError("Evaluation inputs and evaluators must be non-empty")
        if len(set(command.input_session_ids)) != len(command.input_session_ids):
            raise ValidationError("Evaluation input sessions must be unique")
        if pair_count > self._evaluation_pair_limit:
            raise ValidationError(
                f"Evaluation batch exceeds {self._evaluation_pair_limit} pairs"
            )
        for session_id in command.input_session_ids:
            await self._sessions.get(session_id)
        evaluators = await validate_evaluators(command.evaluators, self._plugins)
        job = await self.create_job(actor.account.id)
        for session_id in command.input_session_ids:
            for config in evaluators:
                assert config.evaluator_version_id is not None
                await self.add_task(
                    job.id,
                    EvaluationTask(
                        job_id=job.id,
                        plugin_version_id=config.evaluator_version_id,
                        input_session_id=session_id,
                        params=config.params,
                        on_failure=TaskOnFailure.CONTINUE,
                    ),
                )
        return job

    async def create_job(self, owner_id: uuid.UUID) -> Job:
        """Create an empty internal job."""
        return await self._jobs.create(Job(owner_id=owner_id))

    async def add_task(self, job_id: uuid.UUID, task: Task) -> Task:
        """Append a task after locking and checking the job."""
        job = await self._jobs.get(job_id, exclusive=True)
        if job.status.terminal:
            raise JobSettledConflict(job_id)
        if task.job_id != job_id:
            raise ValidationError("Task job id does not match the target job")
        return await self._tasks.create(task)

    async def start_job(self, job_id: uuid.UUID) -> Job:
        """Start a job when its first task is claimed."""
        job = await self._jobs.get(job_id, exclusive=True)
        if job.status is JobStatus.PENDING:
            job.start()
            return await self._jobs.update(job)
        return job

    async def advance_job(self, terminal_task: Task) -> Job | None:
        """Propagate abort failure and settle a drained job exactly once."""
        if self._task_service is None:
            raise RuntimeError("JobService is not wired to TaskService")
        if (
            terminal_task.status.hard_failure
            and terminal_task.on_failure is TaskOnFailure.ABORT
        ):
            now = datetime.now(UTC)
            siblings = await self._tasks.list_job_tasks(
                terminal_task.job_id, exclusive=True
            )
            for sibling in siblings:
                if sibling.id != terminal_task.id and not sibling.status.terminal:
                    await self._task_service.request_cancel(sibling, now)

        job = await self._jobs.get(terminal_task.job_id, exclusive=True)
        if job.status.terminal:
            return job
        tasks = await self._tasks.list_job_tasks(job.id, exclusive=True)
        if any(not task.status.terminal for task in tasks):
            return None
        counted_failures = [
            task
            for task in tasks
            if task.status.hard_failure and task.on_failure is not TaskOnFailure.IGNORE
        ]
        if counted_failures:
            job.settle(JobStatus.FAILED, error=counted_failures[0].error)
        elif any(task.status is TaskStatus.CANCELED for task in tasks):
            job.settle(JobStatus.CANCELED)
        else:
            job.settle(JobStatus.COMPLETED)
        stored = await self._jobs.update(job)
        await self._events.dispatch(JobSettled(stored))
        return stored
