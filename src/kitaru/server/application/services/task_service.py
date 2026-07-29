"""Task claiming, specifications, heartbeats, and transitions."""

import json
import uuid
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from kitaru.server.application.agent_version_resolution import (
    get_agent_run_spec,
    resolve_agent_version,
)
from kitaru.server.application.events import EventRegistry, TaskTerminal
from kitaru.server.application.interfaces.agent_repository import (
    AgentVersionRepository,
)
from kitaru.server.application.interfaces.blob_repository import BlobRepository
from kitaru.server.application.interfaces.plugin_repository import PluginRepository
from kitaru.server.application.interfaces.secret_repository import SecretRepository
from kitaru.server.application.interfaces.session_repository import SessionRepository
from kitaru.server.application.interfaces.task_repository import TaskRepository
from kitaru.server.application.interfaces.worker_repository import WorkerRepository
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.task import TaskFilter, TaskUpdate
from kitaru.server.domain.agent_version import AgentVersion
from kitaru.server.domain.base import PayloadTooLargeError, ValidationError
from kitaru.server.domain.plugin import (
    PackagePluginSource,
    Plugin,
    PluginVersion,
    ScriptPluginSource,
)
from kitaru.server.domain.session import SessionStatus
from kitaru.server.domain.task import (
    AgentTask,
    AgentTaskDetails,
    EvaluationTask,
    EvaluationTaskDetails,
    ImportTask,
    ImportTaskDetails,
    PackagePluginSpec,
    PayloadSpec,
    ScriptPluginSpec,
    Task,
    TaskRunSpec,
    TaskSpec,
    TaskStatus,
)

if TYPE_CHECKING:
    from kitaru.server.application.services.job_service import JobService


class TaskService:
    """Task queue execution protocol."""

    def __init__(
        self,
        task_repository: TaskRepository,
        worker_repository: WorkerRepository,
        agent_version_repository: AgentVersionRepository,
        plugin_repository: PluginRepository,
        blob_repository: BlobRepository,
        secret_repository: SecretRepository,
        session_repository: SessionRepository,
        events: EventRegistry,
        *,
        heartbeat_timeout_seconds: float = 60,
        retry_cap: int = 3,
        sweep_limit: int = 100,
        evaluator_timeout_seconds: int = 300,
        importer_timeout_seconds: int = 600,
        max_result_bytes: int = 1024 * 1024,
        job_service: "JobService | None" = None,
    ) -> None:
        self._tasks = task_repository
        self._workers = worker_repository
        self._agent_versions = agent_version_repository
        self._plugins = plugin_repository
        self._blobs = blob_repository
        self._secrets = secret_repository
        self._sessions = session_repository
        self._events = events
        self._heartbeat_timeout_seconds = heartbeat_timeout_seconds
        self._retry_cap = retry_cap
        self._sweep_limit = sweep_limit
        self._evaluator_timeout_seconds = evaluator_timeout_seconds
        self._importer_timeout_seconds = importer_timeout_seconds
        self._max_result_bytes = max_result_bytes
        self._job_service = job_service

    def set_job_service(self, job_service: "JobService") -> None:
        """Complete the circular task/job service wiring."""
        self._job_service = job_service

    async def claim_tasks(
        self,
        worker_id: uuid.UUID,
        max_tasks: int,
        actor: AuthContext,
    ) -> list[tuple[Task, TaskSpec]]:
        """Sweep stale attempts, claim to a worker's stored scope, and build specs."""
        _ = actor
        now = datetime.now(UTC)
        await self._sweep_stale(now)
        worker = await self._workers.get(worker_id)
        worker.refresh(now=now)
        await self._workers.update(worker)
        tasks = await self._tasks.claim_pending(worker_id, worker.scope, max_tasks)
        if self._job_service is None:
            raise RuntimeError("TaskService is not wired to JobService")
        for job_id in {task.job_id for task in tasks}:
            await self._job_service.start_job(job_id)
        return [(task, await self._build_spec(task)) for task in tasks]

    async def heartbeat_worker(
        self,
        worker_id: uuid.UUID,
        task_ids: list[uuid.UUID],
        actor: AuthContext,
    ) -> list[uuid.UUID]:
        """Refresh worker/task liveness and return task ids that must cancel."""
        _ = actor
        now = datetime.now(UTC)
        worker = await self._workers.get(worker_id)
        worker.refresh(now=now)
        await self._workers.update(worker)
        return await self._tasks.heartbeat(worker_id, task_ids, now)

    async def get_task(self, task_id: uuid.UUID, actor: AuthContext) -> Task:
        """Get a task with stale attempts represented by effective status."""
        _ = actor
        task = await self._tasks.get(task_id)
        return task.with_staleness(self._stale_before(), self._retry_cap)

    async def list_tasks(
        self, task_filter: TaskFilter, actor: AuthContext
    ) -> tuple[list[Task], str | None]:
        """List tasks with effective stale statuses."""
        _ = actor
        tasks, cursor = await self._tasks.query(task_filter)
        cutoff = self._stale_before()
        return (
            [task.with_staleness(cutoff, self._retry_cap) for task in tasks],
            cursor,
        )

    async def get_spec(self, task_id: uuid.UUID, actor: AuthContext) -> TaskSpec:
        """Build the current full task specification."""
        _ = actor
        return await self._build_spec(await self._tasks.get(task_id))

    async def update_task(
        self,
        task_id: uuid.UUID,
        update: TaskUpdate,
        actor: AuthContext,
    ) -> Task:
        """Apply one attempt-fenced executor transition."""
        _ = actor
        if update.status is None or update.attempt is None:
            raise ValidationError("Task status and attempt are required")
        if (
            update.result is not None
            and len(
                json.dumps(
                    update.result, separators=(",", ":"), ensure_ascii=False
                ).encode()
            )
            > self._max_result_bytes
        ):
            raise PayloadTooLargeError(
                f"Task result exceeds the {self._max_result_bytes} byte limit"
            )
        task = await self._tasks.get(task_id, exclusive=True)
        if update.status is TaskStatus.COMPLETED and isinstance(task, AgentTask):
            await self._check_result_session(task)
        return await self._apply_status(
            task,
            update.status,
            attempt=update.attempt,
            error=update.error,
            result=update.result,
        )

    async def request_cancel(self, task: Task, now: datetime) -> Task:
        """Apply server-driven cancellation request to a locked task."""
        previous = task.status
        task.request_cancel(now)
        stored = await self._tasks.update(task)
        if not previous.terminal and stored.status.terminal:
            await self._after_terminal(stored, previous)
        return stored

    async def _sweep_stale(self, now: datetime) -> None:
        stale = await self._tasks.stale(
            now - timedelta(seconds=self._heartbeat_timeout_seconds),
            self._sweep_limit,
        )
        for task in stale:
            if task.cancel_requested_at is not None:
                await self._apply_status(
                    task, TaskStatus.CANCELED, attempt=task.attempt
                )
            elif task.attempt >= self._retry_cap:
                await self._apply_status(task, TaskStatus.ABANDONED)
            else:
                if task.result_session_id is not None:
                    await self._sessions.unlink_task(task.id)
                task.requeue()
                await self._tasks.update(task, expected_attempt=task.attempt)

    async def _apply_status(
        self,
        task: Task,
        status: TaskStatus,
        *,
        attempt: int | None = None,
        error: str | None = None,
        result: Any = None,
    ) -> Task:
        previous = task.status
        if status is TaskStatus.RUNNING:
            task.start(self._require_attempt(attempt))
        elif status is TaskStatus.COMPLETED:
            task.complete(self._require_attempt(attempt), result=result)
        elif status is TaskStatus.FAILED:
            task.fail(self._require_attempt(attempt), error=error, result=result)
        elif status is TaskStatus.TIMED_OUT:
            task.time_out(self._require_attempt(attempt), error=error)
        elif status is TaskStatus.CANCELED:
            task.cancel(self._require_attempt(attempt))
        elif status is TaskStatus.ABANDONED:
            task.abandon()
        else:
            raise ValidationError(f"Task status {status} is not executor-writable")
        stored = await self._tasks.update(task, expected_attempt=attempt)
        if stored.status.terminal:
            await self._after_terminal(stored, previous)
        return stored

    async def _after_terminal(self, task: Task, previous_status: TaskStatus) -> None:
        await self._events.dispatch(TaskTerminal(task, previous_status))
        if self._job_service is None:
            raise RuntimeError("TaskService is not wired to JobService")
        await self._job_service.advance_job(task)

    async def _check_result_session(self, task: AgentTask) -> None:
        if task.result_session_id is None:
            return
        session = await self._sessions.get(task.result_session_id)
        if session.status is not SessionStatus.COMPLETED:
            from kitaru.server.domain.task import InvalidTaskResult

            raise InvalidTaskResult(
                f"Result session {session.id} is {session.status.value}, not completed."
            )

    async def _build_spec(self, task: Task) -> TaskSpec:
        if isinstance(task, AgentTask):
            return await self._agent_spec(task)
        if isinstance(task, EvaluationTask):
            return await self._evaluation_spec(task)
        if isinstance(task, ImportTask):
            return await self._import_spec(task)
        raise TypeError(f"Unsupported task type: {type(task).__name__}")

    async def _agent_spec(self, task: AgentTask) -> TaskSpec:
        version = await resolve_agent_version(
            task.agent_version_id, self._agent_versions
        )
        secret_env = await self._resolve_secrets(version)
        run = get_agent_run_spec(version)
        return TaskSpec(
            task_id=task.id,
            kind=task.kind,
            timeout_seconds=run.timeout_seconds,
            run=TaskRunSpec(
                command=run.command,
                working_dir=run.working_dir,
                env=run.env,
            ),
            env=task.env,
            secret_env=secret_env,
            details=AgentTaskDetails(inputs=task.inputs),
        )

    async def _evaluation_spec(self, task: EvaluationTask) -> TaskSpec:
        plugin, version = await self._get_plugin_version(task.plugin_version_id)
        return TaskSpec(
            task_id=task.id,
            kind=task.kind,
            timeout_seconds=self._evaluator_timeout_seconds,
            env=task.env,
            details=EvaluationTaskDetails(
                evaluator_name=str(plugin.name),
                params=task.params,
                plugin=await self._plugin_spec(version),
                input_session_id=task.input_session_id,
            ),
        )

    async def _import_spec(self, task: ImportTask) -> TaskSpec:
        plugin, version = await self._get_plugin_version(task.plugin_version_id)
        payload = await self._blobs.get(task.payload_blob_id)
        return TaskSpec(
            task_id=task.id,
            kind=task.kind,
            timeout_seconds=self._importer_timeout_seconds,
            env=task.env,
            details=ImportTaskDetails(
                plugin=await self._plugin_spec(version),
                payload=PayloadSpec(blob_id=payload.id, sha256=payload.sha256),
                provider=plugin.provider,
                agent_id=task.agent_id,
                params=task.params,
            ),
        )

    async def _get_plugin_version(
        self, version_id: uuid.UUID
    ) -> tuple[Plugin, PluginVersion]:
        version = await self._plugins.get_version(version_id)
        return await self._plugins.get(version.plugin_id), version

    async def _plugin_spec(
        self, version: PluginVersion
    ) -> ScriptPluginSpec | PackagePluginSpec:
        source = version.source
        if isinstance(source, ScriptPluginSource):
            blob = await self._blobs.get(source.blob_id)
            return ScriptPluginSpec(
                entrypoint=source.entrypoint,
                blob_id=blob.id,
                sha256=blob.sha256,
            )
        if isinstance(source, PackagePluginSource):
            return PackagePluginSpec(
                entrypoint=source.entrypoint,
                requirement=source.requirement,
            )
        raise TypeError(f"Unsupported plugin source: {type(source).__name__}")

    async def _resolve_secrets(self, version: AgentVersion) -> dict[str, str]:
        env: dict[str, str] = {}
        assert version.run_spec is not None
        for secret_id in version.run_spec.secret_ids:
            secret = await self._secrets.get(secret_id)
            env.update(
                {key: value.get_secret_value() for key, value in secret.values.items()}
            )
        return env

    def _stale_before(self) -> datetime:
        return datetime.now(UTC) - timedelta(seconds=self._heartbeat_timeout_seconds)

    @staticmethod
    def _require_attempt(attempt: int | None) -> int:
        if attempt is None:
            raise ValidationError("Task attempt is required")
        return attempt
