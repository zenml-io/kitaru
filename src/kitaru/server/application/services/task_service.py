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
"""Task use cases."""

import json
import uuid
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from functools import partial
from typing import Any

from kitaru.api_models.v1.session import SessionStatus
from kitaru.api_models.v1.task import TaskKind, TaskStatus
from kitaru.server.application.interfaces.agent_version_repository import (
    AgentVersionRepository,
)
from kitaru.server.application.interfaces.blob_repository import BlobRepository
from kitaru.server.application.interfaces.plugin_repository import PluginRepository
from kitaru.server.application.interfaces.secret_repository import SecretRepository
from kitaru.server.application.interfaces.session_repository import SessionRepository
from kitaru.server.application.interfaces.task_repository import TaskRepository
from kitaru.server.application.interfaces.worker_repository import WorkerRepository
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.task import (
    ClaimedTask,
    TaskFilter,
    TaskPolicy,
    TaskUpdate,
)
from kitaru.server.application.services.agent_version_resolution import (
    resolve_agent_version,
)
from kitaru.server.application.services.task_transitions import TaskTransitions
from kitaru.server.domain.plugin import PluginVersion, ScriptPluginSource
from kitaru.server.domain.task import (
    AgentTask,
    AgentTaskDetails,
    EvaluationTask,
    EvaluationTaskDetails,
    IllegalTaskStatusTransition,
    ImportTask,
    ImportTaskDetails,
    PackagePluginSpec,
    PayloadSpec,
    PluginSpec,
    ScriptPluginSpec,
    Task,
    TaskResultSessionMissing,
    TaskResultSessionNotCompleted,
    TaskResultTooLarge,
    TaskRunSpec,
    TaskSpec,
    TaskUpdateRequiresStatus,
)


class TaskService:
    """Task use cases."""

    def __init__(
        self,
        repository: TaskRepository,
        worker_repository: WorkerRepository,
        session_repository: SessionRepository,
        agent_version_repository: AgentVersionRepository,
        plugin_repository: PluginRepository,
        blob_repository: BlobRepository,
        secret_repository: SecretRepository,
        transitions: TaskTransitions,
        policy: TaskPolicy,
    ) -> None:
        """Initialize the service.

        Args:
            repository: Task repository.
            worker_repository: Worker repository.
            session_repository: Session repository.
            agent_version_repository: Agent version repository.
            plugin_repository: Plugin repository.
            blob_repository: Blob repository.
            secret_repository: Secret repository.
            transitions: Task transition dispatch.
            policy: Task execution policy.
        """
        self._repository = repository
        self._workers = worker_repository
        self._sessions = session_repository
        self._agent_versions = agent_version_repository
        self._plugins = plugin_repository
        self._blobs = blob_repository
        self._secrets = secret_repository
        self._transitions = transitions
        self._policy = policy

    async def claim_tasks(
        self, worker_id: uuid.UUID, max_tasks: int, actor: AuthContext
    ) -> list[ClaimedTask]:
        """Sweep stale tasks, then claim pending tasks matching the worker's scope.

        The sweep runs first so a task whose worker died is back in the queue
        before this claim reads it.

        Args:
            worker_id: Id of the claiming worker.
            max_tasks: Maximum number of tasks to claim.
            actor: Caller context.

        Raises:
            WorkerNotFound: No worker has this id.

        Returns:
            Claimed tasks paired with their execution specs.
        """
        _ = actor
        worker = await self._workers.get(worker_id)
        now = datetime.now(UTC)
        await self._workers.update_last_seen_at(worker_id, now)
        await self._sweep_stale_tasks(now)
        claimed = await self._repository.claim_pending(
            worker.scope, worker_id, max_tasks, now
        )
        started_jobs: set[uuid.UUID] = set()
        results: list[ClaimedTask] = []
        for task in claimed:
            if task.job_id not in started_jobs:
                await self._transitions.start_job(task.job_id)
                started_jobs.add(task.job_id)
            results.append(ClaimedTask(task=task, spec=await self._build_spec(task)))
        return results

    async def heartbeat_worker(
        self, worker_id: uuid.UUID, task_ids: Sequence[uuid.UUID], actor: AuthContext
    ) -> list[uuid.UUID]:
        """Stamp the heartbeat on the tasks the caller still owns.

        A reported task the caller no longer owns, that no longer exists, that
        already reached a terminal status, or whose cancellation was
        requested comes back for the worker to stop.

        Args:
            worker_id: Id of the reporting worker.
            task_ids: Tasks the worker currently holds.
            actor: Caller context.

        Raises:
            WorkerNotFound: No worker has this id.

        Returns:
            Ids of the reported tasks the worker should stop running.
        """
        _ = actor
        now = datetime.now(UTC)
        await self._workers.update_last_seen_at(worker_id, now)
        stamped = await self._repository.stamp_heartbeats(task_ids, worker_id, now)
        cancel_task_ids: list[uuid.UUID] = []
        for task_id in task_ids:
            if task_id not in stamped or stamped[task_id] is not None:
                cancel_task_ids.append(task_id)
        return cancel_task_ids

    async def get_task(self, task_id: uuid.UUID, actor: AuthContext) -> Task:
        """Get a task by id, carrying its effective status.

        Args:
            task_id: Id of the task.
            actor: Caller context.

        Raises:
            TaskNotFound: No task has this id.

        Returns:
            Stored task.
        """
        _ = actor
        task = await self._repository.get(task_id)
        return self._with_staleness(task, datetime.now(UTC))

    async def list_tasks(
        self, task_filter: TaskFilter, actor: AuthContext
    ) -> tuple[list[Task], str | None]:
        """List tasks matching a filter, each carrying its effective status.

        Args:
            task_filter: Filter and pagination parameters.
            actor: Caller context.

        Returns:
            Page of matching tasks and the next cursor.
        """
        _ = actor
        tasks, next_cursor = await self._repository.query(task_filter)
        now = datetime.now(UTC)
        return [self._with_staleness(task, now) for task in tasks], next_cursor

    async def get_spec(self, task_id: uuid.UUID, actor: AuthContext) -> TaskSpec:
        """Get the execution spec of a task.

        Args:
            task_id: Id of the task.
            actor: Caller context.

        Raises:
            TaskNotFound: No task has this id.

        Returns:
            Execution spec.
        """
        _ = actor
        task = await self._repository.get(task_id)
        return await self._build_spec(task)

    async def update_task(
        self, task_id: uuid.UUID, command: TaskUpdate, actor: AuthContext
    ) -> Task:
        """Apply an executor transition, fenced by the claim's attempt.

        Args:
            task_id: Id of the task.
            command: Transition to apply, built from the request's set fields.
            actor: Caller context.

        Raises:
            TaskNotFound: No task has this id.
            TaskUpdateRequiresStatus: The command carries no status.
            TaskAttemptMismatch: The command is fenced by an attempt the task
                has moved past.
            TaskResultTooLarge: The completion result exceeds the size cap.
            IllegalTaskStatusTransition: The status is not one an executor
                writes, or the transition is not allowed from the task's
                current status.

        Returns:
            Task carrying its new status.
        """
        _ = actor
        task = await self._repository.get(task_id, exclusive=True)
        if command.status is None:
            raise TaskUpdateRequiresStatus(task_id)
        task.check_attempt(command.attempt)
        now = datetime.now(UTC)
        transition: Callable[[Task], None]
        if command.status is TaskStatus.RUNNING:
            transition = partial(Task.start, now=now)
        elif command.status is TaskStatus.COMPLETED:
            self._check_result_size(command.result)
            await self._check_result_session(task)
            transition = partial(Task.complete, result=command.result, now=now)
        elif command.status is TaskStatus.FAILED:
            self._check_result_size(command.result)
            transition = partial(
                Task.fail, error=command.error, result=command.result, now=now
            )
        elif command.status is TaskStatus.TIMED_OUT:
            transition = partial(Task.time_out, error=command.error, now=now)
        elif command.status is TaskStatus.CANCELED:
            transition = partial(Task.cancel, now=now)
        else:
            raise IllegalTaskStatusTransition(task_id, task.status, command.status)
        return await self._apply_status(task, transition)

    async def _apply_status(
        self, task: Task, transition: Callable[[Task], None]
    ) -> Task:
        """Route a task transition through the single status write point.

        Args:
            task: Task to transition.
            transition: Domain method application deciding the new status.

        Returns:
            Stored task carrying its new status.
        """
        return await self._transitions.apply_status(task, transition)

    def _with_staleness(self, task: Task, now: datetime) -> Task:
        """Return a task carrying the status the next sweep would write.

        Nothing sweeps while no worker claims, so a read reports what the
        stored row will become rather than the attempt that stopped
        reporting.

        Args:
            task: Stored task.
            now: Current time.

        Returns:
            Task carrying its effective status.
        """
        return task.with_staleness(
            now, self._policy.heartbeat_timeout_seconds, self._policy.retry_limit
        )

    def _check_result_size(self, result: Any) -> None:
        """Require a result to stay within the configured size cap.

        Args:
            result: Result the transition carries.

        Raises:
            TaskResultTooLarge: The encoded result exceeds the cap.
        """
        if result is None:
            return
        encoded = json.dumps(result).encode("utf-8")
        if len(encoded) > self._policy.max_result_bytes:
            raise TaskResultTooLarge(self._policy.max_result_bytes)

    async def _check_result_session(self, task: Task) -> None:
        """Require an agent task's linked result session to exist and be completed.

        Args:
            task: Task about to complete.

        Raises:
            TaskResultSessionMissing: No session is linked.
            TaskResultSessionNotCompleted: The linked session is still in
                progress or failed.
        """
        if not isinstance(task, AgentTask):
            return
        if task.result_session_id is None:
            raise TaskResultSessionMissing(task.id)
        session = await self._sessions.get(task.result_session_id)
        if session.status is not SessionStatus.COMPLETED:
            raise TaskResultSessionNotCompleted(task.id, session.id)

    async def _sweep_stale_tasks(self, now: datetime) -> None:
        """Settle or requeue in-flight tasks that stopped heartbeating.

        Args:
            now: Current time.
        """
        cutoff = now - timedelta(seconds=self._policy.heartbeat_timeout_seconds)
        stale = await self._repository.claim_stale(
            cutoff, self._policy.sweep_batch_limit
        )
        for task in stale:
            if task.cancel_requested_at is not None:
                await self._apply_status(task, partial(Task.cancel, now=now))
            elif task.attempt < self._policy.retry_limit:
                await self._unlink_result_session(task)
                await self._apply_status(task, Task.requeue)
            else:
                error = (
                    f"Task stopped reporting after {task.attempt} attempts "
                    "and was abandoned"
                )
                await self._apply_status(
                    task, partial(Task.abandon, error=error, now=now)
                )

    async def _unlink_result_session(self, task: Task) -> None:
        """Free the result session slot a requeued attempt left behind.

        Args:
            task: Task about to be requeued.
        """
        if task.result_session_id is None:
            return
        session = await self._sessions.get(task.result_session_id, exclusive=True)
        session.unlink_task()
        await self._sessions.update(session)

    async def _build_spec(self, task: Task) -> TaskSpec:
        """Build the execution spec of a task, dispatching on its kind.

        Args:
            task: Task to build the spec for.

        Raises:
            ValueError: The task is not one of the three known kinds.

        Returns:
            Execution spec.
        """
        if isinstance(task, AgentTask):
            return await self._agent_spec(task)
        if isinstance(task, EvaluationTask):
            return await self._evaluation_spec(task)
        if isinstance(task, ImportTask):
            return await self._import_spec(task)
        raise ValueError(f"Task {task.id} has no spec builder")

    async def _agent_spec(self, task: AgentTask) -> TaskSpec:
        """Build the execution spec of an agent task.

        Args:
            task: Agent task.

        Raises:
            AgentVersionNotFound: The task names an unknown agent version.
            AgentVersionWithoutRunSpec: The agent version carries no run spec.
            SecretNotFound: The run spec names an unknown secret.

        Returns:
            Execution spec.
        """
        agent_version = await resolve_agent_version(
            task.agent_version_id, self._agent_versions
        )
        run_spec = agent_version.run_spec
        assert run_spec is not None
        secret_env: dict[str, str] = {}
        for secret_id in run_spec.secret_ids:
            secret = await self._secrets.get(secret_id)
            for key, value in secret.values.items():
                secret_env[key] = value.get_secret_value()
        return TaskSpec(
            task_id=task.id,
            kind=TaskKind.AGENT,
            timeout_seconds=run_spec.timeout_seconds,
            run_spec=TaskRunSpec(
                command=run_spec.command,
                working_dir=run_spec.working_dir,
                env=run_spec.env,
            ),
            env=task.env,
            secret_env=secret_env,
            details=AgentTaskDetails(inputs=task.inputs),
        )

    async def _evaluation_spec(self, task: EvaluationTask) -> TaskSpec:
        """Build the execution spec of an evaluator task.

        Args:
            task: Evaluation task.

        Raises:
            PluginVersionIdNotFound: The task names an unknown plugin version.
            BlobNotFound: The script plugin names an unknown blob.

        Returns:
            Execution spec.
        """
        plugin_version = await self._plugins.get_version_by_id(task.plugin_version_id)
        plugin = await self._plugins.get(plugin_version.plugin_id)
        return TaskSpec(
            task_id=task.id,
            kind=TaskKind.EVALUATOR,
            timeout_seconds=self._policy.evaluator_timeout_seconds,
            env=task.env,
            details=EvaluationTaskDetails(
                evaluator_name=plugin.name,
                params=task.params,
                plugin=await self._plugin_spec(plugin_version),
                input_session_id=task.input_session_id,
            ),
        )

    async def _import_spec(self, task: ImportTask) -> TaskSpec:
        """Build the execution spec of an importer task.

        Args:
            task: Import task.

        Raises:
            PluginVersionIdNotFound: The task names an unknown plugin version.
            BlobNotFound: The script plugin or the payload names an unknown
                blob.

        Returns:
            Execution spec.
        """
        plugin_version = await self._plugins.get_version_by_id(task.plugin_version_id)
        plugin = await self._plugins.get(plugin_version.plugin_id)
        payload = await self._blobs.get_metadata(task.payload_blob_id)
        return TaskSpec(
            task_id=task.id,
            kind=TaskKind.IMPORTER,
            timeout_seconds=self._policy.importer_timeout_seconds,
            env=task.env,
            details=ImportTaskDetails(
                plugin=await self._plugin_spec(plugin_version),
                payload=PayloadSpec(blob_id=payload.id, sha256=payload.sha256),
                provider=plugin.provider,
                agent_id=task.agent_id,
                params=task.params,
            ),
        )

    async def _plugin_spec(self, plugin_version: PluginVersion) -> PluginSpec:
        """Convert a plugin version's code source into its spec form.

        Args:
            plugin_version: Plugin version holding the code source.

        Raises:
            BlobNotFound: The script source names an unknown blob.

        Returns:
            Plugin spec the task process loads its code from.
        """
        source = plugin_version.source
        if isinstance(source, ScriptPluginSource):
            blob = await self._blobs.get_metadata(source.blob_id)
            return ScriptPluginSpec(
                entrypoint=source.entrypoint, blob_id=blob.id, sha256=blob.sha256
            )
        return PackagePluginSpec(
            entrypoint=source.entrypoint, requirement=source.requirement
        )
