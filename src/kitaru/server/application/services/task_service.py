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
from kitaru.api_models.v1.task import TaskStatus
from kitaru.server.application.interfaces.job_repository import JobRepository
from kitaru.server.application.interfaces.session_repository import SessionRepository
from kitaru.server.application.interfaces.task_repository import TaskRepository
from kitaru.server.application.interfaces.worker_repository import WorkerRepository
from kitaru.server.application.models.auth import (
    AuthContext,
    TaskAuthContext,
    TaskPrincipal,
    WorkerAuthContext,
    WorkerPrincipal,
)
from kitaru.server.application.models.task import (
    ClaimedTask,
    TaskFilter,
    TaskPolicy,
    TaskUpdate,
)
from kitaru.server.application.services.resource_access import check_task_attempt
from kitaru.server.application.services.task_spec import TaskSpecBuilder
from kitaru.server.application.services.task_transitions import TaskTransitions
from kitaru.server.domain.task import (
    AgentTask,
    IllegalTaskStatusTransition,
    ImportWaitTask,
    Task,
    TaskAccessDenied,
    TaskResultSessionMissing,
    TaskResultSessionNotCompleted,
    TaskResultTooLarge,
    TaskSpec,
    TaskUpdateRequiresStatus,
)
from kitaru.server.domain.worker import WorkerAccessDenied, WorkerCredentialRequired


class TaskService:
    """Task use cases."""

    def __init__(
        self,
        repository: TaskRepository,
        worker_repository: WorkerRepository,
        session_repository: SessionRepository,
        job_repository: JobRepository,
        spec_builder: TaskSpecBuilder,
        transitions: TaskTransitions,
        policy: TaskPolicy,
    ) -> None:
        """Initialize the service.

        Args:
            repository: Task repository.
            worker_repository: Worker repository.
            session_repository: Session repository.
            job_repository: Job repository.
            spec_builder: Task execution spec builder.
            transitions: Task transition dispatch.
            policy: Task execution policy.
        """
        self._repository = repository
        self._workers = worker_repository
        self._sessions = session_repository
        self._jobs = job_repository
        self._spec_builder = spec_builder
        self._transitions = transitions
        self._policy = policy

    async def claim_tasks(
        self, max_tasks: int, actor: WorkerAuthContext
    ) -> list[ClaimedTask]:
        """Claim pending tasks matching the worker's scope.

        Args:
            max_tasks: Maximum number of tasks to claim.
            actor: Caller context.

        Raises:
            WorkerCredentialRequired: The caller holds no worker token.
            WorkerNotFound: No worker has the claiming principal's id.

        Returns:
            Claimed tasks paired with their execution specs and job owners.
        """
        if not isinstance(actor.principal, WorkerPrincipal):
            raise WorkerCredentialRequired()
        worker_id = actor.principal.worker_id
        worker = await self._workers.get(worker_id)
        now = datetime.now(UTC)
        await self._workers.update_last_seen_at(worker_id, now)
        claimed = await self._repository.claim_pending(
            worker.scope, worker_id, max_tasks, now
        )
        job_ids = sorted({task.job_id for task in claimed})
        owners = await self._jobs.get_many(job_ids)
        # Start the jobs in ascending id order so this transaction locks job
        # rows in the order the cancellation path locks them in.
        for job_id in job_ids:
            await self._transitions.start_job(job_id)
        results: list[ClaimedTask] = []
        for task in claimed:
            results.append(
                ClaimedTask(
                    task=task,
                    spec=await self._spec_builder.build_spec(task),
                    job_owner_id=owners[task.job_id].owner_id,
                )
            )
        return results

    async def heartbeat_worker(
        self,
        worker_id: uuid.UUID,
        task_ids: Sequence[uuid.UUID],
        actor: WorkerAuthContext,
    ) -> list[uuid.UUID]:
        """Stamp the heartbeat on the tasks the caller still owns.

        A reported task the caller no longer owns, that no longer exists, that
        already reached a terminal status, or whose cancellation was requested
        on the task or on its job comes back for the worker to stop. A task
        the caller owns and is in flight but whose row a settlement
        transaction holds locked keeps running, unstamped for this tick.

        Args:
            worker_id: Id of the reporting worker.
            task_ids: Tasks the worker currently holds.
            actor: Caller context.

        Raises:
            WorkerAccessDenied: The caller's worker token does not name this
                worker.
            WorkerNotFound: No worker has this id.

        Returns:
            Ids of the reported tasks the worker should stop running.
        """
        if (
            not isinstance(actor.principal, WorkerPrincipal)
            or actor.principal.worker_id != worker_id
        ):
            raise WorkerAccessDenied(worker_id)
        now = datetime.now(UTC)
        await self._workers.update_last_seen_at(worker_id, now)
        stamped, skipped = await self._repository.stamp_heartbeats(
            task_ids, worker_id, now
        )
        cancel_task_ids: list[uuid.UUID] = []
        for task_id in task_ids:
            if task_id in skipped:
                continue
            if task_id not in stamped or stamped[task_id] is not None:
                cancel_task_ids.append(task_id)
        return cancel_task_ids

    async def get_task(self, task_id: uuid.UUID, actor: AuthContext) -> Task:
        """Get a task by id, carrying its effective status.

        An account principal reads any task. A task principal reads only its
        own task.

        Args:
            task_id: Id of the task.
            actor: Caller context.

        Raises:
            TaskAccessDenied: The caller's task token names a different task.
            TaskNotFound: No task has this id.

        Returns:
            Stored task.
        """
        if isinstance(actor.principal, TaskPrincipal) and (
            actor.principal.task_id != task_id
        ):
            raise TaskAccessDenied(task_id)
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

        An account principal reads any spec. A task principal reads only its
        own task's spec.

        Args:
            task_id: Id of the task.
            actor: Caller context.

        Raises:
            TaskAccessDenied: The caller's task token names a different task.
            TaskNotFound: No task has this id.

        Returns:
            Execution spec.
        """
        if isinstance(actor.principal, TaskPrincipal) and (
            actor.principal.task_id != task_id
        ):
            raise TaskAccessDenied(task_id)
        await check_task_attempt(actor, self._repository)
        task = await self._repository.get(task_id)
        return await self._spec_builder.build_spec(task)

    async def update_task(
        self, task_id: uuid.UUID, command: TaskUpdate, actor: TaskAuthContext
    ) -> Task:
        """Apply an executor transition, fenced by the caller's claimed attempt.

        Args:
            task_id: Id of the task.
            command: Transition to apply, built from the request's set fields.
            actor: Caller context.

        Raises:
            TaskAccessDenied: The caller holds no task token, or its task
                token names a different task.
            TaskNotFound: No task has this id.
            TaskUpdateRequiresStatus: The command carries no status.
            TaskAttemptMismatch: The token is fenced by an attempt the task
                has moved past.
            TaskResultTooLarge: The completion result exceeds the size cap.
            IllegalTaskStatusTransition: The status is not one an executor
                writes, or the transition is not allowed from the task's
                current status.

        Returns:
            Task carrying its new status.
        """
        if not isinstance(actor.principal, TaskPrincipal) or (
            actor.principal.task_id != task_id
        ):
            raise TaskAccessDenied(task_id)
        task = await self._repository.get(task_id, exclusive=True)
        if command.status is None:
            raise TaskUpdateRequiresStatus(task_id)
        task.check_attempt(actor.principal.attempt)
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

    async def list_stale_task_ids(self, now: datetime) -> list[uuid.UUID]:
        """Read the ids of in-flight tasks that stopped heartbeating.

        Takes no lock.

        Args:
            now: Current time.

        Returns:
            Ids of the stale tasks in ascending order.
        """
        cutoff = now - timedelta(seconds=self._policy.heartbeat_timeout_seconds)
        return await self._repository.list_stale_ids(
            cutoff, self._policy.sweep_batch_limit
        )

    async def sweep_stale_task(self, task_id: uuid.UUID, now: datetime) -> None:
        """Settle or requeue one in-flight task that stopped heartbeating.

        Locks the task row, then the result session row a requeue frees, then
        the job row. A task another sweep holds, or one that resumed
        reporting, is left alone.

        Args:
            task_id: Id of the candidate task.
            now: Current time.
        """
        cutoff = now - timedelta(seconds=self._policy.heartbeat_timeout_seconds)
        task = await self._repository.claim_stale(task_id, cutoff)
        if task is None:
            return
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
            await self._apply_status(task, partial(Task.abandon, error=error, now=now))

    async def list_expired_import_wait_ids(self, now: datetime) -> list[uuid.UUID]:
        """Read the ids of pending import wait tasks past their import deadline.

        Takes no lock.

        Args:
            now: Current time.

        Returns:
            Ids of the expired tasks in ascending order.
        """
        return await self._repository.list_expired_import_wait_ids(
            now, self._policy.sweep_batch_limit
        )

    async def sweep_expired_import_wait(
        self, task_id: uuid.UUID, now: datetime
    ) -> None:
        """Fail one pending import wait task whose import deadline passed.

        Locks the task row, then the job row. A task another sweep holds, or
        one that completed in the meantime, is left alone.

        Args:
            task_id: Id of the candidate task.
            now: Current time.
        """
        task = await self._repository.claim_expired_import_wait(task_id, now)
        if not isinstance(task, ImportWaitTask):
            return
        error = f"No import arrived within {task.import_deadline_seconds} seconds"

        def transition(candidate: Task) -> None:
            assert isinstance(candidate, ImportWaitTask)
            candidate.fail_pending(error, now)

        await self._apply_status(task, transition)

    async def list_unpropagated_cancel_job_ids(self) -> list[uuid.UUID]:
        """Read the ids of canceling jobs whose live tasks still owe the stamp.

        Takes no lock.

        Returns:
            Ids of the canceling jobs in ascending order.
        """
        return await self._jobs.list_unpropagated_cancel_ids(
            self._policy.sweep_batch_limit
        )

    async def propagate_job_cancel(self, job_id: uuid.UUID) -> None:
        """Carry a job's cancel request to its live tasks and settle it if drained.

        Locks the job's live task rows in one id-ordered acquisition, then
        the job row.

        Args:
            job_id: Id of the job.

        Raises:
            DBAPIError: Another transaction holds one of the task rows.
        """
        await self._transitions.request_jobs_cancel([job_id], nowait=True)
        await self._transitions.settle_job_if_drained(job_id)

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

        A read between the heartbeat timeout and the sweep tick that acts on
        it reports what the stored row will become rather than the attempt
        that stopped reporting.

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

        A pending-import result session also passes, the placeholder a
        trigger-mode task hands off to the import.

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
        if session.status not in (
            SessionStatus.COMPLETED,
            SessionStatus.PENDING_IMPORT,
        ):
            raise TaskResultSessionNotCompleted(task.id, session.id)

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
