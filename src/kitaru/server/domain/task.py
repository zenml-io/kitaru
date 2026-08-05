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
"""Task entities, spec value objects, and errors."""

import uuid
from datetime import datetime, timedelta
from typing import Annotated, Any, Literal, Self

import pydantic
from pydantic import Field, field_validator

from kitaru.api_models.v1.evaluation import EvaluationResult
from kitaru.api_models.v1.task import (
    TaskKind,
    TaskOnFailure,
    TaskStatus,
)
from kitaru.base import FrozenModel
from kitaru.server.domain.base import (
    ConflictError,
    DomainModel,
    ForbiddenError,
    NotFoundError,
    PayloadTooLargeError,
    ValidationError,
)
from kitaru.server.domain.ids import uuid7

__all__ = [
    "AgentTask",
    "AgentTaskDetails",
    "DuplicateEvaluationTask",
    "EvaluationTask",
    "EvaluationTaskDetails",
    "ImportTask",
    "ImportTaskDetails",
    "InvalidTaskEnv",
    "InvalidTaskResult",
    "PackagePluginSpec",
    "PayloadSpec",
    "PluginSpec",
    "ScriptPluginSpec",
    "Task",
    "TaskAccessDenied",
    "TaskAttemptMismatch",
    "TaskDetails",
    "TaskNotFound",
    "TaskResultSessionAlreadyLinked",
    "TaskResultSessionMissing",
    "TaskRunSpec",
    "TaskSpec",
]

TERMINAL_TASK_STATUSES = frozenset(
    {
        TaskStatus.COMPLETED,
        TaskStatus.FAILED,
        TaskStatus.TIMED_OUT,
        TaskStatus.CANCELED,
        TaskStatus.ABANDONED,
    }
)
HARD_FAILURE_TASK_STATUSES = frozenset(
    {TaskStatus.FAILED, TaskStatus.TIMED_OUT, TaskStatus.ABANDONED}
)

# Variables the worker owns in the task process environment, rejected in the
# creator-set extras so no creator can shadow the process contract.
CONTRACT_ENV_NAMES = frozenset(
    {"KITARU_API_URL", "KITARU_API_KEY", "KITARU_API_TOKEN", "KITARU_REPLAY_ID"}
)
CONTRACT_ENV_PREFIX = "KITARU_TASK_"


class TaskNotFound(NotFoundError):
    """Raised when a task lookup does not resolve."""

    def __init__(self, task_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            task_id: Id of the missing task.
        """
        super().__init__(f"Task {task_id} was not found")


class TaskAccessDenied(ForbiddenError):
    """Raised when the caller's credential does not authorize this task."""

    def __init__(self, task_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            task_id: Id of the task.
        """
        super().__init__(f"Task {task_id} is not accessible to this caller")


class IllegalTaskStatusTransition(ConflictError):
    """Raised when a task status transition is not allowed."""

    def __init__(
        self, task_id: uuid.UUID, current: TaskStatus, target: TaskStatus
    ) -> None:
        """Initialize the error.

        Args:
            task_id: Id of the task.
            current: Current task status.
            target: Target task status.
        """
        super().__init__(f"Task {task_id} cannot transition from {current} to {target}")


class DuplicateEvaluationTask(ConflictError):
    """Raised when a job already scores an input session with an evaluator version."""

    def __init__(
        self,
        job_id: uuid.UUID,
        input_session_id: uuid.UUID | None,
        plugin_version_id: uuid.UUID | None,
    ) -> None:
        """Initialize the error.

        Args:
            job_id: Id of the owning job.
            input_session_id: Id of the scored session.
            plugin_version_id: Id of the evaluator version.
        """
        super().__init__(
            f"Job {job_id} already holds an evaluator task for session "
            f"{input_session_id} and evaluator version {plugin_version_id}"
        )


class TaskAttemptMismatch(ConflictError):
    """Raised when a transition is fenced by an attempt the task has moved past."""

    def __init__(self, task_id: uuid.UUID, attempt: int | None, current: int) -> None:
        """Initialize the error.

        Args:
            task_id: Id of the task.
            attempt: Attempt the transition was fenced by.
            current: Attempt the task currently holds.
        """
        super().__init__(
            f"Task {task_id} transition is fenced by attempt {attempt}, "
            f"the task holds attempt {current}"
        )


class InvalidTaskResult(ConflictError):
    """Raised when a completion result does not satisfy the task kind's contract."""


class TaskResultSessionMissing(ConflictError):
    """Raised when an agent task completes without a linked result session."""

    def __init__(self, task_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            task_id: Id of the task.
        """
        super().__init__(f"Task {task_id} has no linked result session")


class TaskResultSessionNotCompleted(ConflictError):
    """Raised when an agent task completes while its result session has not."""

    def __init__(self, task_id: uuid.UUID, session_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            task_id: Id of the task.
            session_id: Id of the linked result session.
        """
        super().__init__(
            f"Task {task_id} result session {session_id} has not completed"
        )


class TaskUpdateRequiresStatus(ValidationError):
    """Raised when a task update carries no status."""

    def __init__(self, task_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            task_id: Id of the task.
        """
        super().__init__(f"Task {task_id} update requires a status")


class TaskResultSessionAlreadyLinked(ConflictError):
    """Raised when a second session links to a task that already has one."""

    def __init__(self, task_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            task_id: Id of the task.
        """
        super().__init__(f"Task {task_id} already links a result session")


class TaskNotRunning(ConflictError):
    """Raised when an operation requires a running task."""

    def __init__(self, task_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            task_id: Id of the task.
        """
        super().__init__(f"Task {task_id} is not running")


class TaskResultTooLarge(PayloadTooLargeError):
    """Raised when a task result exceeds the configured size cap."""

    def __init__(self, max_bytes: int) -> None:
        """Initialize the error.

        Args:
            max_bytes: Maximum allowed result size in bytes.
        """
        super().__init__(f"Task result exceeds {max_bytes} bytes")


class InvalidTaskEnv(ValidationError):
    """Raised when task env extras name a contract variable."""

    def __init__(self, name: str) -> None:
        """Initialize the error.

        Args:
            name: Contract variable the extras tried to set.
        """
        super().__init__(f"Task env must not set the contract variable '{name}'")


class Task(DomainModel):
    """Task."""

    id: uuid.UUID = Field(default_factory=uuid7)
    job_id: uuid.UUID
    status: TaskStatus = TaskStatus.PENDING
    attempt: int = 0
    on_failure: TaskOnFailure = TaskOnFailure.ABORT
    labels: dict[str, str] = Field(default_factory=dict)
    env: dict[str, str] = Field(default_factory=dict)
    worker_id: uuid.UUID | None = None
    result_session_id: uuid.UUID | None = None
    claimed_at: datetime | None = None
    heartbeat_at: datetime | None = None
    cancel_requested_at: datetime | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    error: str | None = None
    result: Any = None
    created: datetime | None = None
    updated: datetime | None = None

    @field_validator("env")
    @classmethod
    def _check_env(cls, value: dict[str, str]) -> dict[str, str]:
        """Reject contract variable names in the creator-set env extras.

        Args:
            value: Env extras to validate.

        Raises:
            InvalidTaskEnv: A key names a contract variable.

        Returns:
            Validated env extras.
        """
        for name in value:
            if name in CONTRACT_ENV_NAMES or name.startswith(CONTRACT_ENV_PREFIX):
                raise InvalidTaskEnv(name)
        return value

    @property
    def kind(self) -> TaskKind:
        """Kind of work the task runs.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError

    @property
    def terminal(self) -> bool:
        """Whether the task reached a terminal status.

        Returns:
            Whether the task reached a terminal status.
        """
        return self.status in TERMINAL_TASK_STATUSES

    @property
    def counted_hard_failure(self) -> bool:
        """Whether the task failed hard and counts toward the job outcome.

        Returns:
            Whether the task failed hard and counts toward the job outcome.
        """
        return (
            self.status in HARD_FAILURE_TASK_STATUSES
            and self.on_failure is not TaskOnFailure.IGNORE
        )

    def _require_status(self, allowed: set[TaskStatus], target: TaskStatus) -> None:
        """Require the current status to allow a move to a target status.

        Args:
            allowed: Statuses the move is legal from.
            target: Target status.

        Raises:
            IllegalTaskStatusTransition: The current status is not in
                ``allowed``.
        """
        if self.status not in allowed:
            raise IllegalTaskStatusTransition(self.id, self.status, target)

    def check_attempt(self, attempt: int | None) -> None:
        """Require a fencing attempt to match the task's current attempt.

        Args:
            attempt: Attempt the transition is fenced by.

        Raises:
            TaskAttemptMismatch: The attempt does not match.
        """
        if attempt != self.attempt:
            raise TaskAttemptMismatch(self.id, attempt, self.attempt)

    def claim(self, worker_id: uuid.UUID, now: datetime) -> None:
        """Hand a pending task to a worker, incrementing the fencing attempt.

        Args:
            worker_id: Worker claiming the task.
            now: Current time.

        Raises:
            IllegalTaskStatusTransition: The task is not pending.
        """
        self._require_status({TaskStatus.PENDING}, TaskStatus.CLAIMED)
        self.status = TaskStatus.CLAIMED
        self.attempt += 1
        self.worker_id = worker_id
        self.claimed_at = now

    def start(self, now: datetime) -> None:
        """Move a claimed task to running and stamp started_at.

        Args:
            now: Current time.

        Raises:
            IllegalTaskStatusTransition: The task is not claimed.
        """
        self._require_status({TaskStatus.CLAIMED}, TaskStatus.RUNNING)
        self.status = TaskStatus.RUNNING
        self.started_at = now

    def requeue(self) -> None:
        """Return a claimed or running task to the queue, dropping its attempt state.

        Raises:
            IllegalTaskStatusTransition: The task is neither claimed nor
                running.
        """
        self._require_status(
            {TaskStatus.CLAIMED, TaskStatus.RUNNING}, TaskStatus.PENDING
        )
        self.status = TaskStatus.PENDING
        self.worker_id = None
        self.claimed_at = None
        self.heartbeat_at = None
        self.started_at = None
        self.result_session_id = None

    def check_result(self, result: Any) -> None:
        """Validate a completion result against the task kind's contract.

        Args:
            result: Result the completion carries.
        """

    def complete(self, result: Any, now: datetime) -> None:
        """Move a running task to completed, storing its validated result.

        Args:
            result: Result the completion carries.
            now: Current time.

        Raises:
            IllegalTaskStatusTransition: The task is not running.
        """
        self._require_status({TaskStatus.RUNNING}, TaskStatus.COMPLETED)
        self.check_result(result)
        self.status = TaskStatus.COMPLETED
        self.result = result
        self.ended_at = now

    def fail(self, error: str | None, result: Any, now: datetime) -> None:
        """Move a claimed or running task to failed, storing its diagnostic result.

        Args:
            error: Failure reason.
            result: Diagnostic output the failure carries.
            now: Current time.

        Raises:
            IllegalTaskStatusTransition: The task is neither claimed nor
                running.
        """
        self._require_status(
            {TaskStatus.CLAIMED, TaskStatus.RUNNING}, TaskStatus.FAILED
        )
        self.status = TaskStatus.FAILED
        self.error = error
        self.result = result
        self.ended_at = now

    def time_out(self, error: str | None, now: datetime) -> None:
        """Move a running task to timed_out.

        Args:
            error: Timeout reason.
            now: Current time.

        Raises:
            IllegalTaskStatusTransition: The task is not running.
        """
        self._require_status({TaskStatus.RUNNING}, TaskStatus.TIMED_OUT)
        self.status = TaskStatus.TIMED_OUT
        self.error = error
        self.ended_at = now

    def cancel(self, now: datetime) -> None:
        """Move a claimed or running task to canceled once its process is gone.

        Args:
            now: Current time.

        Raises:
            IllegalTaskStatusTransition: The task is neither claimed nor
                running.
        """
        self._require_status(
            {TaskStatus.CLAIMED, TaskStatus.RUNNING}, TaskStatus.CANCELED
        )
        self.status = TaskStatus.CANCELED
        self.ended_at = now

    def request_cancel(self, now: datetime) -> None:
        """Cancel a pending task outright, or stamp cancel_requested_at otherwise.

        Args:
            now: Current time.

        Raises:
            IllegalTaskStatusTransition: The task is terminal.
        """
        self._require_status(
            {TaskStatus.PENDING, TaskStatus.CLAIMED, TaskStatus.RUNNING},
            TaskStatus.CANCELED,
        )
        if self.cancel_requested_at is None:
            self.cancel_requested_at = now
        if self.status is TaskStatus.PENDING:
            self.status = TaskStatus.CANCELED
            self.ended_at = now

    def abandon(self, error: str | None, now: datetime) -> None:
        """Move a claimed or running task to abandoned at the retry cap.

        Args:
            error: Reason the task was abandoned.
            now: Current time.

        Raises:
            IllegalTaskStatusTransition: The task is neither claimed nor
                running.
        """
        self._require_status(
            {TaskStatus.CLAIMED, TaskStatus.RUNNING}, TaskStatus.ABANDONED
        )
        self.status = TaskStatus.ABANDONED
        self.error = error
        self.ended_at = now

    def link_result_session(self, session_id: uuid.UUID) -> None:
        """Link the session this task produced.

        Args:
            session_id: Id of the produced session.

        Raises:
            TaskResultSessionAlreadyLinked: A session is already linked.
        """
        if self.result_session_id is not None:
            raise TaskResultSessionAlreadyLinked(self.id)
        self.result_session_id = session_id

    def check_running(self) -> None:
        """Require the task to be running.

        Raises:
            TaskNotRunning: The task is not running.
        """
        if self.status is not TaskStatus.RUNNING:
            raise TaskNotRunning(self.id)

    def is_stale(self, now: datetime, timeout_seconds: int) -> bool:
        """Report whether an in-flight task missed the heartbeat window.

        Args:
            now: Current time.
            timeout_seconds: Heartbeat window in seconds.

        Returns:
            Whether the task is claimed or running and past the window.
        """
        if self.status not in (TaskStatus.CLAIMED, TaskStatus.RUNNING):
            return False
        last_seen = (
            self.heartbeat_at if self.heartbeat_at is not None else self.claimed_at
        )
        if last_seen is None:
            return False
        return now - last_seen > timedelta(seconds=timeout_seconds)

    def with_staleness(
        self, now: datetime, timeout_seconds: int, retry_limit: int
    ) -> Self:
        """Return the task carrying the status the next sweep would write.

        A task that is not stale is returned unchanged.

        Args:
            now: Current time.
            timeout_seconds: Heartbeat window in seconds.
            retry_limit: Attempts a task is requeued for before it is
                abandoned.

        Returns:
            Task carrying its effective status.
        """
        if not self.is_stale(now, timeout_seconds):
            return self
        if self.cancel_requested_at is not None:
            status = TaskStatus.CANCELED
        elif self.attempt < retry_limit:
            status = TaskStatus.PENDING
        else:
            status = TaskStatus.ABANDONED
        return self.model_copy(update={"status": status})


class AgentTask(Task):
    """Agent task."""

    agent_version_id: uuid.UUID
    inputs: Any = None

    @property
    def kind(self) -> TaskKind:
        """Kind of work the task runs.

        Returns:
            Agent kind.
        """
        return TaskKind.AGENT

    def check_result(self, result: Any) -> None:
        """Require a linked result session, the agent task's actual outcome.

        Args:
            result: Result the completion carries, diagnostic only.

        Raises:
            TaskResultSessionMissing: No session is linked.
        """
        if self.result_session_id is None:
            raise TaskResultSessionMissing(self.id)


class EvaluationTask(Task):
    """Evaluation task."""

    plugin_version_id: uuid.UUID
    input_session_id: uuid.UUID
    params: dict[str, Any] = Field(default_factory=dict)

    @property
    def kind(self) -> TaskKind:
        """Kind of work the task runs.

        Returns:
            Evaluator kind.
        """
        return TaskKind.EVALUATOR

    def check_result(self, result: Any) -> None:
        """Require a non-empty list of evaluation results with unique names.

        Args:
            result: Result the completion carries.

        Raises:
            InvalidTaskResult: The result is not a non-empty list of valid
                evaluation results, or two results share a name.
        """
        if not isinstance(result, list) or not result:
            raise InvalidTaskResult(
                f"Task {self.id} requires a non-empty list of evaluation results"
            )
        names: set[str] = set()
        for entry in result:
            try:
                parsed = EvaluationResult.model_validate(entry)
            except pydantic.ValidationError as exc:
                raise InvalidTaskResult(
                    f"Task {self.id} carries an invalid evaluation result: {exc}"
                ) from exc
            if parsed.name in names:
                raise InvalidTaskResult(
                    f"Task {self.id} carries the evaluation name "
                    f"'{parsed.name}' more than once"
                )
            names.add(parsed.name)


class ImportTask(Task):
    """Import task."""

    plugin_version_id: uuid.UUID
    payload_blob_id: uuid.UUID
    agent_id: uuid.UUID
    agent_version_id: uuid.UUID | None = None
    params: dict[str, Any] = Field(default_factory=dict)

    @property
    def kind(self) -> TaskKind:
        """Kind of work the task runs.

        Returns:
            Importer kind.
        """
        return TaskKind.IMPORTER

    def check_result(self, result: Any) -> None:
        """Require a result payload.

        Args:
            result: Result the completion carries.

        Raises:
            InvalidTaskResult: The result is null.
        """
        if result is None:
            raise InvalidTaskResult(f"Task {self.id} requires a result")


class TaskRunSpec(FrozenModel):
    """Task run spec."""

    command: str
    working_dir: str | None = None
    env: dict[str, str] = Field(default_factory=dict)


class ScriptPluginSpec(FrozenModel):
    """Script plugin spec."""

    type: Literal["script"] = "script"
    entrypoint: str
    blob_id: uuid.UUID
    sha256: str


class PackagePluginSpec(FrozenModel):
    """Package plugin spec."""

    type: Literal["package"] = "package"
    entrypoint: str
    requirement: str


PluginSpec = Annotated[
    ScriptPluginSpec | PackagePluginSpec, Field(discriminator="type")
]


class PayloadSpec(FrozenModel):
    """Payload spec."""

    blob_id: uuid.UUID
    sha256: str


class AgentTaskDetails(FrozenModel):
    """Agent task details."""

    kind: Literal[TaskKind.AGENT] = TaskKind.AGENT
    agent_id: uuid.UUID
    agent_version_id: uuid.UUID
    inputs: Any = None
    replay_id: uuid.UUID | None = None


class EvaluationTaskDetails(FrozenModel):
    """Evaluation task details."""

    kind: Literal[TaskKind.EVALUATOR] = TaskKind.EVALUATOR
    evaluator_name: str
    params: dict[str, Any] = Field(default_factory=dict)
    plugin: PluginSpec
    input_session_id: uuid.UUID


class ImportTaskDetails(FrozenModel):
    """Import task details."""

    kind: Literal[TaskKind.IMPORTER] = TaskKind.IMPORTER
    plugin: PluginSpec
    payload: PayloadSpec
    provider: str | None = None
    agent_id: uuid.UUID
    params: dict[str, Any] = Field(default_factory=dict)


TaskDetails = Annotated[
    AgentTaskDetails | EvaluationTaskDetails | ImportTaskDetails,
    Field(discriminator="kind"),
]


class TaskSpec(FrozenModel):
    """Task spec."""

    task_id: uuid.UUID
    kind: TaskKind
    timeout_seconds: int
    run_spec: TaskRunSpec | None = None
    env: dict[str, str] = Field(default_factory=dict)
    secret_env: dict[str, str] = Field(default_factory=dict)
    details: TaskDetails
