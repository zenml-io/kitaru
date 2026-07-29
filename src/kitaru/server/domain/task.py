"""Task queue entities, specifications, and lifecycle rules."""

import uuid
from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import Field, model_validator

from kitaru.base import FrozenModel
from kitaru.server.domain.base import (
    ConflictError,
    DomainModel,
    NotFoundError,
    ValidationError,
)
from kitaru.server.domain.ids import uuid7

TASK_CONTRACT_ENV_NAMES = frozenset(
    {
        "KITARU_API_URL",
        "KITARU_API_KEY",
        "KITARU_TASK_ID",
        "KITARU_TASK_INPUTS",
        "KITARU_TASK_PLUGIN_PATH",
        "KITARU_TASK_PAYLOAD_PATH",
        "KITARU_TASK_RESULT_PATH",
    }
)


class TaskKind(StrEnum):
    """Executable task kind."""

    AGENT = "agent"
    EVALUATOR = "evaluator"
    IMPORTER = "importer"


class TaskOnFailure(StrEnum):
    """Effect of a hard task failure on its job."""

    ABORT = "abort"
    CONTINUE = "continue"
    IGNORE = "ignore"


class TaskStatus(StrEnum):
    """Task lifecycle status."""

    PENDING = "pending"
    CLAIMED = "claimed"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMED_OUT = "timed_out"
    CANCELED = "canceled"
    ABANDONED = "abandoned"

    @property
    def terminal(self) -> bool:
        """Report whether no further transition is allowed."""
        return self in {
            self.COMPLETED,
            self.FAILED,
            self.TIMED_OUT,
            self.CANCELED,
            self.ABANDONED,
        }

    @property
    def hard_failure(self) -> bool:
        """Report whether the status contributes a hard failure."""
        return self in {self.FAILED, self.TIMED_OUT, self.ABANDONED}


class TaskNotFound(NotFoundError):
    """Raised when a task lookup does not resolve."""

    def __init__(self, task_id: uuid.UUID) -> None:
        super().__init__(f"Task {task_id} was not found")


class InvalidTaskTransition(ConflictError):
    """Raised when a task transition is illegal."""

    def __init__(
        self, task_id: uuid.UUID, source: TaskStatus, target: TaskStatus
    ) -> None:
        super().__init__(f"Task {task_id} cannot move from {source} to {target}")


class StaleTaskAttempt(ConflictError):
    """Raised when an executor writes with an old attempt token."""

    def __init__(self, task_id: uuid.UUID, attempt: int) -> None:
        super().__init__(f"Task {task_id} no longer belongs to attempt {attempt}")


class InvalidTaskResult(ConflictError):
    """Raised when a successful transition lacks the kind-specific result."""


class InvalidTaskEnvironment(ValidationError):
    """Raised when task creator env overrides worker contract variables."""


class TaskRunSpec(FrozenModel):
    """Process command shipped in a task specification."""

    command: str
    working_dir: str | None = None
    env: dict[str, str] = Field(default_factory=dict)


class ScriptPluginSpec(FrozenModel):
    """Materialized script plugin."""

    type: Literal["script"] = "script"
    entrypoint: str
    blob_id: uuid.UUID
    sha256: str


class PackagePluginSpec(FrozenModel):
    """Exact-pinned package plugin."""

    type: Literal["package"] = "package"
    entrypoint: str
    requirement: str


PluginSpec = Annotated[
    ScriptPluginSpec | PackagePluginSpec, Field(discriminator="type")
]


class PayloadSpec(FrozenModel):
    """Import payload reference."""

    blob_id: uuid.UUID
    sha256: str


class AgentTaskDetails(FrozenModel):
    """Agent task inputs."""

    kind: Literal["agent"] = "agent"
    inputs: Any = None


class EvaluationTaskDetails(FrozenModel):
    """Evaluator task inputs."""

    kind: Literal["evaluator"] = "evaluator"
    evaluator_name: str
    params: dict[str, Any] = Field(default_factory=dict)
    plugin: PluginSpec
    input_session_id: uuid.UUID


class ImportTaskDetails(FrozenModel):
    """Importer task inputs."""

    kind: Literal["importer"] = "importer"
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
    """Fully resolved task execution specification."""

    task_id: uuid.UUID
    kind: TaskKind
    timeout_seconds: int
    run: TaskRunSpec | None = None
    env: dict[str, str] = Field(default_factory=dict)
    secret_env: dict[str, str] = Field(default_factory=dict)
    details: TaskDetails


class Task(DomainModel):
    """Generic claimed work item."""

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

    @property
    def kind(self) -> TaskKind:
        """Return the concrete task kind."""
        raise NotImplementedError

    @model_validator(mode="after")
    def _validate_env(self) -> "Task":
        reserved = TASK_CONTRACT_ENV_NAMES & self.env.keys()
        if reserved:
            raise InvalidTaskEnvironment(
                f"Task env contains reserved variables: {', '.join(sorted(reserved))}"
            )
        return self

    def _check_attempt(self, attempt: int) -> None:
        if attempt != self.attempt:
            raise StaleTaskAttempt(self.id, attempt)

    def _require_status(self, target: TaskStatus, *allowed: TaskStatus) -> None:
        if self.status not in allowed:
            raise InvalidTaskTransition(self.id, self.status, target)

    def claim(self, worker_id: uuid.UUID, now: datetime | None = None) -> None:
        """Assign a pending task and increment its fencing token."""
        self._require_status(TaskStatus.CLAIMED, TaskStatus.PENDING)
        timestamp = now or datetime.now(UTC)
        self.status = TaskStatus.CLAIMED
        self.attempt += 1
        self.worker_id = worker_id
        self.claimed_at = timestamp
        self.heartbeat_at = timestamp

    def start(self, attempt: int, now: datetime | None = None) -> None:
        """Record process start for the current attempt."""
        self._check_attempt(attempt)
        self._require_status(TaskStatus.RUNNING, TaskStatus.CLAIMED)
        self.status = TaskStatus.RUNNING
        self.started_at = now or datetime.now(UTC)

    def request_cancel(self, now: datetime | None = None) -> None:
        """Record cancellation, immediately settling an unclaimed task."""
        if self.status.terminal:
            return
        timestamp = now or datetime.now(UTC)
        self.cancel_requested_at = self.cancel_requested_at or timestamp
        if self.status is TaskStatus.PENDING:
            self.status = TaskStatus.CANCELED
            self.ended_at = timestamp

    def requeue(self) -> None:
        """Return a stale attempt to pending."""
        self._require_status(TaskStatus.PENDING, TaskStatus.CLAIMED, TaskStatus.RUNNING)
        self.status = TaskStatus.PENDING
        self.worker_id = None
        self.result_session_id = None
        self.claimed_at = None
        self.heartbeat_at = None
        self.started_at = None
        self.ended_at = None
        self.error = None
        self.result = None

    def check_result(self, result: Any) -> None:
        """Validate a successful kind-specific result."""
        _ = result

    def complete(
        self, attempt: int, result: Any = None, now: datetime | None = None
    ) -> None:
        """Record successful completion for the current attempt."""
        self._check_attempt(attempt)
        self._require_status(TaskStatus.COMPLETED, TaskStatus.RUNNING)
        self.check_result(result)
        self.status = TaskStatus.COMPLETED
        self.result = result
        self.ended_at = now or datetime.now(UTC)
        self.error = None

    def fail(
        self,
        attempt: int,
        error: str | None,
        result: Any = None,
        now: datetime | None = None,
    ) -> None:
        """Record process failure for the current attempt."""
        self._check_attempt(attempt)
        self._require_status(TaskStatus.FAILED, TaskStatus.CLAIMED, TaskStatus.RUNNING)
        self.status = TaskStatus.FAILED
        self.error = error
        self.result = result
        self.ended_at = now or datetime.now(UTC)

    def time_out(
        self, attempt: int, error: str | None, now: datetime | None = None
    ) -> None:
        """Record worker process timeout."""
        self._check_attempt(attempt)
        self._require_status(TaskStatus.TIMED_OUT, TaskStatus.RUNNING)
        self.status = TaskStatus.TIMED_OUT
        self.error = error
        self.ended_at = now or datetime.now(UTC)

    def cancel(self, attempt: int, now: datetime | None = None) -> None:
        """Confirm process cancellation for the current attempt."""
        self._check_attempt(attempt)
        self._require_status(
            TaskStatus.CANCELED, TaskStatus.CLAIMED, TaskStatus.RUNNING
        )
        self.status = TaskStatus.CANCELED
        self.ended_at = now or datetime.now(UTC)

    def abandon(self, now: datetime | None = None) -> None:
        """Settle a stale attempt at the retry cap."""
        self._require_status(
            TaskStatus.ABANDONED, TaskStatus.CLAIMED, TaskStatus.RUNNING
        )
        self.status = TaskStatus.ABANDONED
        self.error = "Task was abandoned after stale attempts"
        self.ended_at = now or datetime.now(UTC)

    def link_result_session(self, session_id: uuid.UUID) -> None:
        """Link the session created by this task attempt."""
        if self.result_session_id is not None and self.result_session_id != session_id:
            raise ConflictError(f"Task {self.id} already has a result session")
        self.result_session_id = session_id

    def is_stale(self, stale_before: datetime) -> bool:
        """Report whether the last claim heartbeat predates a cutoff."""
        if self.status not in {TaskStatus.CLAIMED, TaskStatus.RUNNING}:
            return False
        last_seen = self.heartbeat_at or self.claimed_at
        return last_seen is not None and last_seen < stale_before

    def with_staleness(self, stale_before: datetime, retry_cap: int) -> "Task":
        """Return an effective-status copy for stale reads."""
        effective = self.model_copy(deep=True)
        if not effective.is_stale(stale_before):
            return effective
        if effective.cancel_requested_at is not None:
            effective.status = TaskStatus.CANCELED
        elif effective.attempt >= retry_cap:
            effective.status = TaskStatus.ABANDONED
        else:
            effective.status = TaskStatus.PENDING
        return effective


class AgentTask(Task):
    """Task running an agent version."""

    agent_version_id: uuid.UUID
    inputs: Any = None

    @property
    def kind(self) -> TaskKind:
        """Return the agent kind."""
        return TaskKind.AGENT

    def check_result(self, result: Any) -> None:
        """Require a linked completed session, checked by the service."""
        _ = result
        if self.result_session_id is None:
            raise InvalidTaskResult(
                "Agent process exited successfully without recording a result session."
            )


class EvaluationTask(Task):
    """Task running a registered evaluator."""

    plugin_version_id: uuid.UUID
    input_session_id: uuid.UUID
    params: dict[str, Any] = Field(default_factory=dict)

    @property
    def kind(self) -> TaskKind:
        """Return the evaluator kind."""
        return TaskKind.EVALUATOR

    def check_result(self, result: Any) -> None:
        """Require a non-empty list with unique evaluation names."""
        if not isinstance(result, list) or not result:
            raise InvalidTaskResult(
                "Evaluator process exited successfully without writing a result."
            )
        names: list[Any] = []
        for entry in result:
            name = (
                entry.get("name")
                if isinstance(entry, dict)
                else getattr(entry, "name", None)
            )
            if name is None:
                raise InvalidTaskResult("Evaluator result is missing a name")
            names.append(name)
        if len(names) != len(set(names)):
            raise InvalidTaskResult("Evaluator result names must be unique")


class ImportTask(Task):
    """Task running a registered importer."""

    plugin_version_id: uuid.UUID
    payload_blob_id: uuid.UUID
    agent_id: uuid.UUID
    params: dict[str, Any] = Field(default_factory=dict)

    @property
    def kind(self) -> TaskKind:
        """Return the importer kind."""
        return TaskKind.IMPORTER

    def check_result(self, result: Any) -> None:
        """Require import statistics."""
        if result is None:
            raise InvalidTaskResult(
                "Importer process exited successfully without writing a result."
            )
