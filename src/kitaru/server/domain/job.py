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
"""Job entity and errors."""

import uuid
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Self

from pydantic import Field, SecretStr, model_validator

from kitaru.server.base import FrozenModel
from kitaru.server.domain.agent_version import RunSpec
from kitaru.server.domain.base import (
    ConflictError,
    DomainModel,
    NotFoundError,
    ValidationError,
)
from kitaru.server.domain.execution import ExecutionTarget
from kitaru.server.domain.ids import uuid7
from kitaru.server.domain.plugin import PluginFormat
from kitaru.server.domain.replay_config import (
    ReplayOverride,
    ScorerConfig,
    SourceScorerConfig,
    ToolPolicyConfig,
)
from kitaru.server.domain.session import SessionProvider


class JobKind(StrEnum):
    """Job kind."""

    REPLAY = "replay"
    SESSION_RUN = "session_run"
    SCORE = "score"
    IMPORT = "import"


class JobStatus(StrEnum):
    """Job status."""

    PENDING = "pending"
    CLAIMED = "claimed"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMED_OUT = "timed_out"
    CANCELED = "canceled"


TERMINAL_JOB_STATUSES = frozenset(
    {
        JobStatus.COMPLETED,
        JobStatus.FAILED,
        JobStatus.TIMED_OUT,
        JobStatus.CANCELED,
    }
)

HEARTBEAT_TIMEOUT_ERROR = "Job heartbeat timed out"


class JobNotFound(NotFoundError):
    """Raised when a job lookup does not resolve."""

    def __init__(self, job_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            job_id: Id of the missing job.
        """
        super().__init__(f"Job {job_id} was not found")


class DuplicateReplaySession(ConflictError):
    """Raised when a run already replays a session."""

    def __init__(self, run_id: uuid.UUID, session_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            run_id: Id of the experiment run.
            session_id: Id of the already replayed session.
        """
        super().__init__(
            f"Session {session_id} is already replayed in experiment run {run_id}"
        )


class InvalidJob(ValidationError):
    """Raised when a job violates its shape rules."""


class InvalidWorkerScope(ValidationError):
    """Raised when a worker scope violates its shape rules."""


class InvalidJobTransition(ConflictError):
    """Raised when a job status transition is illegal."""

    def __init__(self, job_id: uuid.UUID, status: JobStatus, target: JobStatus) -> None:
        """Initialize the error.

        Args:
            job_id: Id of the job.
            status: Current status.
            target: Requested status.
        """
        super().__init__(
            f"Job {job_id} cannot transition from '{status}' to '{target}'"
        )


class JobNotRunning(ConflictError):
    """Raised when an operation requires a running job."""

    def __init__(self, job_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            job_id: Id of the job.
        """
        super().__init__(f"Job {job_id} is not running")


class JobActive(ConflictError):
    """Raised when an operation requires a job that is not claimed or running."""

    def __init__(self, job_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            job_id: Id of the job.
        """
        super().__init__(f"Job {job_id} is claimed or running")


class JobNotStandalone(ConflictError):
    """Raised when an operation requires a standalone job."""

    def __init__(self, job_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            job_id: Id of the job.
        """
        super().__init__(f"Job {job_id} belongs to an experiment run")


class JobAlreadyLinked(ConflictError):
    """Raised when a job already has a result session."""

    def __init__(self, job_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            job_id: Id of the job.
        """
        super().__init__(f"Job {job_id} already has a result session")


class JobMissingResultSession(ConflictError):
    """Raised when an operation requires a linked result session."""

    def __init__(self, job_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            job_id: Id of the job.
        """
        super().__init__(f"Job {job_id} has no result session")


class JobResultSessionNotCompleted(ConflictError):
    """Raised when an operation requires a completed result session."""

    def __init__(self, job_id: uuid.UUID, session_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            job_id: Id of the job.
            session_id: Id of the result session.
        """
        super().__init__(
            f"Result session {session_id} of job {job_id} is not completed"
        )


class JobMissingScore(ConflictError):
    """Raised when an operation requires a score result."""

    def __init__(self, job_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            job_id: Id of the job.
        """
        super().__init__(f"Job {job_id} has no score")


class JobMissingResult(ConflictError):
    """Raised when an operation requires a job result."""

    def __init__(self, job_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            job_id: Id of the job.
        """
        super().__init__(f"Job {job_id} has no result")


class DuplicateScoreJob(ConflictError):
    """Raised when a job already scores a session with a scorer."""

    def __init__(
        self, parent_job_id: uuid.UUID, session_id: uuid.UUID, scorer_name: str
    ) -> None:
        """Initialize the error.

        Args:
            parent_job_id: Id of the parent job.
            session_id: Id of the scored session.
            scorer_name: Name of the scorer.
        """
        super().__init__(
            f"Session {session_id} is already scored by '{scorer_name}' "
            f"for job {parent_job_id}"
        )


class InvalidToolLookup(ValidationError):
    """Raised when a tool lookup request violates its shape rules."""


class JobKindMismatch(ConflictError):
    """Raised when an operation requires a job of another kind."""

    def __init__(self, job_id: uuid.UUID, kind: JobKind) -> None:
        """Initialize the error.

        Args:
            job_id: Id of the job.
            kind: Required kind.
        """
        super().__init__(f"Job {job_id} is not of kind '{kind}'")


def _score_value(result: Any) -> float | None:
    """Read a score value out of a job result.

    Args:
        result: Result of a score job.

    Returns:
        Score value, ``None`` when the result is no number within 0 and 1.
    """
    if isinstance(result, bool) or not isinstance(result, int | float):
        return None
    if not 0 <= result <= 1:
        return None
    return float(result)


class PluginSpec(FrozenModel):
    """Plugin spec."""

    format: PluginFormat
    entrypoint: str
    blob_id: uuid.UUID
    sha256: str


class ScorerSpec(FrozenModel):
    """Scorer spec."""

    config: ScorerConfig
    plugin: PluginSpec | None = None
    input_session_id: uuid.UUID


class PayloadSpec(FrozenModel):
    """Payload spec."""

    blob_id: uuid.UUID
    sha256: str


class ImporterSpec(FrozenModel):
    """Importer spec."""

    plugin: PluginSpec
    payload: PayloadSpec
    provider: SessionProvider
    agent_id: uuid.UUID
    params: dict[str, Any] = Field(default_factory=dict)


class WorkerScope(FrozenModel):
    """Worker claim scope."""

    agent_version_ids: list[uuid.UUID] | None = None
    kinds: list[JobKind] | None = None
    experiment_run_id: uuid.UUID | None = None
    job_id: uuid.UUID | None = None

    @model_validator(mode="after")
    def validate_scope(self) -> Self:
        """Validate the mutual exclusivity and non-emptiness of the fields.

        Raises:
            InvalidWorkerScope: Both experiment_run_id and job_id are set,
                or kinds or agent_version_ids is set to an empty list.

        Returns:
            Validated scope.
        """
        if self.experiment_run_id is not None and self.job_id is not None:
            raise InvalidWorkerScope(
                "Worker scope experiment run id and job id are mutually exclusive"
            )
        if self.kinds is not None and not self.kinds:
            raise InvalidWorkerScope("Worker scope kinds must be non-empty when set")
        if self.agent_version_ids is not None and not self.agent_version_ids:
            raise InvalidWorkerScope(
                "Worker scope agent version ids must be non-empty when set"
            )
        return self

    @property
    def pinned(self) -> bool:
        """Whether the scope is pinned to an experiment run or a job.

        Returns:
            Whether the scope is pinned to an experiment run or a job.
        """
        return self.experiment_run_id is not None or self.job_id is not None


class JobSpec(FrozenModel):
    """Job spec."""

    job_id: uuid.UUID
    kind: JobKind
    inputs: Any = None
    override: ReplayOverride | None = None
    tool_policy: ToolPolicyConfig | None = None
    scorer: ScorerSpec | None = None
    importer: ImporterSpec | None = None
    run_spec: RunSpec | None = None
    secret_env: dict[str, SecretStr] = Field(default_factory=dict)
    input_session_id: uuid.UUID | None = None
    name: str | None = None


class Job(DomainModel):
    """Job."""

    id: uuid.UUID = Field(default_factory=uuid7)
    agent_version_id: uuid.UUID | None = None
    result_session_id: uuid.UUID | None = None
    status: JobStatus = JobStatus.PENDING
    attempt: int = 1
    worker_id: uuid.UUID | None = None
    execution_target: ExecutionTarget
    executor_handle: str | None = None
    claimed_at: datetime | None = None
    heartbeat_at: datetime | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    error: str | None = None
    result: Any = None
    created: datetime | None = None
    updated: datetime | None = None

    @property
    def kind(self) -> JobKind:
        """Kind of the job.

        Returns:
            Kind of the job.
        """
        raise NotImplementedError

    @property
    def standalone(self) -> bool:
        """Whether the job belongs to no experiment run.

        Returns:
            Whether the job belongs to no experiment run.
        """
        return True

    def claim(self, worker_id: uuid.UUID) -> None:
        """Claim the job for a worker.

        Args:
            worker_id: Id of the claiming worker.

        Raises:
            InvalidJobTransition: The job is not pending.
        """
        if self.status is not JobStatus.PENDING:
            raise InvalidJobTransition(self.id, self.status, JobStatus.CLAIMED)
        now = datetime.now(UTC)
        self.status = JobStatus.CLAIMED
        self.worker_id = worker_id
        self.claimed_at = now
        self.heartbeat_at = now

    def start(self) -> None:
        """Start executing the job.

        Run-created jobs start from claimed, standalone jobs start
        from claimed or skip the claim and start from pending.

        Raises:
            InvalidJobTransition: The job is not in a required
                status.
        """
        allowed = (
            (JobStatus.PENDING, JobStatus.CLAIMED)
            if self.standalone
            else (JobStatus.CLAIMED,)
        )
        if self.status not in allowed:
            raise InvalidJobTransition(self.id, self.status, JobStatus.RUNNING)
        now = datetime.now(UTC)
        self.status = JobStatus.RUNNING
        self.started_at = now
        self.heartbeat_at = now

    def requeue(self) -> None:
        """Requeue the job for another attempt.

        Raises:
            InvalidJobTransition: The job is not claimed or running.
        """
        if self.status not in (JobStatus.CLAIMED, JobStatus.RUNNING):
            raise InvalidJobTransition(self.id, self.status, JobStatus.PENDING)
        self.status = JobStatus.PENDING
        self.attempt += 1
        self.worker_id = None
        self.claimed_at = None
        self.heartbeat_at = None
        self.started_at = None

    def retry(self) -> None:
        """Requeue the job for another attempt after it finished.

        Raises:
            InvalidJobTransition: The job is not failed, timed out,
                or canceled.
        """
        if self.status not in (
            JobStatus.FAILED,
            JobStatus.TIMED_OUT,
            JobStatus.CANCELED,
        ):
            raise InvalidJobTransition(self.id, self.status, JobStatus.PENDING)
        self.status = JobStatus.PENDING
        self.attempt += 1
        self.worker_id = None
        self.claimed_at = None
        self.heartbeat_at = None
        self.started_at = None
        self.ended_at = None
        self.error = None
        self.result = None
        self.result_session_id = None

    def check_result(self, result: Any) -> None:
        """Check the result a completion ships against the job's kind.

        Args:
            result: Result shipped with the completion.
        """

    def complete(self, result: Any) -> None:
        """Complete the job with its result.

        Args:
            result: Result shipped with the completion.

        Raises:
            InvalidJobTransition: The job is not running.
        """
        if self.status is not JobStatus.RUNNING:
            raise InvalidJobTransition(self.id, self.status, JobStatus.COMPLETED)
        self.check_result(result)
        self.status = JobStatus.COMPLETED
        self.result = result
        self.ended_at = datetime.now(UTC)

    def fail(self, error: str) -> None:
        """Fail the job.

        Args:
            error: Error message.

        Raises:
            InvalidJobTransition: The job is not claimed or running.
        """
        if self.status not in (JobStatus.CLAIMED, JobStatus.RUNNING):
            raise InvalidJobTransition(self.id, self.status, JobStatus.FAILED)
        self.status = JobStatus.FAILED
        self.error = error
        self.ended_at = datetime.now(UTC)

    def time_out(self, error: str) -> None:
        """Time out the job.

        Args:
            error: Error message.

        Raises:
            InvalidJobTransition: The job is not claimed or running.
        """
        if self.status not in (JobStatus.CLAIMED, JobStatus.RUNNING):
            raise InvalidJobTransition(self.id, self.status, JobStatus.TIMED_OUT)
        self.status = JobStatus.TIMED_OUT
        self.error = error
        self.ended_at = datetime.now(UTC)

    def cancel(self) -> None:
        """Cancel the job.

        Raises:
            InvalidJobTransition: The job is already terminal.
        """
        if self.status in TERMINAL_JOB_STATUSES:
            raise InvalidJobTransition(self.id, self.status, JobStatus.CANCELED)
        self.status = JobStatus.CANCELED
        self.ended_at = datetime.now(UTC)

    def link_result_session(self, session_id: uuid.UUID) -> None:
        """Link the session recorded by the running agent.

        Args:
            session_id: Id of the result session.

        Raises:
            JobNotRunning: The job is not running.
            JobAlreadyLinked: The job already has a result session.
        """
        if self.status is not JobStatus.RUNNING:
            raise JobNotRunning(self.id)
        if self.result_session_id is not None:
            raise JobAlreadyLinked(self.id)
        self.result_session_id = session_id

    def is_stale(self, stale_before: datetime) -> bool:
        """Report whether the job lost its worker heartbeat.

        Args:
            stale_before: Heartbeats older than this time count as lost.

        Returns:
            ``True`` for a claimed or running job whose last heartbeat,
            or claim when no heartbeat arrived yet, is older than the
            threshold.
        """
        if self.status not in (JobStatus.CLAIMED, JobStatus.RUNNING):
            return False
        last = self.heartbeat_at or self.claimed_at
        return last is not None and last < stale_before

    def with_staleness(self, stale_before: datetime, max_attempts: int) -> Self:
        """Return the job as the next claim would requeue or time it out.

        Args:
            stale_before: Heartbeats older than this time count as lost.
            max_attempts: Attempt count at which a stale job times out.

        Returns:
            Copy with the staleness rule applied, the job itself when it
            is not stale.
        """
        if not self.is_stale(stale_before):
            return self
        copy = self.model_copy()
        if copy.attempt >= max_attempts:
            copy.time_out(HEARTBEAT_TIMEOUT_ERROR)
        else:
            copy.requeue()
        return copy


class ReplayJob(Job):
    """Replay job."""

    experiment_run_id: uuid.UUID | None = None
    input_session_id: uuid.UUID

    @model_validator(mode="after")
    def validate_agent_version(self) -> Self:
        """Validate that the job is bound to an agent version.

        Raises:
            InvalidJob: The job has no agent version.

        Returns:
            Validated job.
        """
        if self.agent_version_id is None:
            raise InvalidJob("Replays require an agent version")
        return self

    @property
    def kind(self) -> JobKind:
        """Kind of the job.

        Returns:
            Kind of the job.
        """
        return JobKind.REPLAY

    @property
    def standalone(self) -> bool:
        """Whether the job belongs to no experiment run.

        Returns:
            Whether the job belongs to no experiment run.
        """
        return self.experiment_run_id is None

    def check_result(self, result: Any) -> None:
        """Check that the job recorded a result session.

        Args:
            result: Result shipped with the completion.

        Raises:
            JobMissingResultSession: The job has no result session.
        """
        _ = result
        if self.result_session_id is None:
            raise JobMissingResultSession(self.id)


class SessionRun(Job):
    """Session run job."""

    inputs: Any = None
    name: str | None = None

    @model_validator(mode="after")
    def validate_agent_version(self) -> Self:
        """Validate that the job is bound to an agent version.

        Raises:
            InvalidJob: The job has no agent version.

        Returns:
            Validated job.
        """
        if self.agent_version_id is None:
            raise InvalidJob("Session runs require an agent version")
        return self

    @property
    def kind(self) -> JobKind:
        """Kind of the job.

        Returns:
            Kind of the job.
        """
        return JobKind.SESSION_RUN

    def check_result(self, result: Any) -> None:
        """Check that the job recorded a result session.

        Args:
            result: Result shipped with the completion.

        Raises:
            JobMissingResultSession: The job has no result session.
        """
        _ = result
        if self.result_session_id is None:
            raise JobMissingResultSession(self.id)


class Score(Job):
    """Score job."""

    parent_job_id: uuid.UUID | None = None
    input_session_id: uuid.UUID
    plugin_version_id: uuid.UUID | None = None
    scorer_config: ScorerConfig

    @property
    def score(self) -> float | None:
        """Score the scorer produced.

        Returns:
            Score value, ``None`` when the result carries no score.
        """
        return _score_value(self.result)

    def check_result(self, result: Any) -> None:
        """Check that the result is a score value.

        Args:
            result: Result shipped with the completion.

        Raises:
            JobMissingScore: The result is no number within 0 and 1.
        """
        if _score_value(result) is None:
            raise JobMissingScore(self.id)

    @model_validator(mode="after")
    def validate_arm(self) -> Self:
        """Validate the code reference the scorer config arm governs.

        Raises:
            InvalidJob: The job carries a code reference its arm does not
                support, or none at all.

        Returns:
            Validated job.
        """
        if isinstance(self.scorer_config, SourceScorerConfig):
            if self.agent_version_id is None:
                raise InvalidJob("Source scorers require an agent version")
            if self.plugin_version_id is not None:
                raise InvalidJob("Source scorers carry no plugin version")
        else:
            if self.plugin_version_id is None:
                raise InvalidJob("Registry scorers require a plugin version")
            if self.agent_version_id is not None:
                raise InvalidJob("Registry scorers carry no agent version")
        return self

    @property
    def kind(self) -> JobKind:
        """Kind of the job.

        Returns:
            Kind of the job.
        """
        return JobKind.SCORE


class Import(Job):
    """Import job."""

    plugin_version_id: uuid.UUID
    payload_blob_id: uuid.UUID
    agent_id: uuid.UUID
    inputs: Any = None

    @model_validator(mode="after")
    def validate_agent_version(self) -> Self:
        """Validate that the job is bound to no agent version.

        Raises:
            InvalidJob: The job has an agent version.

        Returns:
            Validated job.
        """
        if self.agent_version_id is not None:
            raise InvalidJob("Imports carry no agent version")
        return self

    @property
    def kind(self) -> JobKind:
        """Kind of the job.

        Returns:
            Kind of the job.
        """
        return JobKind.IMPORT

    def check_result(self, result: Any) -> None:
        """Check that the completion ships a result.

        Args:
            result: Result shipped with the completion.

        Raises:
            JobMissingResult: The completion ships no result.
        """
        if result is None:
            raise JobMissingResult(self.id)
