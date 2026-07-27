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
    ScoringResult,
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
    SCORING = "scoring"
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

MAX_IMPORT_FAILURES = 20


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


class JobNotActive(ConflictError):
    """Raised when an operation requires a claimed or running job."""

    def __init__(self, job_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            job_id: Id of the job.
        """
        super().__init__(f"Job {job_id} is not claimed or running")


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


class JobMissingScore(ConflictError):
    """Raised when an operation requires a recorded score."""

    def __init__(self, job_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            job_id: Id of the job.
        """
        super().__init__(f"Job {job_id} has no score")


class JobMissingStats(ConflictError):
    """Raised when an operation requires recorded import stats."""

    def __init__(self, job_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            job_id: Id of the job.
        """
        super().__init__(f"Job {job_id} has no stats")


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


class ImportFailure(FrozenModel):
    """Import failure."""

    line: int = Field(ge=0)
    external_id: str | None = None
    error: str


class ImportStats(FrozenModel):
    """Import stats."""

    created: int = Field(ge=0)
    skipped: int = Field(ge=0)
    failed: int = Field(ge=0)
    failures: list[ImportFailure] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_failures(self) -> Self:
        """Validate the failure sample size.

        Raises:
            InvalidJob: The sample exceeds the entry limit.

        Returns:
            Validated stats.
        """
        if len(self.failures) > MAX_IMPORT_FAILURES:
            raise InvalidJob(
                f"Import stats carry at most {MAX_IMPORT_FAILURES} failures"
            )
        return self


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
        self.result_session_id = None

    def fail(self, error: str) -> None:
        """Fail the job.

        Args:
            error: Error message.

        Raises:
            InvalidJobTransition: The job is not claimed, running, or
                scoring.
        """
        if self.status not in (
            JobStatus.CLAIMED,
            JobStatus.RUNNING,
            JobStatus.SCORING,
        ):
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
        """Link the session recorded by the replayed agent.

        Args:
            session_id: Id of the result session.

        Raises:
            JobNotActive: The job is not claimed or running.
            JobAlreadyLinked: The job already has a result session.
        """
        if self.status not in (JobStatus.CLAIMED, JobStatus.RUNNING):
            raise JobNotActive(self.id)
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


class Replay(Job):
    """Replay job."""

    experiment_run_id: uuid.UUID | None = None
    replay_config_id: uuid.UUID
    input_session_id: uuid.UUID
    passed: bool | None = None
    score: float | None = None
    scores: dict[str, float] | None = None
    diff: dict[str, Any] | None = None

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

    def enter_scoring(self) -> None:
        """Hand the job over to its score jobs.

        Raises:
            InvalidJobTransition: The job is not running.
            JobMissingResultSession: The job has no result session.
        """
        if self.status is not JobStatus.RUNNING:
            raise InvalidJobTransition(self.id, self.status, JobStatus.SCORING)
        if self.result_session_id is None:
            raise JobMissingResultSession(self.id)
        self.status = JobStatus.SCORING

    def complete(self, result: ScoringResult, diff: dict[str, Any] | None) -> None:
        """Complete the job with its scoring result.

        Args:
            result: Scoring result computed from the score jobs.
            diff: Diff summary.

        Raises:
            InvalidJobTransition: The job is not scoring.
            JobMissingResultSession: The job has no result session.
        """
        if self.status is not JobStatus.SCORING:
            raise InvalidJobTransition(self.id, self.status, JobStatus.COMPLETED)
        if self.result_session_id is None:
            raise JobMissingResultSession(self.id)
        self.status = JobStatus.COMPLETED
        self.passed = result.passed
        self.score = result.score
        self.scores = result.scores
        self.diff = diff
        self.ended_at = datetime.now(UTC)


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

    def complete(self) -> None:
        """Complete the job.

        Raises:
            InvalidJobTransition: The job is not running.
            JobMissingResultSession: The job has no result session.
        """
        if self.status is not JobStatus.RUNNING:
            raise InvalidJobTransition(self.id, self.status, JobStatus.COMPLETED)
        if self.result_session_id is None:
            raise JobMissingResultSession(self.id)
        self.status = JobStatus.COMPLETED
        self.ended_at = datetime.now(UTC)


class Score(Job):
    """Score job."""

    parent_job_id: uuid.UUID | None = None
    input_session_id: uuid.UUID
    plugin_version_id: uuid.UUID | None = None
    scorer_config: ScorerConfig
    score: float | None = None

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

    def record_score(self, score: float) -> None:
        """Record the score the scorer produced.

        Args:
            score: Score value.

        Raises:
            JobNotActive: The job is not claimed or running.
        """
        if self.status not in (JobStatus.CLAIMED, JobStatus.RUNNING):
            raise JobNotActive(self.id)
        self.score = score

    def complete(self) -> None:
        """Complete the job.

        Raises:
            InvalidJobTransition: The job is not running.
            JobMissingScore: The job has no recorded score.
        """
        if self.status is not JobStatus.RUNNING:
            raise InvalidJobTransition(self.id, self.status, JobStatus.COMPLETED)
        if self.score is None:
            raise JobMissingScore(self.id)
        self.status = JobStatus.COMPLETED
        self.ended_at = datetime.now(UTC)

    def retry(self) -> None:
        """Requeue the job for another attempt after it finished.

        Raises:
            InvalidJobTransition: The job is not failed, timed out,
                or canceled.
        """
        super().retry()
        self.score = None


class Import(Job):
    """Import job."""

    plugin_version_id: uuid.UUID
    payload_blob_id: uuid.UUID
    agent_id: uuid.UUID
    inputs: Any = None
    stats: ImportStats | None = None

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

    def record_stats(self, stats: ImportStats) -> None:
        """Record the stats the importer produced.

        Args:
            stats: Import stats.

        Raises:
            JobNotActive: The job is not claimed or running.
        """
        if self.status not in (JobStatus.CLAIMED, JobStatus.RUNNING):
            raise JobNotActive(self.id)
        self.stats = stats

    def complete(self) -> None:
        """Complete the job.

        Raises:
            InvalidJobTransition: The job is not running.
            JobMissingStats: The job has no recorded stats.
        """
        if self.status is not JobStatus.RUNNING:
            raise InvalidJobTransition(self.id, self.status, JobStatus.COMPLETED)
        if self.stats is None:
            raise JobMissingStats(self.id)
        self.status = JobStatus.COMPLETED
        self.ended_at = datetime.now(UTC)

    def retry(self) -> None:
        """Requeue the job for another attempt after it finished.

        Raises:
            InvalidJobTransition: The job is not failed, timed out,
                or canceled.
        """
        super().retry()
        self.stats = None
