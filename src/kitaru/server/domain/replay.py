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
"""Replay entity and errors."""

import uuid
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import Field, SecretStr

from kitaru.server.base import FrozenModel
from kitaru.server.domain.agent_version import RunSpec
from kitaru.server.domain.base import (
    ConflictError,
    DomainModel,
    NotFoundError,
    ValidationError,
)
from kitaru.server.domain.ids import uuid7
from kitaru.server.domain.replay_config import (
    ReplayOverride,
    ScoringPolicy,
    ScoringResult,
    ToolPolicyConfig,
)


class ReplayStatus(StrEnum):
    """Replay status."""

    PENDING = "pending"
    CLAIMED = "claimed"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMED_OUT = "timed_out"
    CANCELED = "canceled"


TERMINAL_REPLAY_STATUSES = frozenset(
    {
        ReplayStatus.COMPLETED,
        ReplayStatus.FAILED,
        ReplayStatus.TIMED_OUT,
        ReplayStatus.CANCELED,
    }
)

HEARTBEAT_TIMEOUT_ERROR = "Replay heartbeat timed out"


class ReplayNotFound(NotFoundError):
    """Raised when a replay lookup does not resolve."""

    def __init__(self, replay_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            replay_id: Id of the missing replay.
        """
        super().__init__(f"Replay {replay_id} was not found")


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


class InvalidReplay(ValidationError):
    """Raised when a replay violates its shape rules."""


class InvalidReplayTransition(ConflictError):
    """Raised when a replay status transition is illegal."""

    def __init__(
        self, replay_id: uuid.UUID, status: ReplayStatus, target: ReplayStatus
    ) -> None:
        """Initialize the error.

        Args:
            replay_id: Id of the replay.
            status: Current status.
            target: Requested status.
        """
        super().__init__(
            f"Replay {replay_id} cannot transition from '{status}' to '{target}'"
        )


class ReplayNotActive(ConflictError):
    """Raised when an operation requires a claimed or running replay."""

    def __init__(self, replay_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            replay_id: Id of the replay.
        """
        super().__init__(f"Replay {replay_id} is not claimed or running")


class ReplayActive(ConflictError):
    """Raised when an operation requires a replay that is not claimed or running."""

    def __init__(self, replay_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            replay_id: Id of the replay.
        """
        super().__init__(f"Replay {replay_id} is claimed or running")


class ReplayNotStandalone(ConflictError):
    """Raised when an operation requires a standalone replay."""

    def __init__(self, replay_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            replay_id: Id of the replay.
        """
        super().__init__(f"Replay {replay_id} belongs to an experiment run")


class ReplayAlreadyLinked(ConflictError):
    """Raised when a replay already has a result session."""

    def __init__(self, replay_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            replay_id: Id of the replay.
        """
        super().__init__(f"Replay {replay_id} already has a result session")


class ReplayMissingResultSession(ConflictError):
    """Raised when an operation requires a linked result session."""

    def __init__(self, replay_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            replay_id: Id of the replay.
        """
        super().__init__(f"Replay {replay_id} has no result session")


class InvalidToolLookup(ValidationError):
    """Raised when a tool lookup request violates its shape rules."""


class ReplaySpec(FrozenModel):
    """Replay spec."""

    replay_id: uuid.UUID
    inputs: Any = None
    override: ReplayOverride | None = None
    tool_policy: ToolPolicyConfig
    scoring_policy: ScoringPolicy
    score_baselines: bool
    run_spec: RunSpec
    secret_env: dict[str, SecretStr]
    original_session_id: uuid.UUID


class Replay(DomainModel):
    """Replay."""

    id: uuid.UUID = Field(default_factory=uuid7)
    experiment_run_id: uuid.UUID | None = None
    replay_config_id: uuid.UUID
    agent_version_id: uuid.UUID
    original_session_id: uuid.UUID
    result_session_id: uuid.UUID | None = None
    status: ReplayStatus = ReplayStatus.PENDING
    attempt: int = 1
    worker_id: str | None = None
    claimed_at: datetime | None = None
    heartbeat_at: datetime | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    error: str | None = None
    passed: bool | None = None
    score: float | None = None
    scores: dict[str, float] | None = None
    diff: dict[str, Any] | None = None
    created: datetime | None = None
    updated: datetime | None = None

    @property
    def standalone(self) -> bool:
        """Whether the replay belongs to no experiment run.

        Returns:
            Whether the replay belongs to no experiment run.
        """
        return self.experiment_run_id is None

    def claim(self, worker_id: str) -> None:
        """Claim the replay for a worker.

        Args:
            worker_id: Id of the claiming worker.

        Raises:
            InvalidReplayTransition: The replay is not pending.
        """
        if self.status is not ReplayStatus.PENDING:
            raise InvalidReplayTransition(self.id, self.status, ReplayStatus.CLAIMED)
        now = datetime.now(UTC)
        self.status = ReplayStatus.CLAIMED
        self.worker_id = worker_id
        self.claimed_at = now
        self.heartbeat_at = now

    def start(self) -> None:
        """Start executing the replay.

        Run-created replays start from claimed, standalone replays start
        from claimed or skip the claim and start from pending.

        Raises:
            InvalidReplayTransition: The replay is not in a required
                status.
        """
        allowed = (
            (ReplayStatus.PENDING, ReplayStatus.CLAIMED)
            if self.standalone
            else (ReplayStatus.CLAIMED,)
        )
        if self.status not in allowed:
            raise InvalidReplayTransition(self.id, self.status, ReplayStatus.RUNNING)
        now = datetime.now(UTC)
        self.status = ReplayStatus.RUNNING
        self.started_at = now
        self.heartbeat_at = now

    def requeue(self) -> None:
        """Requeue the replay for another attempt.

        Raises:
            InvalidReplayTransition: The replay is not claimed or running.
        """
        if self.status not in (ReplayStatus.CLAIMED, ReplayStatus.RUNNING):
            raise InvalidReplayTransition(self.id, self.status, ReplayStatus.PENDING)
        self.status = ReplayStatus.PENDING
        self.attempt += 1
        self.worker_id = None
        self.claimed_at = None
        self.heartbeat_at = None
        self.started_at = None

    def retry(self) -> None:
        """Requeue the replay for another attempt after it finished.

        Raises:
            InvalidReplayTransition: The replay is not failed, timed out,
                or canceled.
        """
        if self.status not in (
            ReplayStatus.FAILED,
            ReplayStatus.TIMED_OUT,
            ReplayStatus.CANCELED,
        ):
            raise InvalidReplayTransition(self.id, self.status, ReplayStatus.PENDING)
        self.status = ReplayStatus.PENDING
        self.attempt += 1
        self.worker_id = None
        self.claimed_at = None
        self.heartbeat_at = None
        self.started_at = None
        self.ended_at = None
        self.error = None
        self.result_session_id = None

    def complete(self, result: ScoringResult, diff: dict[str, Any] | None) -> None:
        """Complete the replay with its scoring result.

        Args:
            result: Scoring result reported by the runner.
            diff: Diff summary.

        Raises:
            InvalidReplayTransition: The replay is not running.
            ReplayMissingResultSession: The replay has no result session.
        """
        if self.status is not ReplayStatus.RUNNING:
            raise InvalidReplayTransition(self.id, self.status, ReplayStatus.COMPLETED)
        if self.result_session_id is None:
            raise ReplayMissingResultSession(self.id)
        self.status = ReplayStatus.COMPLETED
        self.passed = result.passed
        self.score = result.score
        self.scores = result.scores
        self.diff = diff
        self.ended_at = datetime.now(UTC)

    def fail(self, error: str) -> None:
        """Fail the replay.

        Args:
            error: Error message.

        Raises:
            InvalidReplayTransition: The replay is not claimed or running.
        """
        if self.status not in (ReplayStatus.CLAIMED, ReplayStatus.RUNNING):
            raise InvalidReplayTransition(self.id, self.status, ReplayStatus.FAILED)
        self.status = ReplayStatus.FAILED
        self.error = error
        self.ended_at = datetime.now(UTC)

    def time_out(self, error: str) -> None:
        """Time out the replay.

        Args:
            error: Error message.

        Raises:
            InvalidReplayTransition: The replay is not claimed or running.
        """
        if self.status not in (ReplayStatus.CLAIMED, ReplayStatus.RUNNING):
            raise InvalidReplayTransition(self.id, self.status, ReplayStatus.TIMED_OUT)
        self.status = ReplayStatus.TIMED_OUT
        self.error = error
        self.ended_at = datetime.now(UTC)

    def cancel(self) -> None:
        """Cancel the replay.

        Raises:
            InvalidReplayTransition: The replay is already terminal.
        """
        if self.status in TERMINAL_REPLAY_STATUSES:
            raise InvalidReplayTransition(self.id, self.status, ReplayStatus.CANCELED)
        self.status = ReplayStatus.CANCELED
        self.ended_at = datetime.now(UTC)

    def heartbeat(self) -> None:
        """Record a worker heartbeat.

        Raises:
            ReplayNotActive: The replay is not claimed or running.
        """
        if self.status not in (ReplayStatus.CLAIMED, ReplayStatus.RUNNING):
            raise ReplayNotActive(self.id)
        self.heartbeat_at = datetime.now(UTC)

    def link_result_session(self, session_id: uuid.UUID) -> None:
        """Link the session recorded by the replayed agent.

        Args:
            session_id: Id of the result session.

        Raises:
            ReplayNotActive: The replay is not claimed or running.
            ReplayAlreadyLinked: The replay already has a result session.
        """
        if self.status not in (ReplayStatus.CLAIMED, ReplayStatus.RUNNING):
            raise ReplayNotActive(self.id)
        if self.result_session_id is not None:
            raise ReplayAlreadyLinked(self.id)
        self.result_session_id = session_id

    def is_stale(self, stale_before: datetime) -> bool:
        """Report whether the replay lost its worker heartbeat.

        Args:
            stale_before: Heartbeats older than this time count as lost.

        Returns:
            ``True`` for a claimed or running replay whose last heartbeat,
            or claim when no heartbeat arrived yet, is older than the
            threshold.
        """
        if self.status not in (ReplayStatus.CLAIMED, ReplayStatus.RUNNING):
            return False
        last = self.heartbeat_at or self.claimed_at
        return last is not None and last < stale_before

    def with_staleness(self, stale_before: datetime, max_attempts: int) -> "Replay":
        """Return the replay as the next claim would requeue or time it out.

        Args:
            stale_before: Heartbeats older than this time count as lost.
            max_attempts: Attempt count at which a stale replay times out.

        Returns:
            Copy with the staleness rule applied, the replay itself when it
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
