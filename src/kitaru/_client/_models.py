"""Internal client-facing data models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from kitaru.config import FrozenExecutionSpec
from kitaru.errors import FailureOrigin

if TYPE_CHECKING:
    from kitaru.client import KitaruClient


class ExecutionStatus(StrEnum):
    """Simplified public execution status taxonomy."""

    RUNNING = "running"
    WAITING = "waiting"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(frozen=True)
class PendingWait:
    """Public view of an active wait condition."""

    wait_id: str
    name: str
    question: str | None
    schema: dict[str, Any] | None
    metadata: dict[str, Any]
    entered_waiting_at: datetime | None


@dataclass(frozen=True)
class FailureInfo:
    """Structured failure details for executions/checkpoints."""

    message: str
    exception_type: str | None
    traceback: str | None
    origin: FailureOrigin


@dataclass(frozen=True)
class LogEntry:
    """One runtime log entry retrieved for an execution."""

    message: str
    level: str | None = None
    timestamp: str | None = None
    source: str | None = None
    checkpoint_name: str | None = None
    module: str | None = None
    filename: str | None = None
    lineno: int | None = None


@dataclass(frozen=True)
class CheckpointAttempt:
    """One checkpoint attempt in retry/failure journaling history."""

    attempt_id: str
    status: ExecutionStatus
    started_at: datetime | None
    ended_at: datetime | None
    metadata: dict[str, Any]
    failure: FailureInfo | None


@dataclass(frozen=True)
class ArtifactRef:
    """Reference to an artifact produced by an execution."""

    artifact_id: str
    name: str
    kind: str | None
    save_type: str
    producing_call: str | None
    metadata: dict[str, Any]
    _client: KitaruClient = field(repr=False, compare=False)

    def load(self) -> Any:
        """Load and materialize this artifact value."""
        return self._client._load_artifact_value(self.artifact_id)


@dataclass(frozen=True)
class CheckpointCall:
    """Public view of a checkpoint call inside an execution."""

    call_id: str
    name: str
    status: ExecutionStatus
    started_at: datetime | None
    ended_at: datetime | None
    metadata: dict[str, Any]
    original_call_id: str | None
    parent_call_ids: list[str]
    failure: FailureInfo | None
    attempts: list[CheckpointAttempt]
    artifacts: list[ArtifactRef]
    checkpoint_type: str | None = None


@dataclass(frozen=True)
class Execution:
    """Public view of a Kitaru execution."""

    exec_id: str
    flow_name: str | None
    status: ExecutionStatus
    started_at: datetime | None
    ended_at: datetime | None
    stack_name: str | None
    metadata: dict[str, Any]
    status_reason: str | None
    failure: FailureInfo | None
    pending_wait: PendingWait | None
    frozen_execution_spec: FrozenExecutionSpec | None
    original_exec_id: str | None
    checkpoints: list[CheckpointCall]
    artifacts: list[ArtifactRef]
    _client: KitaruClient = field(repr=False, compare=False)

    def refresh(self) -> Execution:
        """Fetch the latest execution state."""
        return self._client.executions.get(self.exec_id)

    def retry(self) -> Execution:
        """Retry this failed execution as a same-execution recovery."""
        return self._client.executions.retry(self.exec_id)

    def resume(self) -> Execution:
        """Resume this paused execution after wait input is resolved."""
        return self._client.executions.resume(self.exec_id)

    def cancel(self) -> Execution:
        """Cancel this execution."""
        return self._client.executions.cancel(self.exec_id)

    def replay(
        self,
        *,
        from_: str,
        overrides: dict[str, Any] | None = None,
        **flow_inputs: Any,
    ) -> Execution:
        """Replay this execution from a prior checkpoint boundary."""
        return self._client.executions.replay(
            self.exec_id,
            from_=from_,
            overrides=overrides,
            **flow_inputs,
        )

    def list_checkpoints(self) -> list[CheckpointCall]:
        """Return checkpoint calls for this execution."""
        return list(self.checkpoints)

    def list_artifacts(self) -> list[ArtifactRef]:
        """Return artifact refs for this execution."""
        return list(self.artifacts)


__all__ = [
    "ArtifactRef",
    "CheckpointAttempt",
    "CheckpointCall",
    "Execution",
    "ExecutionStatus",
    "FailureInfo",
    "LogEntry",
    "PendingWait",
]
