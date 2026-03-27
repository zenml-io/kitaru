"""Dapr execution ledger record models.

These dataclasses define the persistence schema for Dapr execution state.
They are pure data — no Dapr SDK imports — so they can be used in tests
and mappers without the ``kitaru[dapr]`` extra installed.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from kitaru.errors import FailureOrigin

# ---------------------------------------------------------------------------
# Shared constants for internal flow-result artifact persistence
# ---------------------------------------------------------------------------

DAPR_METADATA_NAMESPACE = "kitaru"
FLOW_RESULT_ARTIFACT_ID_KEY = "flow_result_artifact_id"
INTERNAL_ARTIFACT_FLAG = "kitaru_internal"
FLOW_RESULT_ARTIFACT_NAME = "__flow_result__"
FLOW_RESULT_SAVE_TYPE = "flow_output"

# ---------------------------------------------------------------------------
# Datetime helpers
# ---------------------------------------------------------------------------


def _dt_to_iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.isoformat()


def _iso_to_dt(value: str | None) -> datetime | None:
    if value is None:
        return None
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


# ---------------------------------------------------------------------------
# FailureRecord
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FailureRecord:
    """Structured failure detail for an execution or checkpoint attempt."""

    message: str
    exception_type: str | None = None
    traceback: str | None = None
    origin: FailureOrigin = FailureOrigin.UNKNOWN

    def to_dict(self) -> dict[str, Any]:
        return {
            "message": self.message,
            "exception_type": self.exception_type,
            "traceback": self.traceback,
            "origin": self.origin.value,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> FailureRecord:
        return cls(
            message=data["message"],
            exception_type=data.get("exception_type"),
            traceback=data.get("traceback"),
            origin=FailureOrigin(data.get("origin", FailureOrigin.UNKNOWN)),
        )


# ---------------------------------------------------------------------------
# ArtifactRecord
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ArtifactRecord:
    """Metadata for a persisted artifact in the Dapr ledger."""

    artifact_id: str
    name: str
    kind: str | None = None
    save_type: str = "manual"
    producing_call_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    exec_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_id": self.artifact_id,
            "name": self.name,
            "kind": self.kind,
            "save_type": self.save_type,
            "producing_call_id": self.producing_call_id,
            "metadata": self.metadata,
            "exec_id": self.exec_id,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ArtifactRecord:
        return cls(
            artifact_id=data["artifact_id"],
            name=data["name"],
            kind=data.get("kind"),
            save_type=data.get("save_type", "manual"),
            producing_call_id=data.get("producing_call_id"),
            metadata=data.get("metadata", {}),
            exec_id=data.get("exec_id"),
        )


# ---------------------------------------------------------------------------
# CheckpointAttemptRecord
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CheckpointAttemptRecord:
    """One retry attempt for a checkpoint call."""

    attempt_id: str
    attempt_number: int
    status: str
    started_at: datetime | None = None
    ended_at: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    failure: FailureRecord | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "attempt_id": self.attempt_id,
            "attempt_number": self.attempt_number,
            "status": self.status,
            "started_at": _dt_to_iso(self.started_at),
            "ended_at": _dt_to_iso(self.ended_at),
            "metadata": self.metadata,
            "failure": self.failure.to_dict() if self.failure else None,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> CheckpointAttemptRecord:
        failure_data = data.get("failure")
        return cls(
            attempt_id=data["attempt_id"],
            attempt_number=data["attempt_number"],
            status=data["status"],
            started_at=_iso_to_dt(data.get("started_at")),
            ended_at=_iso_to_dt(data.get("ended_at")),
            metadata=data.get("metadata", {}),
            failure=FailureRecord.from_dict(failure_data) if failure_data else None,
        )


# ---------------------------------------------------------------------------
# CheckpointCallRecord
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CheckpointCallRecord:
    """One checkpoint call (potentially with multiple attempts)."""

    call_id: str
    invocation_id: str
    name: str
    checkpoint_type: str | None = None
    status: str = "pending"
    started_at: datetime | None = None
    ended_at: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    original_call_id: str | None = None
    upstream_call_ids: tuple[str, ...] = ()
    failure: FailureRecord | None = None
    attempts: tuple[CheckpointAttemptRecord, ...] = ()
    artifacts: tuple[ArtifactRecord, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "call_id": self.call_id,
            "invocation_id": self.invocation_id,
            "name": self.name,
            "checkpoint_type": self.checkpoint_type,
            "status": self.status,
            "started_at": _dt_to_iso(self.started_at),
            "ended_at": _dt_to_iso(self.ended_at),
            "metadata": self.metadata,
            "original_call_id": self.original_call_id,
            "upstream_call_ids": list(self.upstream_call_ids),
            "failure": self.failure.to_dict() if self.failure else None,
            "attempts": [a.to_dict() for a in self.attempts],
            "artifacts": [a.to_dict() for a in self.artifacts],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> CheckpointCallRecord:
        failure_data = data.get("failure")
        return cls(
            call_id=data["call_id"],
            invocation_id=data["invocation_id"],
            name=data["name"],
            checkpoint_type=data.get("checkpoint_type"),
            status=data.get("status", "pending"),
            started_at=_iso_to_dt(data.get("started_at")),
            ended_at=_iso_to_dt(data.get("ended_at")),
            metadata=data.get("metadata", {}),
            original_call_id=data.get("original_call_id"),
            upstream_call_ids=tuple(data.get("upstream_call_ids", ())),
            failure=FailureRecord.from_dict(failure_data) if failure_data else None,
            attempts=tuple(
                CheckpointAttemptRecord.from_dict(a) for a in data.get("attempts", ())
            ),
            artifacts=tuple(
                ArtifactRecord.from_dict(a) for a in data.get("artifacts", ())
            ),
        )


# ---------------------------------------------------------------------------
# WaitRecord
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class WaitRecord:
    """One wait condition in an execution."""

    wait_id: str
    name: str
    status: str = "pending"
    question: str | None = None
    schema: dict[str, Any] | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    entered_at: datetime | None = None
    resolved_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "wait_id": self.wait_id,
            "name": self.name,
            "status": self.status,
            "question": self.question,
            "schema": self.schema,
            "metadata": self.metadata,
            "entered_at": _dt_to_iso(self.entered_at),
            "resolved_at": _dt_to_iso(self.resolved_at),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> WaitRecord:
        return cls(
            wait_id=data["wait_id"],
            name=data["name"],
            status=data.get("status", "pending"),
            question=data.get("question"),
            schema=data.get("schema"),
            metadata=data.get("metadata", {}),
            entered_at=_iso_to_dt(data.get("entered_at")),
            resolved_at=_iso_to_dt(data.get("resolved_at")),
        )


# ---------------------------------------------------------------------------
# LogRecord
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LogRecord:
    """One log entry stored in the execution ledger."""

    message: str
    source: str = "step"
    checkpoint_name: str | None = None
    level: str | None = None
    timestamp: datetime | None = None
    module: str | None = None
    filename: str | None = None
    lineno: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "message": self.message,
            "source": self.source,
            "checkpoint_name": self.checkpoint_name,
            "level": self.level,
            "timestamp": _dt_to_iso(self.timestamp),
            "module": self.module,
            "filename": self.filename,
            "lineno": self.lineno,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> LogRecord:
        return cls(
            message=data["message"],
            source=data.get("source", "step"),
            checkpoint_name=data.get("checkpoint_name"),
            level=data.get("level"),
            timestamp=_iso_to_dt(data.get("timestamp")),
            module=data.get("module"),
            filename=data.get("filename"),
            lineno=data.get("lineno"),
        )


# ---------------------------------------------------------------------------
# ExecutionLedgerRecord
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ExecutionLedgerRecord:
    """Top-level execution record stored in the Dapr state store."""

    exec_id: str
    project: str
    backend: str = "dapr"
    flow_name: str | None = None
    workflow_name: str | None = None
    status: str = "pending"
    created_at: datetime | None = None
    updated_at: datetime | None = None
    ended_at: datetime | None = None
    original_exec_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    status_reason: str | None = None
    frozen_execution_spec: dict[str, Any] | None = None
    checkpoints: tuple[CheckpointCallRecord, ...] = ()
    artifacts: tuple[ArtifactRecord, ...] = ()
    waits: tuple[WaitRecord, ...] = ()
    logs: tuple[LogRecord, ...] = ()
    failure: FailureRecord | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "exec_id": self.exec_id,
            "project": self.project,
            "backend": self.backend,
            "flow_name": self.flow_name,
            "workflow_name": self.workflow_name,
            "status": self.status,
            "created_at": _dt_to_iso(self.created_at),
            "updated_at": _dt_to_iso(self.updated_at),
            "ended_at": _dt_to_iso(self.ended_at),
            "original_exec_id": self.original_exec_id,
            "metadata": self.metadata,
            "status_reason": self.status_reason,
            "frozen_execution_spec": self.frozen_execution_spec,
            "checkpoints": [c.to_dict() for c in self.checkpoints],
            "artifacts": [a.to_dict() for a in self.artifacts],
            "waits": [w.to_dict() for w in self.waits],
            "logs": [log.to_dict() for log in self.logs],
            "failure": self.failure.to_dict() if self.failure else None,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ExecutionLedgerRecord:
        failure_data = data.get("failure")
        return cls(
            exec_id=data["exec_id"],
            project=data["project"],
            backend=data.get("backend", "dapr"),
            flow_name=data.get("flow_name"),
            workflow_name=data.get("workflow_name"),
            status=data.get("status", "pending"),
            created_at=_iso_to_dt(data.get("created_at")),
            updated_at=_iso_to_dt(data.get("updated_at")),
            ended_at=_iso_to_dt(data.get("ended_at")),
            original_exec_id=data.get("original_exec_id"),
            metadata=data.get("metadata", {}),
            status_reason=data.get("status_reason"),
            frozen_execution_spec=data.get("frozen_execution_spec"),
            checkpoints=tuple(
                CheckpointCallRecord.from_dict(c) for c in data.get("checkpoints", ())
            ),
            artifacts=tuple(
                ArtifactRecord.from_dict(a) for a in data.get("artifacts", ())
            ),
            waits=tuple(WaitRecord.from_dict(w) for w in data.get("waits", ())),
            logs=tuple(LogRecord.from_dict(log) for log in data.get("logs", ())),
            failure=FailureRecord.from_dict(failure_data) if failure_data else None,
        )
