"""Single-table task hierarchy."""

import uuid
from datetime import datetime
from typing import Any, cast

from sqlalchemy import (
    DateTime,
    ForeignKeyConstraint,
    Index,
    String,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from kitaru.server.adapters.db.orm.base import (
    Base,
    TimestampMixin,
    UUIDPrimaryKeyMixin,
)
from kitaru.server.adapters.db.orm.orm_utils import (
    foreign_key_name,
    index_name,
    unique_constraint_name,
)
from kitaru.server.domain.task import (
    AgentTask,
    EvaluationTask,
    ImportTask,
    Task,
    TaskKind,
    TaskOnFailure,
    TaskStatus,
)

TASK_JOB_FOREIGN_KEY = foreign_key_name("task", ["job_id"])
TASK_AGENT_VERSION_FOREIGN_KEY = foreign_key_name("task", ["agent_version_id"])
TASK_AGENT_FOREIGN_KEY = foreign_key_name("task", ["agent_id"])
TASK_PLUGIN_VERSION_FOREIGN_KEY = foreign_key_name("task", ["plugin_version_id"])
TASK_PAYLOAD_BLOB_FOREIGN_KEY = foreign_key_name("task", ["payload_blob_id"])
TASK_INPUT_SESSION_FOREIGN_KEY = foreign_key_name("task", ["input_session_id"])
TASK_WORKER_FOREIGN_KEY = foreign_key_name("task", ["worker_id"])
TASK_RESULT_SESSION_FOREIGN_KEY = foreign_key_name("task", ["result_session_id"])
TASK_EVALUATION_UNIQUE_CONSTRAINT = unique_constraint_name(
    "task", ["job_id", "input_session_id", "plugin_version_id"]
)
TASK_JOB_STATUS_INDEX = index_name("task", ["job_id", "status"])
TASK_INPUT_SESSION_INDEX = index_name("task", ["input_session_id"])
TASK_PENDING_INDEX = index_name("task", ["pending", "id"])
TASK_PENDING_LABELS_INDEX = index_name("task", ["pending", "labels"])
TASK_STALENESS_INDEX = index_name("task", ["staleness"])


class TaskORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Polymorphic task table."""

    __tablename__ = "task"
    __table_args__ = (
        UniqueConstraint(
            "job_id",
            "input_session_id",
            "plugin_version_id",
            name=TASK_EVALUATION_UNIQUE_CONSTRAINT,
        ),
        Index(TASK_JOB_STATUS_INDEX, "job_id", "status"),
        Index(TASK_INPUT_SESSION_INDEX, "input_session_id"),
        Index(
            TASK_PENDING_INDEX,
            "id",
            postgresql_where=text("status = 'pending'"),
        ),
        Index(
            TASK_PENDING_LABELS_INDEX,
            "labels",
            postgresql_using="gin",
            postgresql_where=text("status = 'pending'"),
        ),
        Index(
            TASK_STALENESS_INDEX,
            text("coalesce(heartbeat_at, claimed_at)"),
            postgresql_where=text("status IN ('claimed', 'running')"),
        ),
        ForeignKeyConstraint(
            ["job_id"], ["job.id"], name=TASK_JOB_FOREIGN_KEY, ondelete="CASCADE"
        ),
        ForeignKeyConstraint(
            ["agent_version_id"],
            ["agent_version.id"],
            name=TASK_AGENT_VERSION_FOREIGN_KEY,
        ),
        ForeignKeyConstraint(["agent_id"], ["agent.id"], name=TASK_AGENT_FOREIGN_KEY),
        ForeignKeyConstraint(
            ["plugin_version_id"],
            ["plugin_version.id"],
            name=TASK_PLUGIN_VERSION_FOREIGN_KEY,
        ),
        ForeignKeyConstraint(
            ["payload_blob_id"],
            ["blob.id"],
            name=TASK_PAYLOAD_BLOB_FOREIGN_KEY,
        ),
        ForeignKeyConstraint(
            ["input_session_id"],
            ["session.id"],
            name=TASK_INPUT_SESSION_FOREIGN_KEY,
        ),
        ForeignKeyConstraint(
            ["worker_id"],
            ["worker.id"],
            name=TASK_WORKER_FOREIGN_KEY,
            ondelete="SET NULL",
        ),
        ForeignKeyConstraint(
            ["result_session_id"],
            ["session.id"],
            name=TASK_RESULT_SESSION_FOREIGN_KEY,
        ),
    )

    kind: Mapped[str] = mapped_column(String(32))
    job_id: Mapped[uuid.UUID]
    agent_version_id: Mapped[uuid.UUID | None]
    agent_id: Mapped[uuid.UUID | None]
    plugin_version_id: Mapped[uuid.UUID | None]
    payload_blob_id: Mapped[uuid.UUID | None]
    input_session_id: Mapped[uuid.UUID | None]
    result_session_id: Mapped[uuid.UUID | None]
    status: Mapped[str] = mapped_column(String(32))
    attempt: Mapped[int]
    on_failure: Mapped[str] = mapped_column(String(32))
    labels: Mapped[dict[str, str]] = mapped_column(JSONB)
    env: Mapped[dict[str, str]] = mapped_column(JSONB)
    worker_id: Mapped[uuid.UUID | None]
    inputs: Mapped[object | None] = mapped_column(JSONB(none_as_null=True))
    claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    cancel_requested_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error: Mapped[str | None]
    result: Mapped[object | None] = mapped_column(JSONB(none_as_null=True))

    @classmethod
    def from_domain(cls, task: Task) -> "TaskORM":
        """Build a row from a task."""
        return cls(
            id=task.id,
            kind=task.kind.value,
            job_id=task.job_id,
            agent_version_id=getattr(task, "agent_version_id", None),
            agent_id=getattr(task, "agent_id", None),
            plugin_version_id=getattr(task, "plugin_version_id", None),
            payload_blob_id=getattr(task, "payload_blob_id", None),
            input_session_id=getattr(task, "input_session_id", None),
            result_session_id=task.result_session_id,
            status=task.status.value,
            attempt=task.attempt,
            on_failure=task.on_failure.value,
            labels=task.labels,
            env=task.env,
            worker_id=task.worker_id,
            inputs=(
                task.inputs
                if isinstance(task, AgentTask)
                else getattr(task, "params", None)
            ),
            claimed_at=task.claimed_at,
            heartbeat_at=task.heartbeat_at,
            cancel_requested_at=task.cancel_requested_at,
            started_at=task.started_at,
            ended_at=task.ended_at,
            error=task.error,
            result=task.result,
        )

    def to_domain(self) -> Task:
        """Build the concrete task from this row."""
        kind = TaskKind(self.kind)
        if kind is TaskKind.AGENT:
            assert self.agent_version_id is not None
            return AgentTask(
                id=self.id,
                job_id=self.job_id,
                agent_version_id=self.agent_version_id,
                inputs=self.inputs,
                status=TaskStatus(self.status),
                attempt=self.attempt,
                on_failure=TaskOnFailure(self.on_failure),
                labels=self.labels,
                env=self.env,
                worker_id=self.worker_id,
                result_session_id=self.result_session_id,
                claimed_at=self.claimed_at,
                heartbeat_at=self.heartbeat_at,
                cancel_requested_at=self.cancel_requested_at,
                started_at=self.started_at,
                ended_at=self.ended_at,
                error=self.error,
                result=self.result,
                created=self.created,
                updated=self.updated,
            )
        if kind is TaskKind.EVALUATOR:
            assert self.plugin_version_id is not None
            assert self.input_session_id is not None
            return EvaluationTask(
                id=self.id,
                job_id=self.job_id,
                plugin_version_id=self.plugin_version_id,
                input_session_id=self.input_session_id,
                params=(
                    cast("dict[str, Any]", self.inputs)
                    if isinstance(self.inputs, dict)
                    else {}
                ),
                status=TaskStatus(self.status),
                attempt=self.attempt,
                on_failure=TaskOnFailure(self.on_failure),
                labels=self.labels,
                env=self.env,
                worker_id=self.worker_id,
                result_session_id=self.result_session_id,
                claimed_at=self.claimed_at,
                heartbeat_at=self.heartbeat_at,
                cancel_requested_at=self.cancel_requested_at,
                started_at=self.started_at,
                ended_at=self.ended_at,
                error=self.error,
                result=self.result,
                created=self.created,
                updated=self.updated,
            )
        assert self.plugin_version_id is not None
        assert self.payload_blob_id is not None
        assert self.agent_id is not None
        return ImportTask(
            id=self.id,
            job_id=self.job_id,
            plugin_version_id=self.plugin_version_id,
            payload_blob_id=self.payload_blob_id,
            agent_id=self.agent_id,
            params=(
                cast("dict[str, Any]", self.inputs)
                if isinstance(self.inputs, dict)
                else {}
            ),
            status=TaskStatus(self.status),
            attempt=self.attempt,
            on_failure=TaskOnFailure(self.on_failure),
            labels=self.labels,
            env=self.env,
            worker_id=self.worker_id,
            result_session_id=self.result_session_id,
            claimed_at=self.claimed_at,
            heartbeat_at=self.heartbeat_at,
            cancel_requested_at=self.cancel_requested_at,
            started_at=self.started_at,
            ended_at=self.ended_at,
            error=self.error,
            result=self.result,
            created=self.created,
            updated=self.updated,
        )

    def copy_from_domain(self, task: Task) -> None:
        """Copy mutable task fields from a domain entity."""
        source = self.from_domain(task)
        for column in (
            "status",
            "attempt",
            "worker_id",
            "result_session_id",
            "claimed_at",
            "heartbeat_at",
            "cancel_requested_at",
            "started_at",
            "ended_at",
            "error",
            "result",
        ):
            setattr(self, column, getattr(source, column))
