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
"""Task ORM table."""

import uuid
from datetime import datetime
from typing import Any

from pydantic import TypeAdapter
from sqlalchemy import (
    DateTime,
    ForeignKeyConstraint,
    Index,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from kitaru.api_models.v1.task import TaskKind, TaskOnFailure, TaskStatus
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
    TERMINAL_TASK_STATUSES,
    AgentTask,
    AnalysisTask,
    EvaluationTask,
    ImportTask,
    Task,
)

_INPUT_SESSION_IDS_ADAPTER: TypeAdapter[list[uuid.UUID]] = TypeAdapter(list[uuid.UUID])

KIND_LENGTH = 16
STATUS_LENGTH = 16
ON_FAILURE_LENGTH = 16

TASK_JOB_ID_FOREIGN_KEY = foreign_key_name("task", ["job_id"])
TASK_WORKER_ID_FOREIGN_KEY = foreign_key_name("task", ["worker_id"])
TASK_AGENT_ID_FOREIGN_KEY = foreign_key_name("task", ["agent_id"])
TASK_EVALUATOR_PAIR_UNIQUE_CONSTRAINT = unique_constraint_name(
    "task", ["job_id", "input_session_id", "plugin_version_id"]
)
TASK_JOB_ID_STATUS_INDEX = index_name("task", ["job_id", "status"])
TASK_INPUT_SESSION_ID_INDEX = index_name("task", ["input_session_id"])
TASK_IMPORT_ID_INDEX = index_name("task", ["import_id"])
TASK_AGENT_ID_INDEX = index_name("task", ["agent_id"])
# Partial indexes covering the queue scans: a scope claiming everything reads
# pending rows in id order, a kind claim reads them per kind, and a
# version-pinned claim reads them per agent version. The staleness sweep
# reads in-flight rows by their last sign of life. Selectors filter the rows
# the claim scans fetch and use no index.
TASK_PENDING_ID_INDEX = index_name("task", ["id"])
TASK_PENDING_KIND_INDEX = index_name("task", ["kind", "id"])
TASK_PENDING_AGENT_VERSION_INDEX = index_name("task", ["agent_version_id", "id"])
TASK_STALENESS_INDEX = index_name("task", ["heartbeat_at", "claimed_at"])

TERMINAL_STATUS_VALUES = [status.value for status in TERMINAL_TASK_STATUSES]

PENDING_PREDICATE = "status = 'pending'"
IN_FLIGHT_PREDICATE = "status IN ('claimed', 'running')"
LAST_SEEN_EXPRESSION = "coalesce(heartbeat_at, claimed_at)"


class TaskORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Task table."""

    __tablename__ = "task"
    __table_args__ = (
        ForeignKeyConstraint(
            ["job_id"], ["job.id"], name=TASK_JOB_ID_FOREIGN_KEY, ondelete="CASCADE"
        ),
        ForeignKeyConstraint(
            ["worker_id"],
            ["worker.id"],
            name=TASK_WORKER_ID_FOREIGN_KEY,
            ondelete="SET NULL",
        ),
        ForeignKeyConstraint(
            ["agent_id"],
            ["agent.id"],
            name=TASK_AGENT_ID_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        UniqueConstraint(
            "job_id",
            "input_session_id",
            "plugin_version_id",
            name=TASK_EVALUATOR_PAIR_UNIQUE_CONSTRAINT,
        ),
        Index(TASK_JOB_ID_STATUS_INDEX, "job_id", "status"),
        Index(TASK_INPUT_SESSION_ID_INDEX, "input_session_id"),
        Index(TASK_IMPORT_ID_INDEX, "import_id"),
        Index(TASK_AGENT_ID_INDEX, "agent_id"),
        Index(TASK_PENDING_ID_INDEX, "id", postgresql_where=text(PENDING_PREDICATE)),
        Index(
            TASK_PENDING_KIND_INDEX,
            "kind",
            "id",
            postgresql_where=text(PENDING_PREDICATE),
        ),
        Index(
            TASK_PENDING_AGENT_VERSION_INDEX,
            "agent_version_id",
            "id",
            postgresql_where=text(PENDING_PREDICATE),
        ),
        Index(
            TASK_STALENESS_INDEX,
            text(LAST_SEEN_EXPRESSION),
            postgresql_where=text(IN_FLIGHT_PREDICATE),
        ),
    )

    kind: Mapped[str] = mapped_column(String(KIND_LENGTH))
    job_id: Mapped[uuid.UUID]
    # A task names the rows it takes as input by id alone, without a foreign
    # key. The queue holds work that outlives the rows it points at, so
    # deleting one neither cascades into the queue nor is blocked by it, and a
    # claim that cannot resolve an input cancels the task instead.
    agent_version_id: Mapped[uuid.UUID | None]
    plugin_version_id: Mapped[uuid.UUID | None]
    input_session_id: Mapped[uuid.UUID | None]
    import_id: Mapped[uuid.UUID | None]
    agent_id: Mapped[uuid.UUID | None]
    input_session_ids: Mapped[list[Any] | None] = mapped_column(
        JSONB(none_as_null=True)
    )
    status: Mapped[str] = mapped_column(String(STATUS_LENGTH))
    attempt: Mapped[int]
    on_failure: Mapped[str] = mapped_column(String(ON_FAILURE_LENGTH))
    labels: Mapped[dict[str, str]] = mapped_column(JSONB)
    env: Mapped[dict[str, str]] = mapped_column(JSONB)
    worker_id: Mapped[uuid.UUID | None]
    inputs: Mapped[Any | None] = mapped_column(JSONB(none_as_null=True))
    claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    cancel_requested_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error: Mapped[str | None] = mapped_column(Text)
    result: Mapped[Any | None] = mapped_column(JSONB(none_as_null=True))

    @classmethod
    def from_domain(cls, task: Task) -> "TaskORM":
        """Build a row from a domain task.

        Args:
            task: Task to store.

        Returns:
            Row without timestamps set.
        """
        row = cls(
            id=task.id,
            kind=task.kind.value,
            job_id=task.job_id,
            status=task.status.value,
            attempt=task.attempt,
            on_failure=task.on_failure.value,
            labels=task.labels,
            env=task.env,
            worker_id=task.worker_id,
            claimed_at=task.claimed_at,
            heartbeat_at=task.heartbeat_at,
            cancel_requested_at=task.cancel_requested_at,
            started_at=task.started_at,
            ended_at=task.ended_at,
            error=task.error,
            result=task.result,
        )
        if isinstance(task, AgentTask):
            row.agent_version_id = task.agent_version_id
            row.inputs = task.inputs
        elif isinstance(task, EvaluationTask):
            row.plugin_version_id = task.plugin_version_id
            row.input_session_id = task.input_session_id
            row.inputs = task.params
        elif isinstance(task, ImportTask):
            row.import_id = task.import_id
        elif isinstance(task, AnalysisTask):
            row.plugin_version_id = task.plugin_version_id
            row.agent_id = task.agent_id
            row.input_session_ids = _INPUT_SESSION_IDS_ADAPTER.dump_python(
                task.input_session_ids, mode="json"
            )
            row.inputs = task.params
        return row

    def apply(self, task: Task) -> None:
        """Copy a domain task's mutable fields onto this row.

        Args:
            task: Task with modified fields.
        """
        self.status = task.status.value
        self.attempt = task.attempt
        self.worker_id = task.worker_id
        self.claimed_at = task.claimed_at
        self.heartbeat_at = task.heartbeat_at
        self.cancel_requested_at = task.cancel_requested_at
        self.started_at = task.started_at
        self.ended_at = task.ended_at
        self.error = task.error
        self.result = task.result

    def to_domain(self) -> Task:
        """Build a domain task from this row, dispatching on the kind column.

        Raises:
            ValueError: The kind column holds an unknown value.

        Returns:
            Task with timestamps set.
        """
        shared: dict[str, Any] = {
            "id": self.id,
            "job_id": self.job_id,
            "status": TaskStatus(self.status),
            "attempt": self.attempt,
            "on_failure": TaskOnFailure(self.on_failure),
            "labels": self.labels,
            "env": self.env,
            "worker_id": self.worker_id,
            "claimed_at": self.claimed_at,
            "heartbeat_at": self.heartbeat_at,
            "cancel_requested_at": self.cancel_requested_at,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "error": self.error,
            "result": self.result,
            "created": self.created,
            "updated": self.updated,
        }
        kind = TaskKind(self.kind)
        if kind is TaskKind.AGENT:
            assert self.agent_version_id is not None
            return AgentTask(
                agent_version_id=self.agent_version_id, inputs=self.inputs, **shared
            )
        if kind is TaskKind.EVALUATOR:
            assert self.plugin_version_id is not None
            assert self.input_session_id is not None
            return EvaluationTask(
                plugin_version_id=self.plugin_version_id,
                input_session_id=self.input_session_id,
                params=self.inputs if self.inputs is not None else {},
                **shared,
            )
        if kind is TaskKind.IMPORTER:
            assert self.import_id is not None
            return ImportTask(import_id=self.import_id, **shared)
        if kind is TaskKind.ANALYZER:
            assert self.plugin_version_id is not None
            assert self.agent_id is not None
            assert self.input_session_ids is not None
            return AnalysisTask(
                plugin_version_id=self.plugin_version_id,
                agent_id=self.agent_id,
                input_session_ids=_INPUT_SESSION_IDS_ADAPTER.validate_python(
                    self.input_session_ids
                ),
                params=self.inputs if self.inputs is not None else {},
                **shared,
            )
        raise ValueError(f"Unknown task kind '{self.kind}'")
