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
"""Job ORM table."""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    DateTime,
    ForeignKeyConstraint,
    Index,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field

from kitaru.server.adapters.db.schemas.base import (
    TimestampMixin,
    UUIDPrimaryKeyMixin,
)
from kitaru.server.adapters.db.schemas.schema_utils import (
    foreign_key_name,
    index_name,
    unique_constraint_name,
)
from kitaru.server.domain.execution import ExecutionTarget
from kitaru.server.domain.job import (
    Import,
    Job,
    JobKind,
    JobStatus,
    ReplayJob,
    Score,
    SessionRun,
)
from kitaru.server.domain.names import MAX_NAME_LENGTH
from kitaru.server.domain.replay_config import parse_scorer_config

JOB_SESSION_UNIQUE_CONSTRAINT = unique_constraint_name(
    "job", ["experiment_run_id", "input_session_id"]
)
JOB_SCORER_UNIQUE_CONSTRAINT = unique_constraint_name(
    "job", ["parent_job_id", "input_session_id", "scorer_name"]
)
JOB_EXPERIMENT_RUN_ID_FOREIGN_KEY = foreign_key_name("job", ["experiment_run_id"])
JOB_AGENT_VERSION_ID_FOREIGN_KEY = foreign_key_name("job", ["agent_version_id"])
JOB_AGENT_ID_FOREIGN_KEY = foreign_key_name("job", ["agent_id"])
JOB_INPUT_SESSION_ID_FOREIGN_KEY = foreign_key_name("job", ["input_session_id"])
JOB_RESULT_SESSION_ID_FOREIGN_KEY = foreign_key_name("job", ["result_session_id"])
JOB_WORKER_ID_FOREIGN_KEY = foreign_key_name("job", ["worker_id"])
JOB_PARENT_JOB_ID_FOREIGN_KEY = foreign_key_name("job", ["parent_job_id"])
JOB_PLUGIN_VERSION_ID_FOREIGN_KEY = foreign_key_name("job", ["plugin_version_id"])
JOB_PAYLOAD_BLOB_ID_FOREIGN_KEY = foreign_key_name("job", ["payload_blob_id"])
JOB_RUN_STATUS_INDEX = index_name("job", ["experiment_run_id", "status"])
JOB_INPUT_SESSION_ID_INDEX = index_name("job", ["input_session_id"])
JOB_PARENT_JOB_ID_INDEX = index_name("job", ["parent_job_id"])
JOB_PENDING_INDEX = index_name("job", ["pending", "id"])
JOB_ACTIVE_HEARTBEAT_INDEX = index_name("job", ["active", "heartbeat_at"])

# Partial index predicates, kept as literals so the migration and the
# metadata compare byte for byte.
JOB_PENDING_PREDICATE = "status = 'pending'"
JOB_ACTIVE_PREDICATE = "status IN ('claimed', 'running')"
JOB_ACTIVE_HEARTBEAT_EXPRESSION = "coalesce(heartbeat_at, claimed_at)"

MAX_KIND_LENGTH = 16
MAX_STATUS_LENGTH = 16
MAX_EXECUTION_TARGET_LENGTH = 16
MAX_EXECUTOR_HANDLE_LENGTH = 255
MAX_SCORER_NAME_LENGTH = 255


class JobSchema(UUIDPrimaryKeyMixin, TimestampMixin, table=True):
    """Job table."""

    __tablename__ = "job"
    __table_args__ = (
        UniqueConstraint(
            "experiment_run_id",
            "input_session_id",
            name=JOB_SESSION_UNIQUE_CONSTRAINT,
        ),
        UniqueConstraint(
            "parent_job_id",
            "input_session_id",
            "scorer_name",
            name=JOB_SCORER_UNIQUE_CONSTRAINT,
        ),
        ForeignKeyConstraint(
            ["experiment_run_id"],
            ["experiment_run.id"],
            name=JOB_EXPERIMENT_RUN_ID_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["agent_version_id"],
            ["agent_version.id"],
            name=JOB_AGENT_VERSION_ID_FOREIGN_KEY,
        ),
        ForeignKeyConstraint(
            ["agent_id"],
            ["agent.id"],
            name=JOB_AGENT_ID_FOREIGN_KEY,
        ),
        ForeignKeyConstraint(
            ["input_session_id"],
            ["session.id"],
            name=JOB_INPUT_SESSION_ID_FOREIGN_KEY,
        ),
        ForeignKeyConstraint(
            ["result_session_id"],
            ["session.id"],
            name=JOB_RESULT_SESSION_ID_FOREIGN_KEY,
        ),
        ForeignKeyConstraint(
            ["worker_id"],
            ["worker.id"],
            name=JOB_WORKER_ID_FOREIGN_KEY,
            ondelete="SET NULL",
        ),
        ForeignKeyConstraint(
            ["parent_job_id"],
            ["job.id"],
            name=JOB_PARENT_JOB_ID_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["plugin_version_id"],
            ["plugin_version.id"],
            name=JOB_PLUGIN_VERSION_ID_FOREIGN_KEY,
        ),
        ForeignKeyConstraint(
            ["payload_blob_id"],
            ["blob.id"],
            name=JOB_PAYLOAD_BLOB_ID_FOREIGN_KEY,
        ),
        Index(JOB_RUN_STATUS_INDEX, "experiment_run_id", "status"),
        Index(JOB_INPUT_SESSION_ID_INDEX, "input_session_id"),
        Index(JOB_PARENT_JOB_ID_INDEX, "parent_job_id"),
        Index(
            JOB_PENDING_INDEX,
            "id",
            postgresql_where=text(JOB_PENDING_PREDICATE),
        ),
        Index(
            JOB_ACTIVE_HEARTBEAT_INDEX,
            text(JOB_ACTIVE_HEARTBEAT_EXPRESSION),
            postgresql_where=text(JOB_ACTIVE_PREDICATE),
        ),
    )

    kind: str = Field(max_length=MAX_KIND_LENGTH, nullable=False)
    experiment_run_id: uuid.UUID | None = Field(default=None)
    agent_version_id: uuid.UUID | None = Field(default=None)
    agent_id: uuid.UUID | None = Field(default=None)
    parent_job_id: uuid.UUID | None = Field(default=None)
    plugin_version_id: uuid.UUID | None = Field(default=None)
    payload_blob_id: uuid.UUID | None = Field(default=None)
    scorer_name: str | None = Field(default=None, max_length=MAX_SCORER_NAME_LENGTH)
    scorer_config: dict[str, Any] | None = Field(default=None, sa_type=JSONB)
    input_session_id: uuid.UUID | None = Field(default=None)
    result_session_id: uuid.UUID | None = Field(default=None)
    status: str = Field(max_length=MAX_STATUS_LENGTH, nullable=False)
    attempt: int = Field(nullable=False)
    worker_id: uuid.UUID | None = Field(default=None)
    execution_target: str = Field(
        max_length=MAX_EXECUTION_TARGET_LENGTH, nullable=False
    )
    executor_handle: str | None = Field(
        default=None, max_length=MAX_EXECUTOR_HANDLE_LENGTH
    )
    inputs: Any = Field(default=None, sa_type=JSONB)
    name: str | None = Field(default=None, max_length=MAX_NAME_LENGTH)
    claimed_at: datetime | None = Field(
        default=None,
        sa_type=DateTime(timezone=True),  # ty: ignore[invalid-argument-type]
    )
    heartbeat_at: datetime | None = Field(
        default=None,
        sa_type=DateTime(timezone=True),  # ty: ignore[invalid-argument-type]
    )
    started_at: datetime | None = Field(
        default=None,
        sa_type=DateTime(timezone=True),  # ty: ignore[invalid-argument-type]
    )
    ended_at: datetime | None = Field(
        default=None,
        sa_type=DateTime(timezone=True),  # ty: ignore[invalid-argument-type]
    )
    error: str | None = Field(default=None, sa_type=Text)
    result: Any = Field(default=None, sa_type=JSONB)

    @classmethod
    def from_domain(cls, job: Job) -> "JobSchema":
        """Build a row from a domain job.

        Args:
            job: Job to store.

        Returns:
            Row without timestamps set.
        """
        row = cls(
            id=job.id,
            kind=job.kind.value,
            agent_version_id=job.agent_version_id,
            result_session_id=job.result_session_id,
            status=job.status.value,
            attempt=job.attempt,
            worker_id=job.worker_id,
            execution_target=job.execution_target.value,
            executor_handle=job.executor_handle,
            claimed_at=job.claimed_at,
            heartbeat_at=job.heartbeat_at,
            started_at=job.started_at,
            ended_at=job.ended_at,
            error=job.error,
            result=job.result,
        )
        if isinstance(job, ReplayJob):
            row.experiment_run_id = job.experiment_run_id
            row.input_session_id = job.input_session_id
        elif isinstance(job, SessionRun):
            row.inputs = job.inputs
            row.name = job.name
        elif isinstance(job, Score):
            row.parent_job_id = job.parent_job_id
            row.input_session_id = job.input_session_id
            row.plugin_version_id = job.plugin_version_id
            row.scorer_name = job.scorer_config.name
            row.scorer_config = job.scorer_config.model_dump(mode="json")
        elif isinstance(job, Import):
            row.plugin_version_id = job.plugin_version_id
            row.payload_blob_id = job.payload_blob_id
            row.agent_id = job.agent_id
            row.inputs = job.inputs
        return row

    def to_domain(self) -> Job:
        """Build a domain job from this row.

        Returns:
            Replay job, session run, score, or import by kind, with
            timestamps set.
        """
        shared: dict[str, Any] = {
            "id": self.id,
            "agent_version_id": self.agent_version_id,
            "result_session_id": self.result_session_id,
            "status": JobStatus(self.status),
            "attempt": self.attempt,
            "worker_id": self.worker_id,
            "execution_target": ExecutionTarget(self.execution_target),
            "executor_handle": self.executor_handle,
            "claimed_at": self.claimed_at,
            "heartbeat_at": self.heartbeat_at,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "error": self.error,
            "result": self.result,
            "created": self.created,
            "updated": self.updated,
        }
        kind = JobKind(self.kind)
        if kind is JobKind.SESSION_RUN:
            return SessionRun(inputs=self.inputs, name=self.name, **shared)
        if kind is JobKind.IMPORT:
            assert self.plugin_version_id is not None
            assert self.payload_blob_id is not None
            assert self.agent_id is not None
            return Import(
                plugin_version_id=self.plugin_version_id,
                payload_blob_id=self.payload_blob_id,
                agent_id=self.agent_id,
                inputs=self.inputs,
                **shared,
            )
        assert self.input_session_id is not None
        if kind is JobKind.SCORE:
            assert self.scorer_config is not None
            return Score(
                parent_job_id=self.parent_job_id,
                input_session_id=self.input_session_id,
                plugin_version_id=self.plugin_version_id,
                scorer_config=parse_scorer_config(self.scorer_config),
                **shared,
            )
        return ReplayJob(
            experiment_run_id=self.experiment_run_id,
            input_session_id=self.input_session_id,
            **shared,
        )
