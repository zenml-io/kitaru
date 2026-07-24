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
    Job,
    JobKind,
    JobStatus,
    Replay,
    SessionRun,
)
from kitaru.server.domain.names import MAX_NAME_LENGTH

JOB_SESSION_UNIQUE_CONSTRAINT = unique_constraint_name(
    "job", ["experiment_run_id", "original_session_id"]
)
JOB_EXPERIMENT_RUN_ID_FOREIGN_KEY = foreign_key_name("job", ["experiment_run_id"])
JOB_REPLAY_CONFIG_ID_FOREIGN_KEY = foreign_key_name("job", ["replay_config_id"])
JOB_AGENT_VERSION_ID_FOREIGN_KEY = foreign_key_name("job", ["agent_version_id"])
JOB_ORIGINAL_SESSION_ID_FOREIGN_KEY = foreign_key_name("job", ["original_session_id"])
JOB_RESULT_SESSION_ID_FOREIGN_KEY = foreign_key_name("job", ["result_session_id"])
JOB_WORKER_ID_FOREIGN_KEY = foreign_key_name("job", ["worker_id"])
JOB_RUN_STATUS_INDEX = index_name("job", ["experiment_run_id", "status"])
JOB_ORIGINAL_SESSION_ID_INDEX = index_name("job", ["original_session_id"])

MAX_KIND_LENGTH = 16
MAX_STATUS_LENGTH = 16
MAX_EXECUTION_TARGET_LENGTH = 16
MAX_EXECUTOR_HANDLE_LENGTH = 255


class JobSchema(UUIDPrimaryKeyMixin, TimestampMixin, table=True):
    """Job table."""

    __tablename__ = "job"
    __table_args__ = (
        UniqueConstraint(
            "experiment_run_id",
            "original_session_id",
            name=JOB_SESSION_UNIQUE_CONSTRAINT,
        ),
        ForeignKeyConstraint(
            ["experiment_run_id"],
            ["experiment_run.id"],
            name=JOB_EXPERIMENT_RUN_ID_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["replay_config_id"],
            ["replay_config.id"],
            name=JOB_REPLAY_CONFIG_ID_FOREIGN_KEY,
        ),
        ForeignKeyConstraint(
            ["agent_version_id"],
            ["agent_version.id"],
            name=JOB_AGENT_VERSION_ID_FOREIGN_KEY,
        ),
        ForeignKeyConstraint(
            ["original_session_id"],
            ["session.id"],
            name=JOB_ORIGINAL_SESSION_ID_FOREIGN_KEY,
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
        Index(JOB_RUN_STATUS_INDEX, "experiment_run_id", "status"),
        Index(JOB_ORIGINAL_SESSION_ID_INDEX, "original_session_id"),
    )

    kind: str = Field(max_length=MAX_KIND_LENGTH, nullable=False)
    experiment_run_id: uuid.UUID | None = Field(default=None)
    replay_config_id: uuid.UUID | None = Field(default=None)
    agent_version_id: uuid.UUID = Field(nullable=False)
    original_session_id: uuid.UUID | None = Field(default=None)
    result_session_id: uuid.UUID | None = Field(default=None)
    status: str = Field(max_length=MAX_STATUS_LENGTH, nullable=False)
    attempt: int = Field(nullable=False)
    worker_id: uuid.UUID | None = Field(default=None)
    execution_target: str | None = Field(
        default=None, max_length=MAX_EXECUTION_TARGET_LENGTH
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
    passed: bool | None = Field(default=None)
    score: float | None = Field(default=None)
    scores: dict[str, float] | None = Field(default=None, sa_type=JSONB)
    diff: dict[str, Any] | None = Field(default=None, sa_type=JSONB)

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
            execution_target=None
            if job.execution_target is None
            else job.execution_target.value,
            executor_handle=job.executor_handle,
            claimed_at=job.claimed_at,
            heartbeat_at=job.heartbeat_at,
            started_at=job.started_at,
            ended_at=job.ended_at,
            error=job.error,
        )
        if isinstance(job, Replay):
            row.experiment_run_id = job.experiment_run_id
            row.replay_config_id = job.replay_config_id
            row.original_session_id = job.original_session_id
            row.passed = job.passed
            row.score = job.score
            row.scores = job.scores
            row.diff = job.diff
        elif isinstance(job, SessionRun):
            row.inputs = job.inputs
            row.name = job.name
        return row

    def to_domain(self) -> Job:
        """Build a domain job from this row.

        Returns:
            Replay or session run by kind, with timestamps set.
        """
        shared: dict[str, Any] = {
            "id": self.id,
            "agent_version_id": self.agent_version_id,
            "result_session_id": self.result_session_id,
            "status": JobStatus(self.status),
            "attempt": self.attempt,
            "worker_id": self.worker_id,
            "execution_target": None
            if self.execution_target is None
            else ExecutionTarget(self.execution_target),
            "executor_handle": self.executor_handle,
            "claimed_at": self.claimed_at,
            "heartbeat_at": self.heartbeat_at,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "error": self.error,
            "created": self.created,
            "updated": self.updated,
        }
        if JobKind(self.kind) is JobKind.SESSION_RUN:
            return SessionRun(inputs=self.inputs, name=self.name, **shared)
        assert self.replay_config_id is not None
        assert self.original_session_id is not None
        return Replay(
            experiment_run_id=self.experiment_run_id,
            replay_config_id=self.replay_config_id,
            original_session_id=self.original_session_id,
            passed=self.passed,
            score=self.score,
            scores=self.scores,
            diff=self.diff,
            **shared,
        )
