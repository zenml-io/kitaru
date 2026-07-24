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
"""Experiment run ORM table."""

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
from kitaru.server.domain.experiment_run import (
    ExperimentRun,
    ExperimentRunStatus,
)

EXPERIMENT_RUN_NUMBER_UNIQUE_CONSTRAINT = unique_constraint_name(
    "experiment_run", ["experiment_id", "number"]
)
EXPERIMENT_RUN_EXPERIMENT_ID_FOREIGN_KEY = foreign_key_name(
    "experiment_run", ["experiment_id"]
)
EXPERIMENT_RUN_AGENT_VERSION_ID_FOREIGN_KEY = foreign_key_name(
    "experiment_run", ["agent_version_id"]
)
EXPERIMENT_RUN_OWNER_ID_INDEX = index_name("experiment_run", ["owner_id"])

MAX_STATUS_LENGTH = 16
MAX_EXECUTION_TARGET_LENGTH = 16
MAX_EXECUTOR_HANDLE_LENGTH = 255


class ExperimentRunSchema(UUIDPrimaryKeyMixin, TimestampMixin, table=True):
    """Experiment run table."""

    __tablename__ = "experiment_run"
    __table_args__ = (
        UniqueConstraint(
            "experiment_id", "number", name=EXPERIMENT_RUN_NUMBER_UNIQUE_CONSTRAINT
        ),
        ForeignKeyConstraint(
            ["experiment_id"],
            ["experiment.id"],
            name=EXPERIMENT_RUN_EXPERIMENT_ID_FOREIGN_KEY,
        ),
        ForeignKeyConstraint(
            ["agent_version_id"],
            ["agent_version.id"],
            name=EXPERIMENT_RUN_AGENT_VERSION_ID_FOREIGN_KEY,
        ),
        Index(EXPERIMENT_RUN_OWNER_ID_INDEX, "owner_id"),
    )

    owner_id: uuid.UUID = Field(foreign_key="account.id", nullable=False)
    experiment_id: uuid.UUID = Field(nullable=False)
    number: int = Field(nullable=False)
    status: str = Field(max_length=MAX_STATUS_LENGTH, nullable=False)
    agent_version_id: uuid.UUID = Field(nullable=False)
    score_baselines: bool = Field(nullable=False)
    execution_target: str = Field(
        max_length=MAX_EXECUTION_TARGET_LENGTH,
        nullable=False,
        sa_column_kwargs={"server_default": ExecutionTarget.POOL.value},
    )
    executor_handle: str | None = Field(
        default=None, max_length=MAX_EXECUTOR_HANDLE_LENGTH
    )
    started_at: datetime | None = Field(
        default=None,
        sa_type=DateTime(timezone=True),  # ty: ignore[invalid-argument-type]
    )
    ended_at: datetime | None = Field(
        default=None,
        sa_type=DateTime(timezone=True),  # ty: ignore[invalid-argument-type]
    )
    summary: dict[str, Any] | None = Field(default=None, sa_type=JSONB)
    error: str | None = Field(default=None, sa_type=Text)

    @classmethod
    def from_domain(cls, run: ExperimentRun) -> "ExperimentRunSchema":
        """Build a row from a domain experiment run.

        Args:
            run: Experiment run to store.

        Returns:
            Row without timestamps set.
        """
        return cls(
            id=run.id,
            owner_id=run.owner_id,
            experiment_id=run.experiment_id,
            number=run.number,
            status=run.status.value,
            agent_version_id=run.agent_version_id,
            score_baselines=run.score_baselines,
            execution_target=run.execution_target.value,
            executor_handle=run.executor_handle,
            started_at=run.started_at,
            ended_at=run.ended_at,
            summary=run.summary,
            error=run.error,
        )

    def to_domain(self) -> ExperimentRun:
        """Build a domain experiment run from this row.

        Returns:
            Experiment run with timestamps set.
        """
        return ExperimentRun(
            id=self.id,
            owner_id=self.owner_id,
            experiment_id=self.experiment_id,
            number=self.number,
            status=ExperimentRunStatus(self.status),
            agent_version_id=self.agent_version_id,
            score_baselines=self.score_baselines,
            execution_target=ExecutionTarget(self.execution_target),
            executor_handle=self.executor_handle,
            started_at=self.started_at,
            ended_at=self.ended_at,
            summary=self.summary,
            error=self.error,
            created=self.created,
            updated=self.updated,
        )
