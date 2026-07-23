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
"""Replay ORM table."""

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
from kitaru.server.domain.names import MAX_NAME_LENGTH
from kitaru.server.domain.replay import Replay, ReplayStatus

REPLAY_SESSION_UNIQUE_CONSTRAINT = unique_constraint_name(
    "replay", ["experiment_run_id", "original_session_id"]
)
REPLAY_EXPERIMENT_RUN_ID_FOREIGN_KEY = foreign_key_name("replay", ["experiment_run_id"])
REPLAY_REPLAY_CONFIG_ID_FOREIGN_KEY = foreign_key_name("replay", ["replay_config_id"])
REPLAY_AGENT_VERSION_ID_FOREIGN_KEY = foreign_key_name("replay", ["agent_version_id"])
REPLAY_ORIGINAL_SESSION_ID_FOREIGN_KEY = foreign_key_name(
    "replay", ["original_session_id"]
)
REPLAY_RESULT_SESSION_ID_FOREIGN_KEY = foreign_key_name("replay", ["result_session_id"])
REPLAY_RUN_STATUS_INDEX = index_name("replay", ["experiment_run_id", "status"])
REPLAY_ORIGINAL_SESSION_ID_INDEX = index_name("replay", ["original_session_id"])

MAX_STATUS_LENGTH = 16


class ReplaySchema(UUIDPrimaryKeyMixin, TimestampMixin, table=True):
    """Replay table."""

    __tablename__ = "replay"
    __table_args__ = (
        UniqueConstraint(
            "experiment_run_id",
            "original_session_id",
            name=REPLAY_SESSION_UNIQUE_CONSTRAINT,
        ),
        ForeignKeyConstraint(
            ["experiment_run_id"],
            ["experiment_run.id"],
            name=REPLAY_EXPERIMENT_RUN_ID_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["replay_config_id"],
            ["replay_config.id"],
            name=REPLAY_REPLAY_CONFIG_ID_FOREIGN_KEY,
        ),
        ForeignKeyConstraint(
            ["agent_version_id"],
            ["agent_version.id"],
            name=REPLAY_AGENT_VERSION_ID_FOREIGN_KEY,
        ),
        ForeignKeyConstraint(
            ["original_session_id"],
            ["session.id"],
            name=REPLAY_ORIGINAL_SESSION_ID_FOREIGN_KEY,
        ),
        ForeignKeyConstraint(
            ["result_session_id"],
            ["session.id"],
            name=REPLAY_RESULT_SESSION_ID_FOREIGN_KEY,
        ),
        Index(REPLAY_RUN_STATUS_INDEX, "experiment_run_id", "status"),
        Index(REPLAY_ORIGINAL_SESSION_ID_INDEX, "original_session_id"),
    )

    experiment_run_id: uuid.UUID | None = Field(default=None)
    replay_config_id: uuid.UUID = Field(nullable=False)
    agent_version_id: uuid.UUID = Field(nullable=False)
    original_session_id: uuid.UUID = Field(nullable=False)
    result_session_id: uuid.UUID | None = Field(default=None)
    status: str = Field(max_length=MAX_STATUS_LENGTH, nullable=False)
    attempt: int = Field(nullable=False)
    worker_id: str | None = Field(default=None, max_length=MAX_NAME_LENGTH)
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
    def from_domain(cls, replay: Replay) -> "ReplaySchema":
        """Build a row from a domain replay.

        Args:
            replay: Replay to store.

        Returns:
            Row without timestamps set.
        """
        return cls(
            id=replay.id,
            experiment_run_id=replay.experiment_run_id,
            replay_config_id=replay.replay_config_id,
            agent_version_id=replay.agent_version_id,
            original_session_id=replay.original_session_id,
            result_session_id=replay.result_session_id,
            status=replay.status.value,
            attempt=replay.attempt,
            worker_id=replay.worker_id,
            claimed_at=replay.claimed_at,
            heartbeat_at=replay.heartbeat_at,
            started_at=replay.started_at,
            ended_at=replay.ended_at,
            error=replay.error,
            passed=replay.passed,
            score=replay.score,
            scores=replay.scores,
            diff=replay.diff,
        )

    def to_domain(self) -> Replay:
        """Build a domain replay from this row.

        Returns:
            Replay with timestamps set.
        """
        return Replay(
            id=self.id,
            experiment_run_id=self.experiment_run_id,
            replay_config_id=self.replay_config_id,
            agent_version_id=self.agent_version_id,
            original_session_id=self.original_session_id,
            result_session_id=self.result_session_id,
            status=ReplayStatus(self.status),
            attempt=self.attempt,
            worker_id=self.worker_id,
            claimed_at=self.claimed_at,
            heartbeat_at=self.heartbeat_at,
            started_at=self.started_at,
            ended_at=self.ended_at,
            error=self.error,
            passed=self.passed,
            score=self.score,
            scores=self.scores,
            diff=self.diff,
            created=self.created,
            updated=self.updated,
        )
