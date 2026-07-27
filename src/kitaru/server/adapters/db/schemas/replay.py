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
from typing import Any

from sqlalchemy import ForeignKeyConstraint, Index, Text, UniqueConstraint
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
from kitaru.server.domain.replay import Replay

REPLAY_JOB_ID_UNIQUE_CONSTRAINT = unique_constraint_name("replay", ["job_id"])
REPLAY_OWNER_ID_FOREIGN_KEY = foreign_key_name("replay", ["owner_id"])
REPLAY_JOB_ID_FOREIGN_KEY = foreign_key_name("replay", ["job_id"])
REPLAY_EXPERIMENT_RUN_ID_FOREIGN_KEY = foreign_key_name("replay", ["experiment_run_id"])
REPLAY_REPLAY_CONFIG_ID_FOREIGN_KEY = foreign_key_name("replay", ["replay_config_id"])
REPLAY_INPUT_SESSION_ID_FOREIGN_KEY = foreign_key_name("replay", ["input_session_id"])
REPLAY_EXPERIMENT_RUN_ID_INDEX = index_name("replay", ["experiment_run_id"])
REPLAY_INPUT_SESSION_ID_INDEX = index_name("replay", ["input_session_id"])


class ReplaySchema(UUIDPrimaryKeyMixin, TimestampMixin, table=True):
    """Replay table."""

    __tablename__ = "replay"
    __table_args__ = (
        UniqueConstraint("job_id", name=REPLAY_JOB_ID_UNIQUE_CONSTRAINT),
        ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name=REPLAY_OWNER_ID_FOREIGN_KEY
        ),
        ForeignKeyConstraint(
            ["job_id"],
            ["job.id"],
            name=REPLAY_JOB_ID_FOREIGN_KEY,
            ondelete="CASCADE",
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
            ["input_session_id"],
            ["session.id"],
            name=REPLAY_INPUT_SESSION_ID_FOREIGN_KEY,
        ),
        Index(REPLAY_EXPERIMENT_RUN_ID_INDEX, "experiment_run_id"),
        Index(REPLAY_INPUT_SESSION_ID_INDEX, "input_session_id"),
    )

    owner_id: uuid.UUID = Field(nullable=False)
    job_id: uuid.UUID = Field(nullable=False)
    experiment_run_id: uuid.UUID | None = Field(default=None)
    replay_config_id: uuid.UUID = Field(nullable=False)
    input_session_id: uuid.UUID = Field(nullable=False)
    passed: bool | None = Field(default=None)
    score: float | None = Field(default=None)
    scores: dict[str, float] | None = Field(default=None, sa_type=JSONB)
    diff: dict[str, Any] | None = Field(default=None, sa_type=JSONB)
    error: str | None = Field(default=None, sa_type=Text)

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
            owner_id=replay.owner_id,
            job_id=replay.job_id,
            experiment_run_id=replay.experiment_run_id,
            replay_config_id=replay.replay_config_id,
            input_session_id=replay.input_session_id,
            passed=replay.passed,
            score=replay.score,
            scores=replay.scores,
            diff=replay.diff,
            error=replay.error,
        )

    def to_domain(self) -> Replay:
        """Build a domain replay from this row.

        Returns:
            Replay with timestamps set.
        """
        return Replay(
            id=self.id,
            owner_id=self.owner_id,
            job_id=self.job_id,
            experiment_run_id=self.experiment_run_id,
            replay_config_id=self.replay_config_id,
            input_session_id=self.input_session_id,
            passed=self.passed,
            score=self.score,
            scores=self.scores,
            diff=self.diff,
            error=self.error,
            created=self.created,
            updated=self.updated,
        )
