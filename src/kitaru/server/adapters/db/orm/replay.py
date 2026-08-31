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

from sqlalchemy import (
    ForeignKeyConstraint,
    Index,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from kitaru.api_models.v1.replay import BaselineEvaluationMode, ReplayStatus
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
from kitaru.server.domain.replay import Replay

STATUS_LENGTH = 16
BASELINE_EVALUATION_MODE_LENGTH = 16

REPLAY_OWNER_ID_FOREIGN_KEY = foreign_key_name("replay", ["owner_id"])
REPLAY_JOB_ID_FOREIGN_KEY = foreign_key_name("replay", ["job_id"])
REPLAY_EXPERIMENT_RUN_ID_FOREIGN_KEY = foreign_key_name("replay", ["experiment_run_id"])
REPLAY_REPLAY_CONFIG_ID_FOREIGN_KEY = foreign_key_name("replay", ["replay_config_id"])
REPLAY_BASELINE_SESSION_ID_FOREIGN_KEY = foreign_key_name(
    "replay", ["baseline_session_id"]
)
REPLAY_RESULT_SESSION_ID_FOREIGN_KEY = foreign_key_name("replay", ["result_session_id"])
REPLAY_JOB_ID_UNIQUE_CONSTRAINT = unique_constraint_name("replay", ["job_id"])
REPLAY_RUN_BASELINE_UNIQUE_CONSTRAINT = unique_constraint_name(
    "replay", ["experiment_run_id", "baseline_session_id"]
)
REPLAY_EXPERIMENT_RUN_ID_STATUS_INDEX = index_name(
    "replay", ["experiment_run_id", "status"]
)
REPLAY_BASELINE_SESSION_ID_INDEX = index_name("replay", ["baseline_session_id"])
REPLAY_RESULT_SESSION_ID_INDEX = index_name("replay", ["result_session_id"])


class ReplayORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Replay table."""

    __tablename__ = "replay"
    __table_args__ = (
        ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name=REPLAY_OWNER_ID_FOREIGN_KEY
        ),
        ForeignKeyConstraint(
            ["job_id"], ["job.id"], name=REPLAY_JOB_ID_FOREIGN_KEY, ondelete="SET NULL"
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
            ["baseline_session_id"],
            ["session.id"],
            name=REPLAY_BASELINE_SESSION_ID_FOREIGN_KEY,
        ),
        ForeignKeyConstraint(
            ["result_session_id"],
            ["session.id"],
            name=REPLAY_RESULT_SESSION_ID_FOREIGN_KEY,
        ),
        UniqueConstraint("job_id", name=REPLAY_JOB_ID_UNIQUE_CONSTRAINT),
        UniqueConstraint(
            "experiment_run_id",
            "baseline_session_id",
            name=REPLAY_RUN_BASELINE_UNIQUE_CONSTRAINT,
        ),
        Index(REPLAY_EXPERIMENT_RUN_ID_STATUS_INDEX, "experiment_run_id", "status"),
        Index(REPLAY_BASELINE_SESSION_ID_INDEX, "baseline_session_id"),
        Index(REPLAY_RESULT_SESSION_ID_INDEX, "result_session_id"),
    )

    owner_id: Mapped[uuid.UUID]
    job_id: Mapped[uuid.UUID | None]
    experiment_run_id: Mapped[uuid.UUID | None]
    replay_config_id: Mapped[uuid.UUID]
    baseline_session_id: Mapped[uuid.UUID]
    result_session_id: Mapped[uuid.UUID | None]
    baseline_evaluation_mode: Mapped[str] = mapped_column(
        String(BASELINE_EVALUATION_MODE_LENGTH)
    )
    status: Mapped[str] = mapped_column(String(STATUS_LENGTH))
    error: Mapped[str | None] = mapped_column(Text)

    @classmethod
    def from_domain(cls, replay: Replay) -> "ReplayORM":
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
            baseline_session_id=replay.baseline_session_id,
            result_session_id=replay.result_session_id,
            baseline_evaluation_mode=replay.baseline_evaluation_mode.value,
            status=replay.status.value,
            error=replay.error,
        )

    def apply(self, replay: Replay) -> None:
        """Copy a domain replay's mutable fields onto this row.

        Args:
            replay: Replay with modified fields.
        """
        self.result_session_id = replay.result_session_id
        self.status = replay.status.value
        self.error = replay.error

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
            baseline_session_id=self.baseline_session_id,
            result_session_id=self.result_session_id,
            baseline_evaluation_mode=BaselineEvaluationMode(
                self.baseline_evaluation_mode
            ),
            status=ReplayStatus(self.status),
            error=self.error,
            created=self.created,
            updated=self.updated,
        )
