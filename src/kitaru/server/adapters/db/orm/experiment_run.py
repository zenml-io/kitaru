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

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKeyConstraint,
    Index,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from kitaru.api_models.v1.experiment_run import ExperimentRunStatus
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
from kitaru.server.domain.experiment_run import ExperimentRun

STATUS_LENGTH = 16

EXPERIMENT_RUN_OWNER_ID_FOREIGN_KEY = foreign_key_name("experiment_run", ["owner_id"])
EXPERIMENT_RUN_EXPERIMENT_ID_FOREIGN_KEY = foreign_key_name(
    "experiment_run", ["experiment_id"]
)
EXPERIMENT_RUN_COHORT_VERSION_ID_FOREIGN_KEY = foreign_key_name(
    "experiment_run", ["cohort_version_id"]
)
EXPERIMENT_RUN_AGENT_VERSION_ID_FOREIGN_KEY = foreign_key_name(
    "experiment_run", ["agent_version_id"]
)
EXPERIMENT_RUN_NUMBER_UNIQUE_CONSTRAINT = unique_constraint_name(
    "experiment_run", ["experiment_id", "number"]
)
EXPERIMENT_RUN_AGENT_VERSION_ID_INDEX = index_name(
    "experiment_run", ["agent_version_id"]
)
EXPERIMENT_RUN_COHORT_VERSION_ID_INDEX = index_name(
    "experiment_run", ["cohort_version_id"]
)


class ExperimentRunORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Experiment run table."""

    __tablename__ = "experiment_run"
    __table_args__ = (
        ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name=EXPERIMENT_RUN_OWNER_ID_FOREIGN_KEY
        ),
        ForeignKeyConstraint(
            ["experiment_id"],
            ["experiment.id"],
            name=EXPERIMENT_RUN_EXPERIMENT_ID_FOREIGN_KEY,
        ),
        ForeignKeyConstraint(
            ["cohort_version_id"],
            ["cohort_version.id"],
            name=EXPERIMENT_RUN_COHORT_VERSION_ID_FOREIGN_KEY,
        ),
        ForeignKeyConstraint(
            ["agent_version_id"],
            ["agent_version.id"],
            name=EXPERIMENT_RUN_AGENT_VERSION_ID_FOREIGN_KEY,
        ),
        UniqueConstraint(
            "experiment_id", "number", name=EXPERIMENT_RUN_NUMBER_UNIQUE_CONSTRAINT
        ),
        Index(EXPERIMENT_RUN_AGENT_VERSION_ID_INDEX, "agent_version_id"),
        Index(EXPERIMENT_RUN_COHORT_VERSION_ID_INDEX, "cohort_version_id"),
    )

    owner_id: Mapped[uuid.UUID]
    experiment_id: Mapped[uuid.UUID]
    number: Mapped[int]
    status: Mapped[str] = mapped_column(String(STATUS_LENGTH))
    cohort_version_id: Mapped[uuid.UUID]
    agent_version_id: Mapped[uuid.UUID]
    evaluate_baselines: Mapped[bool] = mapped_column(Boolean)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error: Mapped[str | None] = mapped_column(Text)

    @classmethod
    def from_domain(cls, run: ExperimentRun) -> "ExperimentRunORM":
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
            cohort_version_id=run.cohort_version_id,
            agent_version_id=run.agent_version_id,
            evaluate_baselines=run.evaluate_baselines,
            started_at=run.started_at,
            ended_at=run.ended_at,
            error=run.error,
        )

    def apply(self, run: ExperimentRun) -> None:
        """Copy a domain experiment run's mutable fields onto this row.

        Args:
            run: Experiment run with modified fields.
        """
        self.status = run.status.value
        self.started_at = run.started_at
        self.ended_at = run.ended_at
        self.error = run.error

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
            cohort_version_id=self.cohort_version_id,
            agent_version_id=self.agent_version_id,
            evaluate_baselines=self.evaluate_baselines,
            started_at=self.started_at,
            ended_at=self.ended_at,
            error=self.error,
            created=self.created,
            updated=self.updated,
        )
