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
"""Experiment ORM table."""

import uuid

from sqlalchemy import ForeignKeyConstraint, Index, Text, UniqueConstraint
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
from kitaru.server.domain.experiment import Experiment
from kitaru.server.domain.names import MAX_NAME_LENGTH

EXPERIMENT_NAME_UNIQUE_CONSTRAINT = unique_constraint_name("experiment", ["name"])
EXPERIMENT_COHORT_ID_FOREIGN_KEY = foreign_key_name("experiment", ["cohort_id"])
EXPERIMENT_REPLAY_CONFIG_ID_FOREIGN_KEY = foreign_key_name(
    "experiment", ["replay_config_id"]
)
EXPERIMENT_OWNER_ID_INDEX = index_name("experiment", ["owner_id"])


class ExperimentSchema(UUIDPrimaryKeyMixin, TimestampMixin, table=True):
    """Experiment table."""

    __tablename__ = "experiment"
    __table_args__ = (
        UniqueConstraint("name", name=EXPERIMENT_NAME_UNIQUE_CONSTRAINT),
        ForeignKeyConstraint(
            ["cohort_id"], ["cohort.id"], name=EXPERIMENT_COHORT_ID_FOREIGN_KEY
        ),
        ForeignKeyConstraint(
            ["replay_config_id"],
            ["replay_config.id"],
            name=EXPERIMENT_REPLAY_CONFIG_ID_FOREIGN_KEY,
        ),
        Index(EXPERIMENT_OWNER_ID_INDEX, "owner_id"),
    )

    owner_id: uuid.UUID = Field(foreign_key="account.id", nullable=False)
    name: str = Field(max_length=MAX_NAME_LENGTH, nullable=False)
    description: str | None = Field(default=None, sa_type=Text)
    cohort_id: uuid.UUID = Field(nullable=False)
    replay_config_id: uuid.UUID = Field(nullable=False)

    @classmethod
    def from_domain(cls, experiment: Experiment) -> "ExperimentSchema":
        """Build a row from a domain experiment.

        Args:
            experiment: Experiment to store.

        Returns:
            Row without timestamps set.
        """
        return cls(
            id=experiment.id,
            owner_id=experiment.owner_id,
            name=experiment.name,
            description=experiment.description,
            cohort_id=experiment.cohort_id,
            replay_config_id=experiment.replay_config_id,
        )

    def to_domain(self) -> Experiment:
        """Build a domain experiment from this row.

        Returns:
            Experiment with timestamps set.
        """
        return Experiment(
            id=self.id,
            owner_id=self.owner_id,
            name=self.name,
            description=self.description,
            cohort_id=self.cohort_id,
            replay_config_id=self.replay_config_id,
            created=self.created,
            updated=self.updated,
        )
