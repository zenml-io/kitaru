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
"""Experiment and replay config ORM tables."""

import uuid
from typing import Any

from sqlalchemy import ForeignKeyConstraint, Index, String, Text, UniqueConstraint
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
from kitaru.server.domain.experiment import Experiment
from kitaru.server.domain.names import MAX_NAME_LENGTH
from kitaru.server.domain.replay_config import (
    EvaluatorConfig,
    ReplayConfig,
    ReplayOverride,
    ToolPolicy,
)

REPLAY_CONFIG_OWNER_ID_FOREIGN_KEY = foreign_key_name("replay_config", ["owner_id"])


class ReplayConfigORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Replay config table."""

    __tablename__ = "replay_config"
    __table_args__ = (
        ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name=REPLAY_CONFIG_OWNER_ID_FOREIGN_KEY
        ),
    )

    owner_id: Mapped[uuid.UUID]
    override: Mapped[dict[str, Any] | None] = mapped_column(JSONB(none_as_null=True))
    tool_policy: Mapped[dict[str, Any]] = mapped_column(JSONB)
    evaluators: Mapped[list[Any]] = mapped_column(JSONB)

    @classmethod
    def from_domain(cls, config: ReplayConfig) -> "ReplayConfigORM":
        """Build a row from a domain replay config.

        Args:
            config: Replay config to store.

        Returns:
            Row without timestamps set.
        """
        return cls(
            id=config.id,
            owner_id=config.owner_id,
            override=(
                config.override.model_dump(mode="json")
                if config.override is not None
                else None
            ),
            tool_policy=config.tool_policy.model_dump(mode="json"),
            evaluators=[
                evaluator.model_dump(mode="json") for evaluator in config.evaluators
            ],
        )

    def to_domain(self) -> ReplayConfig:
        """Build a domain replay config from this row.

        Returns:
            Replay config with timestamps set.
        """
        return ReplayConfig(
            id=self.id,
            owner_id=self.owner_id,
            override=(
                ReplayOverride.model_validate(self.override)
                if self.override is not None
                else None
            ),
            tool_policy=ToolPolicy.model_validate(self.tool_policy),
            evaluators=[
                EvaluatorConfig.model_validate(evaluator)
                for evaluator in self.evaluators
            ],
            created=self.created,
            updated=self.updated,
        )


EXPERIMENT_NAME_UNIQUE_CONSTRAINT = unique_constraint_name("experiment", ["name"])
EXPERIMENT_OWNER_ID_FOREIGN_KEY = foreign_key_name("experiment", ["owner_id"])
EXPERIMENT_AGENT_ID_FOREIGN_KEY = foreign_key_name("experiment", ["agent_id"])
EXPERIMENT_AGENT_ID_INDEX = index_name("experiment", ["agent_id"])
EXPERIMENT_REPLAY_CONFIG_ID_FOREIGN_KEY = foreign_key_name(
    "experiment", ["replay_config_id"]
)


class ExperimentORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Experiment table."""

    __tablename__ = "experiment"
    __table_args__ = (
        UniqueConstraint("name", name=EXPERIMENT_NAME_UNIQUE_CONSTRAINT),
        ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name=EXPERIMENT_OWNER_ID_FOREIGN_KEY
        ),
        ForeignKeyConstraint(
            ["agent_id"], ["agent.id"], name=EXPERIMENT_AGENT_ID_FOREIGN_KEY
        ),
        ForeignKeyConstraint(
            ["replay_config_id"],
            ["replay_config.id"],
            name=EXPERIMENT_REPLAY_CONFIG_ID_FOREIGN_KEY,
        ),
        Index(EXPERIMENT_AGENT_ID_INDEX, "agent_id"),
    )

    owner_id: Mapped[uuid.UUID]
    name: Mapped[str] = mapped_column(String(MAX_NAME_LENGTH))
    description: Mapped[str | None] = mapped_column(Text)
    agent_id: Mapped[uuid.UUID]
    replay_config_id: Mapped[uuid.UUID]

    @classmethod
    def from_domain(cls, experiment: Experiment) -> "ExperimentORM":
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
            agent_id=experiment.agent_id,
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
            agent_id=self.agent_id,
            replay_config_id=self.replay_config_id,
            created=self.created,
            updated=self.updated,
        )
