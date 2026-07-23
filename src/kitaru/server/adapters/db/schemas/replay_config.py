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
"""Replay config ORM table."""

import uuid
from typing import Any

from sqlalchemy import Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field

from kitaru.server.adapters.db.schemas.base import (
    TimestampMixin,
    UUIDPrimaryKeyMixin,
)
from kitaru.server.adapters.db.schemas.schema_utils import index_name
from kitaru.server.domain.replay_config import (
    ReplayConfig,
    ReplayOverride,
    ScoringPolicy,
    ToolPolicyConfig,
)

REPLAY_CONFIG_OWNER_ID_INDEX = index_name("replay_config", ["owner_id"])


class ReplayConfigSchema(UUIDPrimaryKeyMixin, TimestampMixin, table=True):
    """Replay config table."""

    __tablename__ = "replay_config"
    __table_args__ = (Index(REPLAY_CONFIG_OWNER_ID_INDEX, "owner_id"),)

    owner_id: uuid.UUID = Field(foreign_key="account.id", nullable=False)
    override: dict[str, Any] | None = Field(default=None, sa_type=JSONB)
    tool_policy: dict[str, Any] = Field(sa_type=JSONB, nullable=False)
    scoring_policy: dict[str, Any] = Field(sa_type=JSONB, nullable=False)

    @classmethod
    def from_domain(cls, config: ReplayConfig) -> "ReplayConfigSchema":
        """Build a row from a domain replay config.

        Args:
            config: Replay config to store.

        Returns:
            Row without timestamps set.
        """
        return cls(
            id=config.id,
            owner_id=config.owner_id,
            override=config.override.model_dump(mode="json")
            if config.override
            else None,
            tool_policy=config.tool_policy.model_dump(mode="json"),
            scoring_policy=config.scoring_policy.model_dump(mode="json"),
        )

    def to_domain(self) -> ReplayConfig:
        """Build a domain replay config from this row.

        Returns:
            Replay config with timestamps set.
        """
        return ReplayConfig(
            id=self.id,
            owner_id=self.owner_id,
            override=ReplayOverride.model_validate(self.override)
            if self.override is not None
            else None,
            tool_policy=ToolPolicyConfig.model_validate(self.tool_policy),
            scoring_policy=ScoringPolicy.model_validate(self.scoring_policy),
            created=self.created,
            updated=self.updated,
        )
