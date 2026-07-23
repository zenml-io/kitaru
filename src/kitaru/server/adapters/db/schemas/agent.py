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
"""Agent ORM table."""

import uuid

from sqlalchemy import Index, Text, UniqueConstraint
from sqlmodel import Field

from kitaru.server.adapters.db.schemas.base import (
    TimestampMixin,
    UUIDPrimaryKeyMixin,
)
from kitaru.server.adapters.db.schemas.schema_utils import (
    index_name,
    unique_constraint_name,
)
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.names import MAX_NAME_LENGTH

AGENT_NAME_UNIQUE_CONSTRAINT = unique_constraint_name("agent", ["name"])
AGENT_OWNER_ID_INDEX = index_name("agent", ["owner_id"])


class AgentSchema(UUIDPrimaryKeyMixin, TimestampMixin, table=True):
    """Agent table."""

    __tablename__ = "agent"
    __table_args__ = (
        UniqueConstraint("name", name=AGENT_NAME_UNIQUE_CONSTRAINT),
        Index(AGENT_OWNER_ID_INDEX, "owner_id"),
    )

    owner_id: uuid.UUID = Field(foreign_key="account.id", nullable=False)
    name: str = Field(max_length=MAX_NAME_LENGTH, nullable=False)
    description: str | None = Field(default=None, sa_type=Text)

    @classmethod
    def from_domain(cls, agent: Agent) -> "AgentSchema":
        """Build a row from a domain agent.

        Args:
            agent: Agent to store.

        Returns:
            Row without timestamps set.
        """
        return cls(
            id=agent.id,
            owner_id=agent.owner_id,
            name=agent.name,
            description=agent.description,
        )

    def to_domain(self) -> Agent:
        """Build a domain agent from this row.

        Returns:
            Agent with timestamps set.
        """
        return Agent(
            id=self.id,
            owner_id=self.owner_id,
            name=self.name,
            description=self.description,
            created=self.created,
            updated=self.updated,
        )
