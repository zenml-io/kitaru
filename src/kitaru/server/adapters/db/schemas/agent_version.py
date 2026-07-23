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
"""Agent version ORM tables."""

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
from kitaru.server.domain.agent_version import (
    AgentCapabilities,
    AgentVersion,
    RunSpec,
)
from kitaru.server.domain.names import MAX_NAME_LENGTH

AGENT_VERSION_UNIQUE_CONSTRAINT = unique_constraint_name(
    "agent_version", ["agent_id", "version"]
)
AGENT_VERSION_AGENT_ID_FOREIGN_KEY = foreign_key_name("agent_version", ["agent_id"])
AGENT_VERSION_OWNER_ID_INDEX = index_name("agent_version", ["owner_id"])

AGENT_VERSION_SECRET_UNIQUE_CONSTRAINT = unique_constraint_name(
    "agent_version_secret", ["agent_version_id", "secret_id"]
)
AGENT_VERSION_SECRET_VERSION_ID_FOREIGN_KEY = foreign_key_name(
    "agent_version_secret", ["agent_version_id"]
)
AGENT_VERSION_SECRET_SECRET_ID_FOREIGN_KEY = foreign_key_name(
    "agent_version_secret", ["secret_id"]
)


class AgentVersionSchema(UUIDPrimaryKeyMixin, TimestampMixin, table=True):
    """Agent version table."""

    __tablename__ = "agent_version"
    __table_args__ = (
        UniqueConstraint("agent_id", "version", name=AGENT_VERSION_UNIQUE_CONSTRAINT),
        ForeignKeyConstraint(
            ["agent_id"], ["agent.id"], name=AGENT_VERSION_AGENT_ID_FOREIGN_KEY
        ),
        Index(AGENT_VERSION_OWNER_ID_INDEX, "owner_id"),
    )

    owner_id: uuid.UUID = Field(foreign_key="account.id", nullable=False)
    agent_id: uuid.UUID = Field(nullable=False)
    version: str = Field(max_length=MAX_NAME_LENGTH, nullable=False)
    description: str | None = Field(default=None, sa_type=Text)
    run_command: str | None = Field(default=None, sa_type=Text)
    run_working_dir: str | None = Field(default=None, sa_type=Text)
    run_env: dict[str, str] | None = Field(default=None, sa_type=JSONB)
    run_timeout_seconds: int | None = Field(default=None)
    capabilities: dict[str, Any] = Field(sa_type=JSONB, nullable=False)

    @classmethod
    def from_domain(cls, version: AgentVersion) -> "AgentVersionSchema":
        """Build a row from a domain agent version.

        Args:
            version: Agent version to store.

        Returns:
            Row without timestamps set.
        """
        run_spec = version.run_spec
        return cls(
            id=version.id,
            owner_id=version.owner_id,
            agent_id=version.agent_id,
            version=version.version,
            description=version.description,
            run_command=run_spec.command if run_spec else None,
            run_working_dir=run_spec.working_dir if run_spec else None,
            run_env=run_spec.env if run_spec else None,
            run_timeout_seconds=run_spec.timeout_seconds if run_spec else None,
            capabilities=version.capabilities.model_dump(),
        )

    def to_domain(self, secret_ids: list[uuid.UUID]) -> AgentVersion:
        """Build a domain agent version from this row.

        Args:
            secret_ids: Ids of the run spec secrets.

        Returns:
            Agent version with timestamps set.
        """
        run_spec = None
        if self.run_command is not None:
            assert self.run_timeout_seconds is not None
            run_spec = RunSpec(
                command=self.run_command,
                working_dir=self.run_working_dir,
                env=self.run_env or {},
                secret_ids=secret_ids,
                timeout_seconds=self.run_timeout_seconds,
            )
        return AgentVersion(
            id=self.id,
            owner_id=self.owner_id,
            agent_id=self.agent_id,
            version=self.version,
            description=self.description,
            run_spec=run_spec,
            capabilities=AgentCapabilities.model_validate(self.capabilities),
            created=self.created,
            updated=self.updated,
        )


class AgentVersionSecretSchema(UUIDPrimaryKeyMixin, TimestampMixin, table=True):
    """Agent version secret table."""

    __tablename__ = "agent_version_secret"
    __table_args__ = (
        UniqueConstraint(
            "agent_version_id",
            "secret_id",
            name=AGENT_VERSION_SECRET_UNIQUE_CONSTRAINT,
        ),
        ForeignKeyConstraint(
            ["agent_version_id"],
            ["agent_version.id"],
            name=AGENT_VERSION_SECRET_VERSION_ID_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["secret_id"],
            ["secret.id"],
            name=AGENT_VERSION_SECRET_SECRET_ID_FOREIGN_KEY,
        ),
    )

    agent_version_id: uuid.UUID = Field(nullable=False)
    secret_id: uuid.UUID = Field(nullable=False)
