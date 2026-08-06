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
"""Agent version ORM table."""

import uuid

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
from kitaru.server.domain.agent_version import (
    AgentCapabilities,
    AgentVersion,
    RunSpec,
)

AGENT_VERSION_AGENT_ID_VERSION_UNIQUE_CONSTRAINT = unique_constraint_name(
    "agent_version", ["agent_id", "version"]
)
AGENT_VERSION_AGENT_ID_FOREIGN_KEY = foreign_key_name("agent_version", ["agent_id"])
AGENT_VERSION_OWNER_ID_FOREIGN_KEY = foreign_key_name("agent_version", ["owner_id"])
AGENT_VERSION_OWNER_ID_INDEX = index_name("agent_version", ["owner_id"])


class AgentVersionORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Agent version table."""

    __tablename__ = "agent_version"
    __table_args__ = (
        UniqueConstraint(
            "agent_id", "version", name=AGENT_VERSION_AGENT_ID_VERSION_UNIQUE_CONSTRAINT
        ),
        ForeignKeyConstraint(
            ["agent_id"], ["agent.id"], name=AGENT_VERSION_AGENT_ID_FOREIGN_KEY
        ),
        ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name=AGENT_VERSION_OWNER_ID_FOREIGN_KEY
        ),
        Index(AGENT_VERSION_OWNER_ID_INDEX, "owner_id"),
    )

    owner_id: Mapped[uuid.UUID]
    agent_id: Mapped[uuid.UUID]
    version: Mapped[int]
    display_version: Mapped[str | None] = mapped_column(String(255))
    description: Mapped[str | None] = mapped_column(Text)
    run_command: Mapped[str | None] = mapped_column(Text)
    run_working_dir: Mapped[str | None] = mapped_column(Text)
    run_env: Mapped[dict[str, str] | None] = mapped_column(JSONB(none_as_null=True))
    run_timeout_seconds: Mapped[int | None]
    capabilities: Mapped[dict[str, list[str]]] = mapped_column(JSONB)

    @classmethod
    def from_domain(cls, agent_version: AgentVersion) -> "AgentVersionORM":
        """Build a row from a domain agent version.

        The run spec is flattened into the run_* columns, all null when the
        agent version carries no run spec. Secret ids live in the link
        table, managed separately by the repository.

        Args:
            agent_version: Agent version to store.

        Returns:
            Row without timestamps set.
        """
        run_spec = agent_version.run_spec
        return cls(
            id=agent_version.id,
            owner_id=agent_version.owner_id,
            agent_id=agent_version.agent_id,
            version=agent_version.version,
            display_version=agent_version.display_version,
            description=agent_version.description,
            run_command=run_spec.command if run_spec is not None else None,
            run_working_dir=run_spec.working_dir if run_spec is not None else None,
            run_env=run_spec.env if run_spec is not None else None,
            run_timeout_seconds=(
                run_spec.timeout_seconds if run_spec is not None else None
            ),
            capabilities=agent_version.capabilities.model_dump(mode="json"),
        )

    def to_domain(self, secret_ids: list[uuid.UUID]) -> AgentVersion:
        """Build a domain agent version from this row.

        Args:
            secret_ids: Ordered secret ids loaded from the link table.

        Returns:
            Agent version with timestamps set.
        """
        run_spec = None
        if self.run_command is not None:
            assert self.run_timeout_seconds is not None
            run_spec = RunSpec(
                command=self.run_command,
                working_dir=self.run_working_dir,
                env=self.run_env if self.run_env is not None else {},
                secret_ids=secret_ids,
                timeout_seconds=self.run_timeout_seconds,
            )
        return AgentVersion(
            id=self.id,
            owner_id=self.owner_id,
            agent_id=self.agent_id,
            version=self.version,
            display_version=self.display_version,
            description=self.description,
            run_spec=run_spec,
            capabilities=AgentCapabilities(**self.capabilities),
            created=self.created,
            updated=self.updated,
        )
