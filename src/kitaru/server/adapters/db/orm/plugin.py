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
"""Plugin and plugin version ORM tables."""

import uuid
from typing import Any

from sqlalchemy import (
    CheckConstraint,
    ForeignKeyConstraint,
    Index,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from kitaru.server.adapters.db.orm.base import (
    Base,
    TimestampMixin,
    UUIDPrimaryKeyMixin,
)
from kitaru.server.adapters.db.orm.orm_utils import (
    check_constraint_name,
    foreign_key_name,
    index_name,
    unique_constraint_name,
)
from kitaru.server.domain.names import MAX_NAME_LENGTH
from kitaru.server.domain.plugin import (
    MAX_REQUIREMENT_LENGTH,
    PackagePluginSource,
    Plugin,
    PluginKind,
    PluginSource,
    PluginVersion,
    ScriptPluginSource,
)

PLUGIN_KIND_NAME_UNIQUE_CONSTRAINT = unique_constraint_name("plugin", ["kind", "name"])
PLUGIN_OWNER_ID_FOREIGN_KEY = foreign_key_name("plugin", ["owner_id"])
PLUGIN_OWNER_ID_INDEX = index_name("plugin", ["owner_id"])
PLUGIN_KIND_PROVIDER_INDEX = index_name("plugin", ["kind", "provider"])


class PluginORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Plugin table."""

    __tablename__ = "plugin"
    __table_args__ = (
        UniqueConstraint("kind", "name", name=PLUGIN_KIND_NAME_UNIQUE_CONSTRAINT),
        ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name=PLUGIN_OWNER_ID_FOREIGN_KEY
        ),
        Index(PLUGIN_OWNER_ID_INDEX, "owner_id"),
        Index(PLUGIN_KIND_PROVIDER_INDEX, "kind", "provider"),
    )

    owner_id: Mapped[uuid.UUID | None]
    kind: Mapped[str] = mapped_column(String(32))
    name: Mapped[str] = mapped_column(String(MAX_NAME_LENGTH))
    description: Mapped[str | None] = mapped_column(Text)
    provider: Mapped[str | None] = mapped_column(String(MAX_NAME_LENGTH))
    metadata_: Mapped[dict[str, Any]] = mapped_column("metadata", JSONB)
    latest_version: Mapped[int]

    @classmethod
    def from_domain(cls, plugin: Plugin) -> "PluginORM":
        """Build a row from a domain plugin.

        Args:
            plugin: Plugin to store.

        Returns:
            Row without timestamps set.
        """
        return cls(
            id=plugin.id,
            owner_id=plugin.owner_id,
            kind=plugin.kind.value,
            name=plugin.name,
            description=plugin.description,
            provider=plugin.provider,
            metadata_=plugin.metadata,
            latest_version=plugin.latest_version,
        )

    def to_domain(self) -> Plugin:
        """Build a domain plugin from this row.

        Returns:
            Plugin with timestamps set.
        """
        return Plugin(
            id=self.id,
            owner_id=self.owner_id,
            kind=PluginKind(self.kind),
            name=self.name,
            description=self.description,
            provider=self.provider,
            metadata=self.metadata_,
            latest_version=self.latest_version,
            created=self.created,
            updated=self.updated,
        )


PLUGIN_VERSION_PLUGIN_ID_VERSION_UNIQUE_CONSTRAINT = unique_constraint_name(
    "plugin_version", ["plugin_id", "version"]
)
PLUGIN_VERSION_PLUGIN_ID_FOREIGN_KEY = foreign_key_name("plugin_version", ["plugin_id"])
PLUGIN_VERSION_BLOB_ID_FOREIGN_KEY = foreign_key_name("plugin_version", ["blob_id"])
PLUGIN_VERSION_SOURCE_CHECK_CONSTRAINT = check_constraint_name(
    "plugin_version", ["type", "blob_id", "requirement"]
)
_PLUGIN_VERSION_SOURCE_CHECK_SQL = (
    "(type = 'script' AND blob_id IS NOT NULL AND requirement IS NULL) "
    "OR (type = 'package' AND blob_id IS NULL AND requirement IS NOT NULL)"
)


class PluginVersionORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Plugin version table."""

    __tablename__ = "plugin_version"
    __table_args__ = (
        UniqueConstraint(
            "plugin_id",
            "version",
            name=PLUGIN_VERSION_PLUGIN_ID_VERSION_UNIQUE_CONSTRAINT,
        ),
        ForeignKeyConstraint(
            ["plugin_id"],
            ["plugin.id"],
            name=PLUGIN_VERSION_PLUGIN_ID_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["blob_id"], ["blob.id"], name=PLUGIN_VERSION_BLOB_ID_FOREIGN_KEY
        ),
        CheckConstraint(
            _PLUGIN_VERSION_SOURCE_CHECK_SQL,
            name=PLUGIN_VERSION_SOURCE_CHECK_CONSTRAINT,
        ),
    )

    plugin_id: Mapped[uuid.UUID]
    version: Mapped[int]
    display_version: Mapped[str | None] = mapped_column(String(255))
    # Discriminates the flattened source union: "script" populates blob_id,
    # "package" populates requirement, exactly one of the two is set.
    type: Mapped[str] = mapped_column(String(16))
    blob_id: Mapped[uuid.UUID | None]
    requirement: Mapped[str | None] = mapped_column(String(MAX_REQUIREMENT_LENGTH))
    entrypoint: Mapped[str] = mapped_column(Text)

    @classmethod
    def from_domain(
        cls,
        plugin_id: uuid.UUID,
        version: int,
        display_version: str | None,
        source: PluginSource,
    ) -> "PluginVersionORM":
        """Build a row from a plugin id, version number, and code source.

        Args:
            plugin_id: Id of the owning plugin.
            version: Server-assigned version number.
            display_version: Human-readable designator.
            source: Plugin code source.

        Returns:
            Row without timestamps set.
        """
        if isinstance(source, ScriptPluginSource):
            return cls(
                plugin_id=plugin_id,
                version=version,
                display_version=display_version,
                type="script",
                blob_id=source.blob_id,
                requirement=None,
                entrypoint=source.entrypoint,
            )
        return cls(
            plugin_id=plugin_id,
            version=version,
            display_version=display_version,
            type="package",
            blob_id=None,
            requirement=source.requirement,
            entrypoint=source.entrypoint,
        )

    def to_domain(self) -> PluginVersion:
        """Build a domain plugin version from this row.

        Returns:
            Plugin version with timestamps set.
        """
        source: PluginSource
        if self.type == "script":
            assert self.blob_id is not None
            source = ScriptPluginSource(
                blob_id=self.blob_id, entrypoint=self.entrypoint
            )
        else:
            assert self.requirement is not None
            source = PackagePluginSource(
                requirement=self.requirement, entrypoint=self.entrypoint
            )
        return PluginVersion(
            id=self.id,
            plugin_id=self.plugin_id,
            version=self.version,
            display_version=self.display_version,
            source=source,
            created=self.created,
            updated=self.updated,
        )
