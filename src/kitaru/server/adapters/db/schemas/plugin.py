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
"""Plugin ORM tables."""

import uuid
from typing import Any

from sqlalchemy import Column, ForeignKeyConstraint, Index, UniqueConstraint
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
from kitaru.server.domain.names import MAX_NAME_LENGTH
from kitaru.server.domain.plugin import (
    MAX_PLUGIN_ENTRYPOINT_LENGTH,
    MAX_PLUGIN_PROVIDER_LENGTH,
    Plugin,
    PluginFormat,
    PluginKind,
    PluginVersion,
)

MAX_PLUGIN_KIND_LENGTH = 16
MAX_PLUGIN_FORMAT_LENGTH = 16

PLUGIN_NAME_UNIQUE_CONSTRAINT = unique_constraint_name("plugin", ["kind", "name"])
PLUGIN_OWNER_ID_FOREIGN_KEY = foreign_key_name("plugin", ["owner_id"])
PLUGIN_OWNER_ID_INDEX = index_name("plugin", ["owner_id"])
PLUGIN_KIND_PROVIDER_INDEX = index_name("plugin", ["kind", "provider"])

PLUGIN_VERSION_UNIQUE_CONSTRAINT = unique_constraint_name(
    "plugin_version", ["plugin_id", "version"]
)
PLUGIN_VERSION_PLUGIN_ID_FOREIGN_KEY = foreign_key_name("plugin_version", ["plugin_id"])
PLUGIN_VERSION_BLOB_ID_FOREIGN_KEY = foreign_key_name("plugin_version", ["blob_id"])


class PluginSchema(UUIDPrimaryKeyMixin, TimestampMixin, table=True):
    """Plugin table."""

    __tablename__ = "plugin"
    __table_args__ = (
        UniqueConstraint("kind", "name", name=PLUGIN_NAME_UNIQUE_CONSTRAINT),
        ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name=PLUGIN_OWNER_ID_FOREIGN_KEY
        ),
        Index(PLUGIN_OWNER_ID_INDEX, "owner_id"),
        Index(PLUGIN_KIND_PROVIDER_INDEX, "kind", "provider"),
    )

    owner_id: uuid.UUID = Field(nullable=False)
    kind: str = Field(max_length=MAX_PLUGIN_KIND_LENGTH, nullable=False)
    name: str = Field(max_length=MAX_NAME_LENGTH, nullable=False)
    provider: str | None = Field(default=None, max_length=MAX_PLUGIN_PROVIDER_LENGTH)
    metadata_: dict[str, Any] = Field(
        default_factory=dict,
        sa_column=Column("metadata", JSONB, nullable=False),
    )
    latest_version: int = Field(nullable=False)

    @classmethod
    def from_domain(cls, plugin: Plugin) -> "PluginSchema":
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
            provider=self.provider,
            metadata=self.metadata_,
            latest_version=self.latest_version,
            created=self.created,
            updated=self.updated,
        )


class PluginVersionSchema(UUIDPrimaryKeyMixin, TimestampMixin, table=True):
    """Plugin version table."""

    __tablename__ = "plugin_version"
    __table_args__ = (
        UniqueConstraint("plugin_id", "version", name=PLUGIN_VERSION_UNIQUE_CONSTRAINT),
        ForeignKeyConstraint(
            ["plugin_id"],
            ["plugin.id"],
            name=PLUGIN_VERSION_PLUGIN_ID_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["blob_id"], ["blob.id"], name=PLUGIN_VERSION_BLOB_ID_FOREIGN_KEY
        ),
    )

    plugin_id: uuid.UUID = Field(nullable=False)
    version: int = Field(nullable=False)
    format: str = Field(max_length=MAX_PLUGIN_FORMAT_LENGTH, nullable=False)
    blob_id: uuid.UUID = Field(nullable=False)
    entrypoint: str = Field(max_length=MAX_PLUGIN_ENTRYPOINT_LENGTH, nullable=False)

    @classmethod
    def from_domain(cls, version: PluginVersion) -> "PluginVersionSchema":
        """Build a row from a domain plugin version.

        Args:
            version: Plugin version to store.

        Returns:
            Row without timestamps set.
        """
        return cls(
            id=version.id,
            plugin_id=version.plugin_id,
            version=version.version,
            format=version.format.value,
            blob_id=version.blob_id,
            entrypoint=version.entrypoint,
        )

    def to_domain(self) -> PluginVersion:
        """Build a domain plugin version from this row.

        Returns:
            Plugin version with the creation timestamp set.
        """
        return PluginVersion(
            id=self.id,
            plugin_id=self.plugin_id,
            version=self.version,
            format=PluginFormat(self.format),
            blob_id=self.blob_id,
            entrypoint=self.entrypoint,
            created=self.created,
        )
