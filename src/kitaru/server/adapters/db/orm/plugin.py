"""Plugin registry ORM tables."""

import uuid

from sqlalchemy import (
    CheckConstraint,
    ForeignKeyConstraint,
    Index,
    String,
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
    foreign_key_name,
    index_name,
    unique_constraint_name,
)
from kitaru.server.domain.names import MAX_NAME_LENGTH
from kitaru.server.domain.plugin import (
    PackagePluginSource,
    Plugin,
    PluginKind,
    PluginVersion,
    ScriptPluginSource,
)

PLUGIN_NAME_UNIQUE_CONSTRAINT = unique_constraint_name("plugin", ["kind", "name"])
PLUGIN_KIND_PROVIDER_INDEX = index_name("plugin", ["kind", "provider"])
PLUGIN_OWNER_FOREIGN_KEY = foreign_key_name("plugin", ["owner_id"])
PLUGIN_VERSION_UNIQUE_CONSTRAINT = unique_constraint_name(
    "plugin_version", ["plugin_id", "version"]
)
PLUGIN_VERSION_PARENT_FOREIGN_KEY = foreign_key_name("plugin_version", ["plugin_id"])
PLUGIN_VERSION_BLOB_FOREIGN_KEY = foreign_key_name("plugin_version", ["blob_id"])


class PluginORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Importer/evaluator registry table."""

    __tablename__ = "plugin"
    __table_args__ = (
        UniqueConstraint("kind", "name", name=PLUGIN_NAME_UNIQUE_CONSTRAINT),
        Index(PLUGIN_KIND_PROVIDER_INDEX, "kind", "provider"),
        ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name=PLUGIN_OWNER_FOREIGN_KEY
        ),
    )

    owner_id: Mapped[uuid.UUID]
    kind: Mapped[str] = mapped_column(String(32))
    name: Mapped[str] = mapped_column(String(MAX_NAME_LENGTH))
    description: Mapped[str | None]
    provider: Mapped[str | None] = mapped_column(String(255))
    metadata_: Mapped[dict] = mapped_column("metadata", JSONB)
    latest_version: Mapped[int]

    @classmethod
    def from_domain(cls, plugin: Plugin) -> "PluginORM":
        """Build a row from a plugin."""
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
        """Build a plugin from this row."""
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


class PluginVersionORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Plugin-version table."""

    __tablename__ = "plugin_version"
    __table_args__ = (
        UniqueConstraint("plugin_id", "version", name=PLUGIN_VERSION_UNIQUE_CONSTRAINT),
        ForeignKeyConstraint(
            ["plugin_id"],
            ["plugin.id"],
            name=PLUGIN_VERSION_PARENT_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["blob_id"], ["blob.id"], name=PLUGIN_VERSION_BLOB_FOREIGN_KEY
        ),
        CheckConstraint(
            "(type = 'script' AND blob_id IS NOT NULL AND requirement IS NULL) "
            "OR (type = 'package' AND blob_id IS NULL AND requirement IS NOT NULL)",
            name="ck_plugin_version_source",
        ),
    )

    plugin_id: Mapped[uuid.UUID]
    version: Mapped[int]
    display_version: Mapped[str | None] = mapped_column(String(255))
    type: Mapped[str] = mapped_column(String(32))
    blob_id: Mapped[uuid.UUID | None]
    requirement: Mapped[str | None] = mapped_column(String(255))
    entrypoint: Mapped[str] = mapped_column(String(255))

    @classmethod
    def from_domain(cls, version: PluginVersion) -> "PluginVersionORM":
        """Build a row from a plugin version."""
        source = version.source
        return cls(
            id=version.id,
            plugin_id=version.plugin_id,
            version=version.version,
            display_version=version.display_version,
            type=source.type,
            blob_id=source.blob_id if isinstance(source, ScriptPluginSource) else None,
            requirement=(
                source.requirement if isinstance(source, PackagePluginSource) else None
            ),
            entrypoint=source.entrypoint,
        )

    def to_domain(self) -> PluginVersion:
        """Build a plugin version from this row."""
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
