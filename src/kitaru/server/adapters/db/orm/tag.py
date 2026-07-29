"""Tag ORM tables."""

import uuid

from sqlalchemy import ForeignKeyConstraint, Index, String, UniqueConstraint
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
from kitaru.server.domain.tag import Tag, TagLink, TagResourceType

TAG_NAME_UNIQUE_CONSTRAINT = unique_constraint_name("tag", ["name"])
TAG_OWNER_FOREIGN_KEY = foreign_key_name("tag", ["owner_id"])
TAG_LINK_UNIQUE_CONSTRAINT = unique_constraint_name(
    "tag_link", ["tag_id", "resource_type", "resource_id"]
)
TAG_LINK_RESOURCE_INDEX = index_name("tag_link", ["resource_type", "resource_id"])
TAG_LINK_TAG_FOREIGN_KEY = foreign_key_name("tag_link", ["tag_id"])


class TagORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Tag table."""

    __tablename__ = "tag"
    __table_args__ = (
        UniqueConstraint("name", name=TAG_NAME_UNIQUE_CONSTRAINT),
        ForeignKeyConstraint(["owner_id"], ["account.id"], name=TAG_OWNER_FOREIGN_KEY),
    )

    owner_id: Mapped[uuid.UUID]
    name: Mapped[str] = mapped_column(String(MAX_NAME_LENGTH))

    @classmethod
    def from_domain(cls, tag: Tag) -> "TagORM":
        """Build a row from a tag."""
        return cls(id=tag.id, owner_id=tag.owner_id, name=tag.name)

    def to_domain(self) -> Tag:
        """Build a tag from this row."""
        return Tag(
            id=self.id,
            owner_id=self.owner_id,
            name=self.name,
            created=self.created,
            updated=self.updated,
        )


class TagLinkORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Polymorphic tag-link table."""

    __tablename__ = "tag_link"
    __table_args__ = (
        UniqueConstraint(
            "tag_id",
            "resource_type",
            "resource_id",
            name=TAG_LINK_UNIQUE_CONSTRAINT,
        ),
        Index(TAG_LINK_RESOURCE_INDEX, "resource_type", "resource_id"),
        ForeignKeyConstraint(
            ["tag_id"],
            ["tag.id"],
            name=TAG_LINK_TAG_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
    )

    tag_id: Mapped[uuid.UUID]
    resource_type: Mapped[str] = mapped_column(String(32))
    resource_id: Mapped[uuid.UUID]

    @classmethod
    def from_domain(cls, link: TagLink) -> "TagLinkORM":
        """Build a row from a tag link."""
        return cls(
            id=link.id,
            tag_id=link.tag_id,
            resource_type=link.resource_type.value,
            resource_id=link.resource_id,
        )

    def to_domain(self) -> TagLink:
        """Build a tag link from this row."""
        return TagLink(
            id=self.id,
            tag_id=self.tag_id,
            resource_type=TagResourceType(self.resource_type),
            resource_id=self.resource_id,
            created=self.created,
            updated=self.updated,
        )
