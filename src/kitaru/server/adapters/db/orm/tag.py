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
"""Tag and tag link ORM tables."""

import uuid

from sqlalchemy import ForeignKeyConstraint, Index, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from kitaru.api_models.v1.tag import TagResourceType
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
from kitaru.server.domain.tag import Tag, TagLink

TAG_NAME_UNIQUE_CONSTRAINT = unique_constraint_name("tag", ["name"])
TAG_OWNER_ID_FOREIGN_KEY = foreign_key_name("tag", ["owner_id"])
TAG_OWNER_ID_INDEX = index_name("tag", ["owner_id"])

MAX_RESOURCE_TYPE_LENGTH = 32


class TagORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Tag table."""

    __tablename__ = "tag"
    __table_args__ = (
        UniqueConstraint("name", name=TAG_NAME_UNIQUE_CONSTRAINT),
        ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name=TAG_OWNER_ID_FOREIGN_KEY
        ),
        Index(TAG_OWNER_ID_INDEX, "owner_id"),
    )

    owner_id: Mapped[uuid.UUID]
    name: Mapped[str] = mapped_column(String(MAX_NAME_LENGTH))

    @classmethod
    def from_domain(cls, tag: Tag) -> "TagORM":
        """Build a row from a domain tag.

        Args:
            tag: Tag to store.

        Returns:
            Row without timestamps set.
        """
        return cls(id=tag.id, owner_id=tag.owner_id, name=tag.name)

    def to_domain(self) -> Tag:
        """Build a domain tag from this row.

        Returns:
            Tag with timestamps set.
        """
        return Tag(
            id=self.id,
            owner_id=self.owner_id,
            name=self.name,
            created=self.created,
            updated=self.updated,
        )


TAG_LINK_UNIQUE_CONSTRAINT = unique_constraint_name(
    "tag_link", ["tag_id", "resource_type", "resource_id"]
)
TAG_LINK_TAG_ID_FOREIGN_KEY = foreign_key_name("tag_link", ["tag_id"])
TAG_LINK_RESOURCE_INDEX = index_name("tag_link", ["resource_type", "resource_id"])


class TagLinkORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Tag link table."""

    __tablename__ = "tag_link"
    __table_args__ = (
        UniqueConstraint(
            "tag_id",
            "resource_type",
            "resource_id",
            name=TAG_LINK_UNIQUE_CONSTRAINT,
        ),
        ForeignKeyConstraint(
            ["tag_id"],
            ["tag.id"],
            name=TAG_LINK_TAG_ID_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        Index(TAG_LINK_RESOURCE_INDEX, "resource_type", "resource_id"),
    )

    tag_id: Mapped[uuid.UUID]
    resource_type: Mapped[str] = mapped_column(String(MAX_RESOURCE_TYPE_LENGTH))
    resource_id: Mapped[uuid.UUID]

    @classmethod
    def from_domain(cls, link: TagLink) -> "TagLinkORM":
        """Build a row from a domain tag link.

        Args:
            link: Tag link to store.

        Returns:
            Row without timestamps set.
        """
        return cls(
            id=link.id,
            tag_id=link.tag_id,
            resource_type=link.resource_type.value,
            resource_id=link.resource_id,
        )

    def to_domain(self) -> TagLink:
        """Build a domain tag link from this row.

        Returns:
            Tag link with timestamps set.
        """
        return TagLink(
            id=self.id,
            tag_id=self.tag_id,
            resource_type=TagResourceType(self.resource_type),
            resource_id=self.resource_id,
            created=self.created,
            updated=self.updated,
        )
