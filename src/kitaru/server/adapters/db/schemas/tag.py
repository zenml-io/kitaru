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
"""Tag ORM tables."""

import uuid

from sqlalchemy import Index, UniqueConstraint
from sqlmodel import Field

from kitaru.server.adapters.db.schemas.base import (
    TimestampMixin,
    UUIDPrimaryKeyMixin,
)
from kitaru.server.adapters.db.schemas.schema_utils import (
    index_name,
    unique_constraint_name,
)
from kitaru.server.domain.names import MAX_NAME_LENGTH
from kitaru.server.domain.tag import Tag, TagLink, TagResourceType

TAG_NAME_UNIQUE_CONSTRAINT = unique_constraint_name("tag", ["name"])
TAG_LINK_UNIQUE_CONSTRAINT = unique_constraint_name(
    "tag_link", ["tag_id", "resource_type", "resource_id"]
)
TAG_LINK_RESOURCE_INDEX = index_name("tag_link", ["resource_type", "resource_id"])

MAX_RESOURCE_TYPE_LENGTH = 64


class TagSchema(UUIDPrimaryKeyMixin, TimestampMixin, table=True):
    """Tag table."""

    __tablename__ = "tag"
    __table_args__ = (UniqueConstraint("name", name=TAG_NAME_UNIQUE_CONSTRAINT),)

    owner_id: uuid.UUID = Field(foreign_key="account.id", nullable=False)
    name: str = Field(max_length=MAX_NAME_LENGTH, nullable=False)

    @classmethod
    def from_domain(cls, tag: Tag) -> "TagSchema":
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


class TagLinkSchema(UUIDPrimaryKeyMixin, TimestampMixin, table=True):
    """Tag link table."""

    __tablename__ = "tag_link"
    __table_args__ = (
        UniqueConstraint(
            "tag_id",
            "resource_type",
            "resource_id",
            name=TAG_LINK_UNIQUE_CONSTRAINT,
        ),
        Index(TAG_LINK_RESOURCE_INDEX, "resource_type", "resource_id"),
    )

    tag_id: uuid.UUID = Field(foreign_key="tag.id", ondelete="CASCADE", nullable=False)
    resource_type: str = Field(max_length=MAX_RESOURCE_TYPE_LENGTH, nullable=False)
    resource_id: uuid.UUID = Field(nullable=False)

    @classmethod
    def from_domain(cls, link: TagLink) -> "TagLinkSchema":
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
