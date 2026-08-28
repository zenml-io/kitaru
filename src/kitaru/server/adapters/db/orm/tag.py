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

from sqlalchemy import (
    CheckConstraint,
    ForeignKeyConstraint,
    Index,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import InstrumentedAttribute, Mapped, mapped_column

from kitaru.api_models.v1.tag import TagResourceType
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
from kitaru.server.domain.tag import Tag, TagLink

TAG_NAME_UNIQUE_CONSTRAINT = unique_constraint_name("tag", ["name"])
TAG_OWNER_ID_FOREIGN_KEY = foreign_key_name("tag", ["owner_id"])
TAG_OWNER_ID_INDEX = index_name("tag", ["owner_id"])


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


TAG_LINK_TAG_ID_FOREIGN_KEY = foreign_key_name("tag_link", ["tag_id"])
TAG_LINK_TAG_ID_INDEX = index_name("tag_link", ["tag_id"])
TAG_LINK_SESSION_ID_FOREIGN_KEY = foreign_key_name("tag_link", ["session_id"])
TAG_LINK_COHORT_ID_FOREIGN_KEY = foreign_key_name("tag_link", ["cohort_id"])
TAG_LINK_COHORT_VERSION_ID_FOREIGN_KEY = foreign_key_name(
    "tag_link", ["cohort_version_id"]
)
TAG_LINK_AGENT_VERSION_ID_FOREIGN_KEY = foreign_key_name(
    "tag_link", ["agent_version_id"]
)
TAG_LINK_EXPERIMENT_ID_FOREIGN_KEY = foreign_key_name("tag_link", ["experiment_id"])
TAG_LINK_EXPERIMENT_RUN_ID_FOREIGN_KEY = foreign_key_name(
    "tag_link", ["experiment_run_id"]
)
# Resource id leads tag id in each unique constraint so the FK cascade
# lookups and the filter EXISTS join, both keyed by resource id, can use it.
TAG_LINK_SESSION_ID_TAG_ID_UNIQUE_CONSTRAINT = unique_constraint_name(
    "tag_link", ["session_id", "tag_id"]
)
TAG_LINK_COHORT_ID_TAG_ID_UNIQUE_CONSTRAINT = unique_constraint_name(
    "tag_link", ["cohort_id", "tag_id"]
)
TAG_LINK_COHORT_VERSION_ID_TAG_ID_UNIQUE_CONSTRAINT = unique_constraint_name(
    "tag_link", ["cohort_version_id", "tag_id"]
)
TAG_LINK_AGENT_VERSION_ID_TAG_ID_UNIQUE_CONSTRAINT = unique_constraint_name(
    "tag_link", ["agent_version_id", "tag_id"]
)
TAG_LINK_EXPERIMENT_ID_TAG_ID_UNIQUE_CONSTRAINT = unique_constraint_name(
    "tag_link", ["experiment_id", "tag_id"]
)
TAG_LINK_EXPERIMENT_RUN_ID_TAG_ID_UNIQUE_CONSTRAINT = unique_constraint_name(
    "tag_link", ["experiment_run_id", "tag_id"]
)
TAG_LINK_RESOURCE_CHECK_CONSTRAINT = check_constraint_name(
    "tag_link",
    [
        "session_id",
        "cohort_id",
        "cohort_version_id",
        "agent_version_id",
        "experiment_id",
        "experiment_run_id",
    ],
)
_TAG_LINK_RESOURCE_CHECK_SQL = (
    "num_nonnulls(session_id, cohort_id, cohort_version_id, "
    "agent_version_id, experiment_id, experiment_run_id) = 1"
)


class TagLinkORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Tag link table."""

    __tablename__ = "tag_link"
    __table_args__ = (
        UniqueConstraint(
            "session_id", "tag_id", name=TAG_LINK_SESSION_ID_TAG_ID_UNIQUE_CONSTRAINT
        ),
        UniqueConstraint(
            "cohort_id", "tag_id", name=TAG_LINK_COHORT_ID_TAG_ID_UNIQUE_CONSTRAINT
        ),
        UniqueConstraint(
            "cohort_version_id",
            "tag_id",
            name=TAG_LINK_COHORT_VERSION_ID_TAG_ID_UNIQUE_CONSTRAINT,
        ),
        UniqueConstraint(
            "agent_version_id",
            "tag_id",
            name=TAG_LINK_AGENT_VERSION_ID_TAG_ID_UNIQUE_CONSTRAINT,
        ),
        UniqueConstraint(
            "experiment_id",
            "tag_id",
            name=TAG_LINK_EXPERIMENT_ID_TAG_ID_UNIQUE_CONSTRAINT,
        ),
        UniqueConstraint(
            "experiment_run_id",
            "tag_id",
            name=TAG_LINK_EXPERIMENT_RUN_ID_TAG_ID_UNIQUE_CONSTRAINT,
        ),
        Index(TAG_LINK_TAG_ID_INDEX, "tag_id"),
        ForeignKeyConstraint(
            ["tag_id"],
            ["tag.id"],
            name=TAG_LINK_TAG_ID_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["session_id"],
            ["session.id"],
            name=TAG_LINK_SESSION_ID_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["cohort_id"],
            ["cohort.id"],
            name=TAG_LINK_COHORT_ID_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["cohort_version_id"],
            ["cohort_version.id"],
            name=TAG_LINK_COHORT_VERSION_ID_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["agent_version_id"],
            ["agent_version.id"],
            name=TAG_LINK_AGENT_VERSION_ID_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["experiment_id"],
            ["experiment.id"],
            name=TAG_LINK_EXPERIMENT_ID_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["experiment_run_id"],
            ["experiment_run.id"],
            name=TAG_LINK_EXPERIMENT_RUN_ID_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        CheckConstraint(
            _TAG_LINK_RESOURCE_CHECK_SQL, name=TAG_LINK_RESOURCE_CHECK_CONSTRAINT
        ),
    )

    tag_id: Mapped[uuid.UUID]
    session_id: Mapped[uuid.UUID | None]
    cohort_id: Mapped[uuid.UUID | None]
    cohort_version_id: Mapped[uuid.UUID | None]
    agent_version_id: Mapped[uuid.UUID | None]
    experiment_id: Mapped[uuid.UUID | None]
    experiment_run_id: Mapped[uuid.UUID | None]

    @classmethod
    def get_resource_column(
        cls, resource_type: TagResourceType
    ) -> InstrumentedAttribute[uuid.UUID | None]:
        """Map a resource type to its typed foreign key column.

        Args:
            resource_type: Kind of resource a tag link points at.

        Returns:
            Column storing the linked resource id for that type.
        """
        return {
            TagResourceType.SESSION: cls.session_id,
            TagResourceType.COHORT: cls.cohort_id,
            TagResourceType.COHORT_VERSION: cls.cohort_version_id,
            TagResourceType.AGENT_VERSION: cls.agent_version_id,
            TagResourceType.EXPERIMENT: cls.experiment_id,
            TagResourceType.EXPERIMENT_RUN: cls.experiment_run_id,
        }[resource_type]

    @classmethod
    def from_domain(cls, link: TagLink) -> "TagLinkORM":
        """Build a row from a domain tag link.

        Args:
            link: Tag link to store.

        Returns:
            Row without timestamps set.
        """
        row = cls(id=link.id, tag_id=link.tag_id)
        setattr(row, cls.get_resource_column(link.resource_type).key, link.resource_id)
        return row

    def to_domain(self) -> TagLink:
        """Build a domain tag link from this row.

        Raises:
            ValueError: No typed resource column carries a value.

        Returns:
            Tag link with timestamps set.
        """
        for resource_type in TagResourceType:
            resource_id = getattr(self, self.get_resource_column(resource_type).key)
            if resource_id is not None:
                return TagLink(
                    id=self.id,
                    tag_id=self.tag_id,
                    resource_type=resource_type,
                    resource_id=resource_id,
                    created=self.created,
                    updated=self.updated,
                )
        raise ValueError(f"Tag link {self.id} has no resource id set")
