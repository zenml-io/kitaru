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
"""Session node pending link ORM table."""

import uuid

from sqlalchemy import (
    ForeignKeyConstraint,
    Index,
    String,
    Text,
    UniqueConstraint,
)
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
from kitaru.server.domain.session_node import PendingParentLink

SESSION_NODE_PENDING_LINK_UNIQUE_CONSTRAINT = unique_constraint_name(
    "session_node_pending_link", ["child_id", "parent_external_id", "kind"]
)
SESSION_NODE_PENDING_LINK_SESSION_ID_FOREIGN_KEY = foreign_key_name(
    "session_node_pending_link", ["session_id"]
)
SESSION_NODE_PENDING_LINK_CHILD_ID_FOREIGN_KEY = foreign_key_name(
    "session_node_pending_link", ["child_id"]
)
SESSION_NODE_PENDING_LINK_PARENT_EXTERNAL_ID_INDEX = index_name(
    "session_node_pending_link", ["session_id", "parent_external_id"]
)

PENDING_LINK_KIND_LENGTH = 16


class SessionNodePendingLinkORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Session node pending link table."""

    __tablename__ = "session_node_pending_link"
    __table_args__ = (
        UniqueConstraint(
            "child_id",
            "parent_external_id",
            "kind",
            name=SESSION_NODE_PENDING_LINK_UNIQUE_CONSTRAINT,
        ),
        ForeignKeyConstraint(
            ["session_id"],
            ["session.id"],
            name=SESSION_NODE_PENDING_LINK_SESSION_ID_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["child_id"],
            ["session_node.id"],
            name=SESSION_NODE_PENDING_LINK_CHILD_ID_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        Index(
            SESSION_NODE_PENDING_LINK_PARENT_EXTERNAL_ID_INDEX,
            "session_id",
            "parent_external_id",
        ),
    )

    session_id: Mapped[uuid.UUID]
    parent_external_id: Mapped[str] = mapped_column(Text)
    child_id: Mapped[uuid.UUID]
    kind: Mapped[str] = mapped_column(String(PENDING_LINK_KIND_LENGTH))

    @classmethod
    def from_domain(cls, link: PendingParentLink) -> "SessionNodePendingLinkORM":
        """Build a row from a domain pending parent link.

        Args:
            link: Pending parent link to store.

        Returns:
            Row without timestamps set.
        """
        return cls(
            session_id=link.session_id,
            parent_external_id=link.parent_external_id,
            child_id=link.child_id,
            kind=link.kind.value,
        )
