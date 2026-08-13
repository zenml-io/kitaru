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
"""Annotation ORM table."""

import uuid
from typing import Any

from sqlalchemy import ForeignKeyConstraint, Index, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from kitaru.api_models.v1.annotation import AnnotationSelector
from kitaru.server.adapters.db.orm.base import (
    Base,
    TimestampMixin,
    UUIDPrimaryKeyMixin,
)
from kitaru.server.adapters.db.orm.orm_utils import foreign_key_name, index_name
from kitaru.server.domain.annotation import Annotation

ANNOTATION_OWNER_ID_FOREIGN_KEY = foreign_key_name("annotation", ["owner_id"])
ANNOTATION_SESSION_ID_FOREIGN_KEY = foreign_key_name("annotation", ["session_id"])
ANNOTATION_INVESTIGATION_SESSION_ID_FOREIGN_KEY = foreign_key_name(
    "annotation", ["investigation_session_id"]
)
ANNOTATION_OWNER_ID_INDEX = index_name("annotation", ["owner_id"])
ANNOTATION_SESSION_ID_INDEX = index_name("annotation", ["session_id"])

QUESTION_KEY_LENGTH = 64


class AnnotationORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Annotation table."""

    __tablename__ = "annotation"
    __table_args__ = (
        ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name=ANNOTATION_OWNER_ID_FOREIGN_KEY
        ),
        ForeignKeyConstraint(
            ["session_id"],
            ["session.id"],
            name=ANNOTATION_SESSION_ID_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["investigation_session_id"],
            ["investigation_session.id"],
            name=ANNOTATION_INVESTIGATION_SESSION_ID_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        Index(ANNOTATION_OWNER_ID_INDEX, "owner_id"),
        Index(ANNOTATION_SESSION_ID_INDEX, "session_id"),
    )

    owner_id: Mapped[uuid.UUID]
    session_id: Mapped[uuid.UUID]
    investigation_session_id: Mapped[uuid.UUID | None]
    question_key: Mapped[str | None] = mapped_column(String(QUESTION_KEY_LENGTH))
    selector: Mapped[Any | None] = mapped_column(JSONB(none_as_null=True))
    value: Mapped[Any] = mapped_column(JSONB)

    @classmethod
    def column_values(cls, annotation: Annotation) -> dict[str, object]:
        """Map a domain annotation to its column values, id included.

        Args:
            annotation: Annotation to store.

        Returns:
            Column values keyed by column name.
        """
        return {
            "id": annotation.id,
            "owner_id": annotation.owner_id,
            "session_id": annotation.session_id,
            "investigation_session_id": annotation.investigation_session_id,
            "question_key": annotation.question_key,
            "selector": (
                annotation.selector.model_dump(mode="json")
                if annotation.selector is not None
                else None
            ),
            "value": annotation.value,
        }

    @classmethod
    def from_domain(cls, annotation: Annotation) -> "AnnotationORM":
        """Build a row from a domain annotation.

        Args:
            annotation: Annotation to store.

        Returns:
            Row without timestamps set.
        """
        return cls(**cls.column_values(annotation))

    def to_domain(self) -> Annotation:
        """Build a domain annotation from this row.

        Returns:
            Annotation with timestamps set.
        """
        return Annotation(
            id=self.id,
            owner_id=self.owner_id,
            session_id=self.session_id,
            investigation_session_id=self.investigation_session_id,
            question_key=self.question_key,
            selector=(
                AnnotationSelector.model_validate(self.selector)
                if self.selector is not None
                else None
            ),
            value=self.value,
            created=self.created,
            updated=self.updated,
        )
