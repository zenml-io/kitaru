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
"""Shared SQLModel ORM base types and mixins."""

import uuid
from datetime import UTC, datetime

from sqlalchemy import DateTime
from sqlmodel import Field, SQLModel

from kitaru.server.domain.ids import uuid7


class TimestampMixin(SQLModel):
    """Created and updated timestamps on persisted entities.

    Attributes:
        created: UTC time when the row was first stored.
        updated: UTC time of the last modification.
    """

    created: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        sa_type=DateTime(timezone=True),  # ty: ignore[invalid-argument-type]
    )
    updated: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        sa_type=DateTime(timezone=True),  # ty: ignore[invalid-argument-type]
        sa_column_kwargs={"onupdate": lambda: datetime.now(UTC)},
    )


class UUIDPrimaryKeyMixin(SQLModel):
    """UUID primary key for top-level persisted entities.

    Attributes:
        id: Stable identifier for the entity.
    """

    id: uuid.UUID = Field(
        default_factory=uuid7,
        primary_key=True,
        nullable=False,
    )
