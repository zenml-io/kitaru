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
"""Shared ORM base types and mixins."""

import uuid
from datetime import UTC, datetime

from sqlalchemy import DateTime
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from kitaru.server.domain.ids import uuid7


class Base(DeclarativeBase):
    """Declarative base for all ORM tables."""


class TimestampMixin:
    """Created and updated timestamps on persisted entities.

    Attributes:
        created: UTC time when the row was first stored.
        updated: UTC time of the last modification.
    """

    # Negative sort orders keep the mixin columns ahead of subclass columns,
    # matching the column order in the migrations.
    created: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        sort_order=-3,
    )
    updated: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
        sort_order=-2,
    )


class UUIDPrimaryKeyMixin:
    """UUID primary key for top-level persisted entities.

    Attributes:
        id: Stable identifier for the entity.
    """

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        default=uuid7,
        sort_order=-1,
    )
