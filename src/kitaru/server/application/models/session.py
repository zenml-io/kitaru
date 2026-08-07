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
"""Session filter and command models."""

import uuid
from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal
from typing import Any, ClassVar

from pydantic import AwareDatetime, Field

from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.session import SessionOrigin, SessionStatus
from kitaru.base import FrozenModel
from kitaru.server.base import ListFilter
from kitaru.server.filtering import (
    BOOLEAN_OPS,
    EQUALITY_OPS,
    NULLABLE_OPS,
    ORDERED_OPS,
    STRING_OPS,
    FilterField,
)


class SessionFilter(ListFilter):
    """Session list filter."""

    filterable_fields: ClassVar[Mapping[str, FilterField]] = {
        "agent_id": FilterField(value_type=uuid.UUID, ops=EQUALITY_OPS),
        "agent_version_id": FilterField(
            value_type=uuid.UUID, ops=EQUALITY_OPS | NULLABLE_OPS
        ),
        "task_id": FilterField(value_type=uuid.UUID, ops=EQUALITY_OPS | NULLABLE_OPS),
        "origin": FilterField(value_type=SessionOrigin, ops=EQUALITY_OPS),
        "status": FilterField(value_type=SessionStatus, ops=EQUALITY_OPS),
        "imported_from": FilterField(value_type=str, ops=STRING_OPS | NULLABLE_OPS),
        "framework": FilterField(value_type=str, ops=STRING_OPS | NULLABLE_OPS),
        "external_id": FilterField(value_type=str, ops=STRING_OPS | NULLABLE_OPS),
        "name": FilterField(value_type=str, ops=STRING_OPS | NULLABLE_OPS),
        "tag": FilterField(value_type=str, ops=frozenset({FilterOp.EQ, FilterOp.IN})),
        "cohort_version_id": FilterField(
            value_type=uuid.UUID, ops=frozenset({FilterOp.EQ, FilterOp.IN})
        ),
        "has_evaluation": FilterField(value_type=bool, ops=BOOLEAN_OPS),
        "started_at": FilterField(
            value_type=AwareDatetime, ops=ORDERED_OPS | NULLABLE_OPS
        ),
        "ended_at": FilterField(
            value_type=AwareDatetime, ops=ORDERED_OPS | NULLABLE_OPS
        ),
        "cost": FilterField(value_type=Decimal, ops=ORDERED_OPS | NULLABLE_OPS),
        "llm_call_count": FilterField(value_type=int, ops=ORDERED_OPS),
        "tool_call_count": FilterField(value_type=int, ops=ORDERED_OPS),
        "created": FilterField(value_type=AwareDatetime, ops=ORDERED_OPS),
    }


class SessionCreate(FrozenModel):
    """Session create command."""

    agent_id: uuid.UUID | None = None
    agent_version_id: uuid.UUID | None = None
    origin: SessionOrigin
    status: SessionStatus | None = None
    name: str | None = None
    inputs: Any = None
    outputs: Any = None
    error: str | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    external_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    imported_from: str | None = None
    framework: str | None = None
    adapter_version: str | None = None


class SessionUpdate(FrozenModel):
    """Session update command."""

    status: SessionStatus | None = None
    outputs: Any = None
    error: str | None = None
    ended_at: datetime | None = None
    name: str | None = None
    metadata: dict[str, Any] | None = None
