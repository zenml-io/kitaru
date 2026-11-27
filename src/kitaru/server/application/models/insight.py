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
"""Insight filter and command models."""

import uuid
from collections.abc import Mapping
from typing import ClassVar

from kitaru.api_models.v1.insight import InsightInput, InsightStatus
from kitaru.base import FrozenModel
from kitaru.server.base import ListFilter
from kitaru.server.filtering import (
    EQUALITY_OPS,
    NULLABLE_OPS,
    SCOPE_OPS,
    STRING_OPS,
    FilterField,
)


class InsightFilter(ListFilter):
    """Insight list filter."""

    filterable_fields: ClassVar[Mapping[str, FilterField]] = {
        "id": FilterField(value_type=uuid.UUID, ops=EQUALITY_OPS),
        "agent_id": FilterField(value_type=uuid.UUID, ops=EQUALITY_OPS),
        "import_id": FilterField(value_type=uuid.UUID, ops=SCOPE_OPS | NULLABLE_OPS),
        "task_id": FilterField(value_type=uuid.UUID, ops=EQUALITY_OPS | NULLABLE_OPS),
        "name": FilterField(value_type=str, ops=STRING_OPS),
        "title": FilterField(value_type=str, ops=STRING_OPS),
        "status": FilterField(value_type=str, ops=EQUALITY_OPS),
        "source_provider": FilterField(value_type=str, ops=EQUALITY_OPS | NULLABLE_OPS),
    }


class InsightCreate(FrozenModel):
    """Insight create command."""

    agent_id: uuid.UUID
    insights: list[InsightInput]


class InsightUpdate(FrozenModel):
    """Insight update command."""

    title: str | None = None
    summary: str | None = None
    status: InsightStatus | None = None
