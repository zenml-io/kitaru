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
"""Investigation and investigation session filter and command models."""

import uuid
from collections.abc import Mapping
from typing import ClassVar

from kitaru.api_models.v1.investigation import (
    InvestigationSessionVerdict,
    InvestigationSessionView,
    InvestigationStatus,
    QuestionItem,
)
from kitaru.base import FrozenModel
from kitaru.server.base import ListFilter
from kitaru.server.filtering import EQUALITY_OPS, NULLABLE_OPS, FilterField


class InvestigationFilter(ListFilter):
    """Investigation list filter."""

    filterable_fields: ClassVar[Mapping[str, FilterField]] = {
        "agent_id": FilterField(value_type=uuid.UUID, ops=EQUALITY_OPS),
        "status": FilterField(value_type=InvestigationStatus, ops=EQUALITY_OPS),
    }


class InvestigationSessionFilter(ListFilter):
    """Investigation session list filter.

    Ordered by position ascending rather than the created-descending
    default, since position is the session's presentation order.
    """

    sortable_fields: ClassVar[frozenset[str]] = frozenset({"position"})
    filterable_fields: ClassVar[Mapping[str, FilterField]] = {
        "verdict": FilterField(
            value_type=InvestigationSessionVerdict, ops=EQUALITY_OPS | NULLABLE_OPS
        ),
    }

    investigation_id: uuid.UUID
    sort: str = "position:asc"


class InvestigationSessionInput(FrozenModel):
    """Investigation session input."""

    session_id: uuid.UUID
    view: InvestigationSessionView | None = None


class InvestigationCreate(FrozenModel):
    """Investigation create command."""

    agent_id: uuid.UUID
    name: str
    description: str | None = None
    questions: list[QuestionItem]
    sessions: list[InvestigationSessionInput]


class InvestigationUpdate(FrozenModel):
    """Investigation update command."""

    name: str | None = None
    description: str | None = None
    status: InvestigationStatus | None = None
