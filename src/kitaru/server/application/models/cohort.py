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
"""Cohort filter and command models."""

import uuid
from typing import ClassVar

from kitaru.base import FrozenModel
from kitaru.server.base import ListFilter


class CohortFilter(ListFilter):
    """Cohort list filter."""

    name: str | None = None
    tag: str | None = None


class CohortSessionsFilter(ListFilter):
    """Cohort sessions list filter.

    Ordered by index ascending rather than the created-descending default,
    since the wire identity of the listing is a cohort's fixed member order.
    """

    sortable_fields: ClassVar[frozenset[str]] = frozenset({"index"})

    cohort_id: uuid.UUID
    sort: str = "index:asc"


class CohortCreate(FrozenModel):
    """Cohort create command."""

    name: str
    description: str | None = None
    agent_id: uuid.UUID
    session_ids: list[uuid.UUID]


class CohortUpdate(FrozenModel):
    """Cohort update command."""

    name: str | None = None
    description: str | None = None
