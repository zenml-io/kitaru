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
"""Cohort and cohort version filter and command models."""

import uuid
from collections.abc import Mapping
from typing import Any, ClassVar

from pydantic import Field

from kitaru.api_models.v1.filter import FilterOp
from kitaru.base import FrozenModel
from kitaru.server.base import ListFilter
from kitaru.server.filtering import EQUALITY_OPS, STRING_OPS, FilterField


class CohortFilter(ListFilter):
    """Cohort list filter."""

    filterable_fields: ClassVar[Mapping[str, FilterField]] = {
        "agent_id": FilterField(value_type=uuid.UUID, ops=EQUALITY_OPS),
        "name": FilterField(value_type=str, ops=STRING_OPS),
        "tag": FilterField(value_type=str, ops=frozenset({FilterOp.EQ, FilterOp.IN})),
    }


class CohortVersionFilter(ListFilter):
    """Cohort version list filter."""

    cohort_id: uuid.UUID


class CohortCreate(FrozenModel):
    """Cohort create command."""

    name: str
    description: str | None = None
    agent_id: uuid.UUID
    metadata: dict[str, Any] = Field(default_factory=dict)


class CohortUpdate(FrozenModel):
    """Cohort update command."""

    name: str | None = None
    description: str | None = None
    metadata: dict[str, Any] | None = None


class CohortVersionCreate(FrozenModel):
    """Cohort version create command."""

    add_session_ids: list[uuid.UUID] = Field(default_factory=list)
    remove_session_ids: list[uuid.UUID] = Field(default_factory=list)
    display_version: str | None = None


class CohortVersionUpdate(FrozenModel):
    """Cohort version update command."""

    display_version: str | None = None
