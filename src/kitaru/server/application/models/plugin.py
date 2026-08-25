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
"""Plugin filter and command models."""

import uuid
from collections.abc import Mapping
from typing import Any, ClassVar

from pydantic import Field

from kitaru.base import FrozenModel
from kitaru.server.base import ListFilter
from kitaru.server.domain.plugin import PluginKind
from kitaru.server.filtering import (
    EQUALITY_OPS,
    NULLABLE_OPS,
    STRING_OPS,
    FilterField,
)


class PluginFilter(ListFilter):
    """Plugin list filter."""

    kind: PluginKind


class EvaluatorFilter(PluginFilter):
    """Evaluator list filter."""

    filterable_fields: ClassVar[Mapping[str, FilterField]] = {
        "id": FilterField(value_type=uuid.UUID, ops=EQUALITY_OPS),
        "name": FilterField(value_type=str, ops=STRING_OPS),
        "agent_id": FilterField(value_type=uuid.UUID, ops=EQUALITY_OPS | NULLABLE_OPS),
    }


class ImporterFilter(PluginFilter):
    """Importer list filter."""

    filterable_fields: ClassVar[Mapping[str, FilterField]] = {
        "id": FilterField(value_type=uuid.UUID, ops=EQUALITY_OPS),
        "name": FilterField(value_type=str, ops=STRING_OPS),
        "provider": FilterField(value_type=str, ops=STRING_OPS | NULLABLE_OPS),
    }


class PluginVersionFilter(ListFilter):
    """Plugin version list filter."""

    plugin_id: uuid.UUID


class PluginCreate(FrozenModel):
    """Plugin create command."""

    name: str
    description: str | None = None
    provider: str | None = None
    logo_url: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    agent_id: uuid.UUID | None = None


class PluginUpdate(FrozenModel):
    """Plugin update command."""

    description: str | None = None
    logo_url: str | None = None
    metadata: dict[str, Any] | None = None
