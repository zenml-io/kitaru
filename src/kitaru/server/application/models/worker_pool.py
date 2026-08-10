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
"""Worker pool filter and command models."""

from collections.abc import Mapping
from typing import ClassVar

from kitaru.api_models.v1.worker import WorkerScope
from kitaru.base import FrozenModel
from kitaru.server.base import ListFilter
from kitaru.server.filtering import STRING_OPS, FilterField


class WorkerPoolFilter(ListFilter):
    """Worker pool list filter."""

    filterable_fields: ClassVar[Mapping[str, FilterField]] = {
        "name": FilterField(value_type=str, ops=STRING_OPS),
    }


class WorkerPoolUpdate(FrozenModel):
    """Worker pool update command."""

    name: str | None = None
    scope: WorkerScope | None = None
