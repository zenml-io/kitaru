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
"""Import filter and command models."""

import uuid
from collections.abc import Mapping
from typing import Any, ClassVar

from pydantic import Field

from kitaru.base import FrozenModel
from kitaru.server.application.models.replay_config import (
    AnalyzerConfigInput,
    EvaluatorConfigInput,
)
from kitaru.server.base import ListFilter
from kitaru.server.filtering import EQUALITY_OPS, FilterField


class ImportFilter(ListFilter):
    """Import list filter."""

    filterable_fields: ClassVar[Mapping[str, FilterField]] = {
        "id": FilterField(value_type=uuid.UUID, ops=EQUALITY_OPS),
        "agent_id": FilterField(value_type=uuid.UUID, ops=EQUALITY_OPS),
        "job_id": FilterField(value_type=uuid.UUID, ops=EQUALITY_OPS),
    }


class ImportCreate(FrozenModel):
    """Import create command."""

    importer: str
    agent_id: uuid.UUID
    agent_version_id: uuid.UUID | None = None
    version: int | None = None
    payload_blob_id: uuid.UUID
    params: dict[str, Any] = Field(default_factory=dict)
    evaluators: list[EvaluatorConfigInput] = Field(default_factory=list)
    analyzers: list[AnalyzerConfigInput] = Field(default_factory=list)
