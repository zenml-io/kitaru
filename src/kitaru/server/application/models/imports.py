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

from pydantic import Field, model_validator

from kitaru.base import FrozenModel
from kitaru.server.application.models.replay_config import EvaluatorConfigInput
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
    payload_blob_id: uuid.UUID | None = None
    fetch_query: dict[str, Any] | None = None
    params: dict[str, Any] = Field(default_factory=dict)
    evaluators: list[EvaluatorConfigInput] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check_source(self) -> "ImportCreate":
        """Require exactly one of payload_blob_id and fetch_query.

        Raises:
            ValueError: Both or neither field is set.

        Returns:
            The validated command.
        """
        if (self.payload_blob_id is None) == (self.fetch_query is None):
            raise ValueError(
                "Exactly one of payload_blob_id or fetch_query is required"
            )
        return self
