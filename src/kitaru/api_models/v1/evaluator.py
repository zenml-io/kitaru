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
"""Evaluator API models."""

import uuid

from pydantic import Field

from kitaru.api_models.v1.base import (
    JsonValue,
    RequestModel,
    TimestampedResponseModel,
)
from kitaru.api_models.v1.filter import FilterableListParams
from kitaru.api_models.v1.plugin import PluginSource


class EvaluatorCreateRequest(RequestModel):
    """Evaluator create request."""

    name: str = Field(description="Evaluator name.")
    description: str | None = Field(default=None, description="Evaluator description.")
    metadata: dict[str, JsonValue] = Field(
        default_factory=dict, description="Arbitrary metadata."
    )


class EvaluatorUpdateRequest(RequestModel):
    """Evaluator update request."""

    description: str | None = Field(
        default=None, description="New evaluator description."
    )
    metadata: dict[str, JsonValue] | None = Field(
        default=None, description="New metadata."
    )


class EvaluatorListParams(FilterableListParams):
    """Evaluator list params."""


class EvaluatorResponse(TimestampedResponseModel):
    """Evaluator response."""

    owner_id: uuid.UUID | None = Field(
        description="Id of the owning account, null for a default plugin."
    )
    id: uuid.UUID = Field(description="Evaluator id.")
    name: str = Field(description="Evaluator name.")
    description: str | None = Field(description="Evaluator description.")
    metadata: dict[str, JsonValue] = Field(description="Arbitrary metadata.")
    latest_version: int = Field(
        description="Highest version number created for this evaluator."
    )


class EvaluatorVersionCreateRequest(RequestModel):
    """Evaluator version create request."""

    source: PluginSource = Field(description="Evaluator code to load.")
    display_version: str | None = Field(
        default=None, description="Human-readable designator."
    )


class EvaluatorVersionUpdateRequest(RequestModel):
    """Evaluator version update request."""

    display_version: str | None = Field(
        default=None, description="New human-readable designator."
    )


class EvaluatorVersionResponse(TimestampedResponseModel):
    """Evaluator version response."""

    id: uuid.UUID = Field(description="Evaluator version id.")
    evaluator_id: uuid.UUID = Field(description="Evaluator this version belongs to.")
    version: int = Field(description="Server-assigned version number.")
    display_version: str | None = Field(description="Human-readable designator.")
    source: PluginSource = Field(description="Evaluator code to load.")
