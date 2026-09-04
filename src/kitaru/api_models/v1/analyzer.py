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
"""Analyzer API models."""

import uuid

from pydantic import Field

from kitaru.api_models.v1.base import (
    JsonValue,
    RequestModel,
    TimestampedResponseModel,
)
from kitaru.api_models.v1.filter import FilterableListParams
from kitaru.api_models.v1.plugin import PluginSource


class AnalyzerCreateRequest(RequestModel):
    """Analyzer create request."""

    name: str = Field(description="Analyzer name.")
    description: str | None = Field(default=None, description="Analyzer description.")
    logo_url: str | None = Field(default=None, description="Analyzer logo URL.")
    metadata: dict[str, JsonValue] = Field(
        default_factory=dict, description="Arbitrary metadata."
    )


class AnalyzerUpdateRequest(RequestModel):
    """Analyzer update request."""

    description: str | None = Field(
        default=None, description="New analyzer description."
    )
    logo_url: str | None = Field(default=None, description="New logo URL.")
    metadata: dict[str, JsonValue] | None = Field(
        default=None, description="New metadata."
    )


class AnalyzerListParams(FilterableListParams):
    """Analyzer list params."""


class AnalyzerResponse(TimestampedResponseModel):
    """Analyzer response."""

    owner_id: uuid.UUID | None = Field(
        description="Id of the owning account, null for a default plugin."
    )
    id: uuid.UUID = Field(description="Analyzer id.")
    name: str = Field(description="Analyzer name.")
    description: str | None = Field(description="Analyzer description.")
    logo_url: str | None = Field(description="Analyzer logo URL.")
    metadata: dict[str, JsonValue] = Field(description="Arbitrary metadata.")
    latest_version: int = Field(
        description="Highest version number created for this analyzer."
    )


class AnalyzerVersionCreateRequest(RequestModel):
    """Analyzer version create request."""

    source: PluginSource = Field(description="Analyzer code to load.")
    display_version: str | None = Field(
        default=None, description="Human-readable designator."
    )


class AnalyzerVersionUpdateRequest(RequestModel):
    """Analyzer version update request."""

    display_version: str | None = Field(
        default=None, description="New human-readable designator."
    )


class AnalyzerVersionResponse(TimestampedResponseModel):
    """Analyzer version response."""

    id: uuid.UUID = Field(description="Analyzer version id.")
    analyzer_id: uuid.UUID = Field(description="Analyzer this version belongs to.")
    version: int = Field(description="Server-assigned version number.")
    display_version: str | None = Field(description="Human-readable designator.")
    source: PluginSource = Field(description="Analyzer code to load.")
