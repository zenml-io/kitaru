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
"""Importer API models."""

import uuid

from pydantic import Field

from kitaru.api_models.v1.base import (
    JsonValue,
    OwnedResponseModel,
    RequestModel,
    TimestampedResponseModel,
)
from kitaru.api_models.v1.filter import FilterableListParams
from kitaru.api_models.v1.plugin import PluginSource


class ImporterCreateRequest(RequestModel):
    """Importer create request."""

    name: str = Field(description="Importer name.")
    description: str | None = Field(default=None, description="Importer description.")
    provider: str | None = Field(
        default=None, description="Source system this importer reads."
    )
    metadata: dict[str, JsonValue] = Field(
        default_factory=dict, description="Arbitrary metadata."
    )


class ImporterUpdateRequest(RequestModel):
    """Importer update request."""

    description: str | None = Field(
        default=None, description="New importer description."
    )
    metadata: dict[str, JsonValue] | None = Field(
        default=None, description="New metadata."
    )


class ImporterListParams(FilterableListParams):
    """Importer list params."""


class ImporterResponse(OwnedResponseModel):
    """Importer response."""

    id: uuid.UUID = Field(description="Importer id.")
    name: str = Field(description="Importer name.")
    description: str | None = Field(description="Importer description.")
    provider: str | None = Field(description="Source system this importer reads.")
    metadata: dict[str, JsonValue] = Field(description="Arbitrary metadata.")
    latest_version: int = Field(
        description="Highest version number created for this importer."
    )


class ImporterVersionCreateRequest(RequestModel):
    """Importer version create request."""

    source: PluginSource = Field(description="Importer code to load.")
    display_version: str | None = Field(
        default=None, description="Human-readable designator."
    )


class ImporterVersionUpdateRequest(RequestModel):
    """Importer version update request."""

    display_version: str | None = Field(
        default=None, description="New human-readable designator."
    )


class ImporterVersionResponse(TimestampedResponseModel):
    """Importer version response."""

    id: uuid.UUID = Field(description="Importer version id.")
    importer_id: uuid.UUID = Field(description="Importer this version belongs to.")
    version: int = Field(description="Server-assigned version number.")
    display_version: str | None = Field(description="Human-readable designator.")
    source: PluginSource = Field(description="Importer code to load.")
