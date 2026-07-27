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
from datetime import datetime

from pydantic import Field

from kitaru.api_models.v1.base import JsonValue, RequestModel, ResponseModel
from kitaru.api_models.v1.plugins import PluginFormat


class ImporterCreateRequest(RequestModel):
    """Importer create request."""

    name: str = Field(description="Importer name.")
    provider: str | None = Field(
        default=None, description="Provider the importer reads from."
    )
    metadata: dict[str, JsonValue] = Field(
        default_factory=dict, description="Importer configuration."
    )


class ImporterResponse(ResponseModel):
    """Importer response."""

    id: uuid.UUID = Field(description="Importer id.")
    owner_id: uuid.UUID = Field(description="Id of the owning account.")
    name: str = Field(description="Importer name.")
    provider: str | None = Field(description="Provider the importer reads from.")
    metadata: dict[str, JsonValue] = Field(description="Importer configuration.")
    latest_version: int = Field(description="Highest registered version number.")
    created: datetime = Field(description="Creation time.")
    updated: datetime = Field(description="Last modification time.")


class ImporterVersionCreateRequest(RequestModel):
    """Importer version create request."""

    format: PluginFormat = Field(
        default=PluginFormat.INLINE, description="Code format."
    )
    blob_id: uuid.UUID = Field(description="Id of the code blob.")
    entrypoint: str = Field(description="Attribute implementing the importer.")


class ImporterVersionResponse(ResponseModel):
    """Importer version response."""

    id: uuid.UUID = Field(description="Importer version id.")
    importer_id: uuid.UUID = Field(description="Id of the importer.")
    version: int = Field(description="Version number.")
    format: PluginFormat = Field(description="Code format.")
    blob_id: uuid.UUID = Field(description="Id of the code blob.")
    entrypoint: str = Field(description="Attribute implementing the importer.")
    created: datetime = Field(description="Creation time.")
