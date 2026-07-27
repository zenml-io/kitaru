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
"""Scorer API models."""

import uuid
from datetime import datetime

from pydantic import Field

from kitaru.api_models.v1.base import RequestModel, ResponseModel
from kitaru.api_models.v1.plugins import PluginFormat


class ScorerCreateRequest(RequestModel):
    """Scorer create request."""

    name: str = Field(description="Scorer name.")


class ScorerResponse(ResponseModel):
    """Scorer response."""

    id: uuid.UUID = Field(description="Scorer id.")
    owner_id: uuid.UUID = Field(description="Id of the owning account.")
    name: str = Field(description="Scorer name.")
    latest_version: int = Field(description="Highest registered version number.")
    created: datetime = Field(description="Creation time.")
    updated: datetime = Field(description="Last modification time.")


class ScorerVersionCreateRequest(RequestModel):
    """Scorer version create request."""

    format: PluginFormat = Field(
        default=PluginFormat.INLINE, description="Code format."
    )
    blob_id: uuid.UUID = Field(description="Id of the code blob.")
    entrypoint: str = Field(description="Attribute implementing the scorer.")


class ScorerVersionResponse(ResponseModel):
    """Scorer version response."""

    id: uuid.UUID = Field(description="Scorer version id.")
    scorer_id: uuid.UUID = Field(description="Id of the scorer.")
    version: int = Field(description="Version number.")
    format: PluginFormat = Field(description="Code format.")
    blob_id: uuid.UUID = Field(description="Id of the code blob.")
    entrypoint: str = Field(description="Attribute implementing the scorer.")
    created: datetime = Field(description="Creation time.")
