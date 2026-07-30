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
"""Tag API models."""

import uuid
from enum import StrEnum

from pydantic import Field

from kitaru.api_models.v1.base import (
    ListParams,
    OwnedResponseModel,
    RequestModel,
    TimestampedResponseModel,
)


class TagResourceType(StrEnum):
    """Resource kind a tag link points at."""

    SESSION = "session"
    COHORT = "cohort"
    EXPERIMENT = "experiment"
    EXPERIMENT_RUN = "experiment_run"


class TagCreateRequest(RequestModel):
    """Tag create request."""

    name: str = Field(description="Tag name.")


class TagUpdateRequest(RequestModel):
    """Tag update request."""

    name: str = Field(description="New tag name.")


class TagListParams(ListParams):
    """Tag list params."""

    name: str | None = Field(default=None, description="Filter on tag name.")


class TagResponse(OwnedResponseModel):
    """Tag response."""

    id: uuid.UUID = Field(description="Tag id.")
    name: str = Field(description="Tag name.")


class TagLinkCreateRequest(RequestModel):
    """Tag link create request."""

    resource_type: TagResourceType = Field(description="Kind of resource being tagged.")
    resource_id: uuid.UUID = Field(description="Resource being tagged.")


class TagLinkResponse(TimestampedResponseModel):
    """Tag link response."""

    id: uuid.UUID = Field(description="Tag link id.")
    tag_id: uuid.UUID = Field(description="Tag applied.")
    resource_type: TagResourceType = Field(description="Kind of resource tagged.")
    resource_id: uuid.UUID = Field(description="Resource tagged.")
