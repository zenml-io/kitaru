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
from datetime import datetime
from enum import StrEnum

from pydantic import Field

from kitaru.api_models.v1.base import RequestModel, ResponseModel


class TagResourceType(StrEnum):
    """Tag link resource type."""

    SESSION = "session"
    COHORT = "cohort"
    EXPERIMENT = "experiment"
    EXPERIMENT_RUN = "experiment_run"


class TagCreateRequest(RequestModel):
    """Tag create request."""

    name: str = Field(description="Tag name.")


class TagResponse(ResponseModel):
    """Tag response."""

    id: uuid.UUID = Field(description="Tag id.")
    owner_id: uuid.UUID = Field(description="Id of the owning account.")
    name: str = Field(description="Tag name.")
    created: datetime = Field(description="Creation time.")
    updated: datetime = Field(description="Last modification time.")


class TagLinkCreateRequest(RequestModel):
    """Tag link create request."""

    resource_type: TagResourceType = Field(description="Type of the linked resource.")
    resource_id: uuid.UUID = Field(description="Id of the linked resource.")


class TagLinkResponse(ResponseModel):
    """Tag link response."""

    id: uuid.UUID = Field(description="Tag link id.")
    tag_id: uuid.UUID = Field(description="Id of the tag.")
    resource_type: TagResourceType = Field(description="Type of the linked resource.")
    resource_id: uuid.UUID = Field(description="Id of the linked resource.")
    created: datetime = Field(description="Creation time.")
    updated: datetime = Field(description="Last modification time.")
