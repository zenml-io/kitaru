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
"""Worker API models."""

import uuid
from datetime import datetime
from typing import Any

from pydantic import Field

from kitaru.api_models.v1.base import JsonValue, RequestModel, ResponseModel


class WorkerCreateRequest(RequestModel):
    """Worker create request."""

    name: str = Field(description="Worker name.")
    agent_ids: list[uuid.UUID] = Field(
        default_factory=list,
        description="Ids of the served agents, empty means all agents.",
    )
    metadata: dict[str, JsonValue] = Field(
        default_factory=dict, description="Worker metadata."
    )


class WorkerResponse(ResponseModel):
    """Worker response."""

    id: uuid.UUID = Field(description="Worker id.")
    owner_id: uuid.UUID = Field(description="Id of the owning account.")
    name: str = Field(description="Worker name.")
    agent_ids: list[uuid.UUID] = Field(
        description="Ids of the served agents, empty means all agents."
    )
    last_seen_at: datetime = Field(description="Time of the last registration.")
    live: bool = Field(
        description="Whether the worker was seen within the liveness timeout."
    )
    metadata: dict[str, Any] = Field(description="Worker metadata.")
    created: datetime = Field(description="Creation time.")
    updated: datetime = Field(description="Last modification time.")
