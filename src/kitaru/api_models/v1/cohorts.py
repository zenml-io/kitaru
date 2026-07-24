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
"""Cohort API models."""

import uuid
from datetime import datetime

from pydantic import Field

from kitaru.api_models.v1.base import RequestModel, ResponseModel


class CohortCreateRequest(RequestModel):
    """Cohort create request."""

    name: str = Field(description="Cohort name.")
    description: str | None = Field(default=None, description="Cohort description.")
    agent_id: uuid.UUID = Field(description="Id of the agent.")
    session_ids: list[uuid.UUID] = Field(
        description="Ids of the member sessions in position order."
    )


class CohortUpdateRequest(RequestModel):
    """Cohort update request."""

    name: str | None = Field(default=None, description="New cohort name.")
    description: str | None = Field(default=None, description="New cohort description.")


class CohortResponse(ResponseModel):
    """Cohort response."""

    id: uuid.UUID = Field(description="Cohort id.")
    owner_id: uuid.UUID = Field(description="Id of the owning account.")
    name: str = Field(description="Cohort name.")
    description: str | None = Field(description="Cohort description.")
    agent_id: uuid.UUID = Field(description="Id of the agent.")
    session_count: int = Field(description="Number of member sessions.")
    created: datetime = Field(description="Creation time.")
    updated: datetime = Field(description="Last modification time.")
