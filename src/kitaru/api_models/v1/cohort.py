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

from pydantic import Field

from kitaru.api_models.v1.base import ListParams, OwnedResponseModel, RequestModel


class CohortCreateRequest(RequestModel):
    """Cohort create request."""

    name: str = Field(description="Cohort name.")
    description: str | None = Field(default=None, description="Cohort description.")
    agent_id: uuid.UUID = Field(description="Agent the cohort's sessions belong to.")
    session_ids: list[uuid.UUID] = Field(
        min_length=1, description="Ordered sessions in the cohort."
    )


class CohortUpdateRequest(RequestModel):
    """Cohort update request."""

    name: str | None = Field(default=None, description="New cohort name.")
    description: str | None = Field(default=None, description="New cohort description.")


class CohortListParams(ListParams):
    """Cohort list params."""

    name: str | None = Field(default=None, description="Filter on cohort name.")
    tag: str | None = Field(default=None, description="Filter on tag name.")


class CohortSessionsListParams(RequestModel):
    """Cohort sessions list params."""

    cursor: str | None = Field(
        default=None, description="Cursor from the previous page."
    )
    size: int = Field(default=20, ge=1, le=1000, description="Items per page.")


class CohortResponse(OwnedResponseModel):
    """Cohort response."""

    id: uuid.UUID = Field(description="Cohort id.")
    name: str = Field(description="Cohort name.")
    description: str | None = Field(description="Cohort description.")
    agent_id: uuid.UUID = Field(description="Agent the cohort's sessions belong to.")
    session_count: int = Field(description="Number of sessions in the cohort.")
