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
"""Cohort version API models."""

import uuid

from pydantic import Field

from kitaru.api_models.v1.base import OwnedResponseModel, RequestModel
from kitaru.api_models.v1.filter import FilterableListParams


class CohortVersionCreateRequest(RequestModel):
    """Cohort version create request."""

    baseline_id: uuid.UUID | None = Field(
        default=None,
        description="Version the delta applies to, None uses the latest version.",
    )
    add_session_ids: list[uuid.UUID] = Field(
        default_factory=list, description="Sessions to add to the new version."
    )
    remove_session_ids: list[uuid.UUID] = Field(
        default_factory=list, description="Sessions to remove from the new version."
    )
    display_version: str | None = Field(
        default=None, description="Human-readable designator."
    )


class CohortVersionUpdateRequest(RequestModel):
    """Cohort version update request."""

    display_version: str | None = Field(
        default=None, description="New human-readable designator."
    )


class CohortVersionListParams(FilterableListParams):
    """Cohort version list params."""


class CohortVersionResponse(OwnedResponseModel):
    """Cohort version response."""

    id: uuid.UUID = Field(description="Cohort version id.")
    cohort_id: uuid.UUID = Field(description="Cohort this version belongs to.")
    version: int = Field(description="Server-assigned version number.")
    display_version: str | None = Field(description="Human-readable designator.")
    session_count: int = Field(description="Number of sessions in the version.")
