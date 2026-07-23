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
from decimal import Decimal
from typing import Any

from pydantic import AwareDatetime, Field

from kitaru.api_models.v1.base import RequestModel, ResponseModel
from kitaru.api_models.v1.sessions import (
    SessionOrigin,
    SessionProvider,
    SessionStatus,
)


class CohortSessionFilter(RequestModel):
    """Cohort session filter."""

    agent_id: uuid.UUID | None = Field(
        default=None, description="Filter on agent id, sets the cohort's agent."
    )
    agent_version_id: uuid.UUID | None = Field(
        default=None, description="Filter on agent version id."
    )
    origin: SessionOrigin | None = Field(
        default=None, description="Filter on session origin."
    )
    status: SessionStatus | None = Field(
        default=None, description="Filter on session status."
    )
    provider: SessionProvider | None = Field(
        default=None, description="Filter on session provider."
    )
    external_id: str | None = Field(default=None, description="Filter on external id.")
    name: str | None = Field(default=None, description="Filter on session name.")
    tag: str | None = Field(default=None, description="Filter on attached tag name.")
    started_after: AwareDatetime | None = Field(
        default=None, description="Earliest start time."
    )
    started_before: AwareDatetime | None = Field(
        default=None, description="Latest start time."
    )
    ended_after: AwareDatetime | None = Field(
        default=None, description="Earliest end time."
    )
    ended_before: AwareDatetime | None = Field(
        default=None, description="Latest end time."
    )
    has_score: bool | None = Field(
        default=None, description="Filter on the presence of scores."
    )
    min_cost: Decimal | None = Field(default=None, description="Lowest cost.")
    max_cost: Decimal | None = Field(default=None, description="Highest cost.")
    min_total_tokens: int | None = Field(
        default=None, description="Lowest total token count."
    )
    max_total_tokens: int | None = Field(
        default=None, description="Highest total token count."
    )


class CohortCreateRequest(RequestModel):
    """Cohort create request."""

    name: str = Field(description="Cohort name.")
    description: str | None = Field(default=None, description="Cohort description.")
    agent_id: uuid.UUID | None = Field(
        default=None, description="Id of the agent, required with session ids."
    )
    session_ids: list[uuid.UUID] | None = Field(
        default=None,
        description="Ids of the member sessions in position order, mutually "
        "exclusive with the filter.",
    )
    filter: CohortSessionFilter | None = Field(
        default=None,
        description="Session filter resolving the members, mutually exclusive "
        "with session ids.",
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
    filter_snapshot: dict[str, Any] | None = Field(
        description="Filter the members were resolved from."
    )
    created: datetime = Field(description="Creation time.")
    updated: datetime = Field(description="Last modification time.")
