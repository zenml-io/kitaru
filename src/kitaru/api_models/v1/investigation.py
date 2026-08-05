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
"""Investigation and investigation session API models."""

import uuid
from datetime import datetime
from enum import StrEnum

from pydantic import Field, field_validator

from kitaru.api_models.v1.annotation import AnnotationSelector
from kitaru.api_models.v1.base import (
    CursorParams,
    JsonValue,
    OwnedResponseModel,
    RequestModel,
    TimestampedResponseModel,
)
from kitaru.api_models.v1.filter import FilterableListParams


class InvestigationStatus(StrEnum):
    """Investigation status."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"


class InvestigationSessionStatus(StrEnum):
    """Investigation session status."""

    PENDING = "pending"
    COMPLETED = "completed"
    SKIPPED = "skipped"


class QuestionItem(RequestModel):
    """Question item."""

    key: str = Field(description="Key identifying the question within the array.")
    question: str = Field(description="Question display text.")


class InvestigationSessionViewItem(RequestModel):
    """Investigation session view item."""

    label: str = Field(description="Short title for the item.")
    description: str = Field(description="Prose explaining what the curator saw.")
    selectors: list[AnnotationSelector] = Field(
        default_factory=list, description="References the item covers."
    )


class InvestigationSessionView(RequestModel):
    """Investigation session view."""

    version: int = Field(default=1, description="Format version.")
    summary: str = Field(description="Summary shown above the trace.")
    items: list[InvestigationSessionViewItem] = Field(
        default_factory=list, description="Curated findings for the session."
    )


class InvestigationSessionInput(RequestModel):
    """Investigation session input."""

    session_id: uuid.UUID = Field(description="Session to link.")
    view: InvestigationSessionView | None = Field(
        default=None, description="Curated session view."
    )


class InvestigationCreateRequest(RequestModel):
    """Investigation create request."""

    agent_id: uuid.UUID = Field(
        description="Agent the investigation's sessions belong to."
    )
    name: str = Field(description="Investigation name.")
    description: str | None = Field(default=None, description="Curator rationale.")
    questions: list[QuestionItem] = Field(
        description="Questions asked about each session."
    )
    sessions: list[InvestigationSessionInput] = Field(
        description="Sessions to investigate, in presentation order."
    )

    @field_validator("questions")
    @classmethod
    def _check_unique_keys(cls, value: list[QuestionItem]) -> list[QuestionItem]:
        """Reject duplicate question keys.

        Args:
            value: Questions to check.

        Raises:
            ValueError: A key appears more than once.

        Returns:
            Validated questions.
        """
        keys = [item.key for item in value]
        if len(set(keys)) != len(keys):
            raise ValueError("questions must not contain duplicate keys")
        return value


class InvestigationUpdateRequest(RequestModel):
    """Investigation update request."""

    name: str | None = Field(default=None, description="New investigation name.")
    description: str | None = Field(default=None, description="New curator rationale.")


class InvestigationListParams(FilterableListParams):
    """Investigation list params."""


class InvestigationResponse(OwnedResponseModel):
    """Investigation response."""

    id: uuid.UUID = Field(description="Investigation id.")
    agent_id: uuid.UUID = Field(
        description="Agent the investigation's sessions belong to."
    )
    name: str = Field(description="Investigation name.")
    description: str | None = Field(description="Curator rationale.")
    status: InvestigationStatus = Field(description="Investigation status.")
    questions: list[QuestionItem] = Field(
        description="Questions asked about each session."
    )
    started_at: datetime | None = Field(
        default=None, description="Time the first answer was recorded."
    )
    ended_at: datetime | None = Field(
        default=None, description="Time the last session settled."
    )
    metadata: dict[str, JsonValue] = Field(description="Arbitrary metadata.")
    total_sessions: int = Field(description="Number of linked sessions.")
    completed_sessions: int = Field(
        description="Number of linked sessions marked completed or skipped."
    )


class InvestigationSessionsListParams(CursorParams):
    """Investigation sessions list params."""


class InvestigationSessionUpdateRequest(RequestModel):
    """Investigation session update request."""

    status: InvestigationSessionStatus = Field(
        description="New investigation session status."
    )


class InvestigationSessionResponse(TimestampedResponseModel):
    """Investigation session response."""

    id: uuid.UUID = Field(description="Investigation session id.")
    investigation_id: uuid.UUID = Field(
        description="Investigation this session belongs to."
    )
    session_id: uuid.UUID = Field(description="Session being investigated.")
    position: int = Field(description="Presentation order within the investigation.")
    status: InvestigationSessionStatus = Field(
        description="Investigation session status."
    )
    view: InvestigationSessionView | None = Field(description="Curated session view.")
