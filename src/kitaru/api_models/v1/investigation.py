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
from typing import Self

from pydantic import Field, model_validator

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


class InvestigationSessionVerdict(StrEnum):
    """Investigation session verdict."""

    ACCEPTABLE = "acceptable"
    PROBLEMATIC = "problematic"
    UNCERTAIN = "uncertain"


class InvestigationSessionHighlight(RequestModel):
    """Investigation session highlight."""

    selector: AnnotationSelector = Field(description="Part of the session highlighted.")
    description: str = Field(description="Prose explaining what the highlight shows.")


class InvestigationSessionQuestion(RequestModel):
    """Investigation session question."""

    key: str = Field(description="Question key, unique within the session.")
    question: str = Field(description="Question to answer about the session.")
    highlights: list[InvestigationSessionHighlight] = Field(
        default_factory=list, description="Curated highlights for the question."
    )


class InvestigationSessionInput(RequestModel):
    """Investigation session input."""

    session_id: uuid.UUID = Field(description="Session to link.")
    questions: list[InvestigationSessionQuestion] = Field(
        min_length=1, description="Questions to answer about the session."
    )

    @model_validator(mode="after")
    def _unique_question_keys(self) -> Self:
        """Reject duplicate question keys.

        Raises:
            ValueError: A question key repeats.

        Returns:
            The validated input.
        """
        keys = [question.key for question in self.questions]
        if len(keys) != len(set(keys)):
            raise ValueError("question keys must be unique")
        return self


class InvestigationCreateRequest(RequestModel):
    """Investigation create request."""

    agent_id: uuid.UUID = Field(
        description="Agent the investigation's sessions belong to."
    )
    name: str = Field(description="Investigation name.")
    description: str | None = Field(default=None, description="Curator rationale.")
    sessions: list[InvestigationSessionInput] = Field(
        description="Sessions to investigate, in presentation order."
    )


class InvestigationUpdateRequest(RequestModel):
    """Investigation update request."""

    name: str | None = Field(default=None, description="New investigation name.")
    description: str | None = Field(default=None, description="New curator rationale.")
    status: InvestigationStatus | None = Field(
        default=None, description="New investigation status."
    )


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
    started_at: datetime | None = Field(
        default=None, description="Time the first answer was recorded."
    )
    ended_at: datetime | None = Field(
        default=None, description="Time the last session settled."
    )
    metadata: dict[str, JsonValue] = Field(description="Arbitrary metadata.")
    total_sessions: int = Field(description="Number of linked sessions.")
    completed_sessions: int = Field(
        description="Number of linked sessions with a verdict."
    )


class InvestigationSessionsListParams(CursorParams):
    """Investigation sessions list params."""


class InvestigationSessionUpdateRequest(RequestModel):
    """Investigation session update request."""

    verdict: InvestigationSessionVerdict | None = Field(
        description="New investigation session verdict, None clears it."
    )


class InvestigationSessionResponse(TimestampedResponseModel):
    """Investigation session response."""

    id: uuid.UUID = Field(description="Investigation session id.")
    investigation_id: uuid.UUID = Field(
        description="Investigation this session belongs to."
    )
    session_id: uuid.UUID = Field(description="Session being investigated.")
    position: int = Field(description="Presentation order within the investigation.")
    questions: list[InvestigationSessionQuestion] = Field(
        description="Questions to answer about the session."
    )
    verdict: InvestigationSessionVerdict | None = Field(
        description="Investigation session verdict."
    )
