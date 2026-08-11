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
"""Annotation API models."""

import uuid
from typing import Self

from pydantic import Field, model_validator

from kitaru.api_models.v1.base import JsonValue, OwnedResponseModel, RequestModel
from kitaru.api_models.v1.filter import FilterableListParams


class AnnotationSpan(RequestModel):
    """Annotation span."""

    start: int = Field(description="Start offset of the character range.")
    end: int = Field(description="End offset of the character range.")


class AnnotationSelector(RequestModel):
    """Annotation selector."""

    node_id: uuid.UUID | None = Field(default=None, description="Targeted node.")
    path: str | None = Field(
        default=None,
        description="RFC 6901 JSON Pointer into the targeted node or the "
        "session response.",
    )
    span: AnnotationSpan | None = Field(
        default=None, description="Character range within the resolved string."
    )

    @model_validator(mode="after")
    def _span_requires_path(self) -> Self:
        """Reject a span without a path.

        Raises:
            ValueError: span is set but path is not.

        Returns:
            The validated selector.
        """
        if self.span is not None and self.path is None:
            raise ValueError("span requires path")
        return self


class ManualAnnotationCreateRequest(RequestModel):
    """Manual annotation create request."""

    session_id: uuid.UUID = Field(description="Session being annotated.")
    selector: AnnotationSelector | None = Field(
        default=None, description="Part of the session being annotated."
    )
    value: JsonValue = Field(description="Annotation value.")


class InvestigationAnswerCreateRequest(RequestModel):
    """Investigation answer create request."""

    investigation_session_id: uuid.UUID = Field(
        description="Investigation session being answered."
    )
    selector: AnnotationSelector | None = Field(
        default=None, description="Part of the session being annotated."
    )
    value: JsonValue = Field(description="Annotation value.")


AnnotationCreateRequest = (
    ManualAnnotationCreateRequest | InvestigationAnswerCreateRequest
)


class AnnotationUpdateRequest(RequestModel):
    """Annotation update request."""

    value: JsonValue = Field(description="New annotation value.")


class AnnotationListParams(FilterableListParams):
    """Annotation list params."""


class AnnotationResponse(OwnedResponseModel):
    """Annotation response."""

    id: uuid.UUID = Field(description="Annotation id.")
    session_id: uuid.UUID = Field(description="Session being annotated.")
    investigation_session_id: uuid.UUID | None = Field(
        default=None, description="Investigation session being answered."
    )
    selector: AnnotationSelector | None = Field(
        default=None, description="Part of the session being annotated."
    )
    value: JsonValue = Field(description="Annotation value.")
