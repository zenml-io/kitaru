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
"""Evaluation API models."""

import re
import uuid
from enum import StrEnum
from typing import Annotated, Any, Self

from pydantic import AfterValidator, Field, field_validator, model_validator

from kitaru.api_models.v1.base import (
    FiniteFloat,
    OwnedResponseModel,
    RequestModel,
)
from kitaru.api_models.v1.filter import FilterableListParams
from kitaru.api_models.v1.replay_config import EvaluatorConfig

MAX_NAME_LENGTH = 255

_NAME_PATTERN = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9_.-]*[A-Za-z0-9])?$")


def _validate_evaluation_name(value: str) -> str:
    """Validate an evaluation name against the restricted character set.

    Args:
        value: Name to validate.

    Raises:
        ValueError: The name is empty, too long, or outside the character set.

    Returns:
        Validated name.
    """
    if not value or len(value) > MAX_NAME_LENGTH:
        raise ValueError(f"Name must be 1-{MAX_NAME_LENGTH} characters")
    if not _NAME_PATTERN.fullmatch(value):
        raise ValueError(
            "Name must contain only letters, digits, '_', '.', '-', and must "
            "start and end with a letter or digit"
        )
    return value


EvaluationName = Annotated[str, AfterValidator(_validate_evaluation_name)]


class EvaluationDataType(StrEnum):
    """Data type an evaluation result carries."""

    FLOAT = "float"
    BOOL = "bool"
    STR = "str"
    CATEGORICAL = "categorical"


class EvaluationResult(RequestModel):
    """Evaluation result."""

    name: EvaluationName = Field(description="Evaluation name.")
    score: FiniteFloat | bool | None = Field(
        default=None, description="Numeric or boolean score."
    )
    value: str | None = Field(default=None, description="Label or string value.")
    explanation: str | None = Field(default=None, description="Free-form explanation.")
    passed: bool | None = Field(default=None, description="Pass or fail verdict.")

    def __init__(self, value: float | bool | str | None = None, **data: Any) -> None:
        """Route a single positional score or value by type.

        Args:
            value: Positional score, bool or float routes to score and str
                routes to value.
            data: Remaining fields, passed as keywords.
        """
        if isinstance(value, str):
            data.setdefault("value", value)
        elif value is not None:
            data.setdefault("score", value)
        super().__init__(**data)

    @model_validator(mode="after")
    def _check_score_or_value(self) -> Self:
        """Require at least one of score or value.

        Raises:
            ValueError: Neither score nor value is set.

        Returns:
            The validated result.
        """
        if self.score is None and self.value is None:
            raise ValueError("At least one of score or value must be set")
        return self

    @property
    def data_type(self) -> EvaluationDataType:
        """Data type derived from which of score and value are set.

        Returns:
            Derived data type.
        """
        if self.score is not None and self.value is not None:
            return EvaluationDataType.CATEGORICAL
        if self.score is not None:
            if isinstance(self.score, bool):
                return EvaluationDataType.BOOL
            return EvaluationDataType.FLOAT
        return EvaluationDataType.STR


class EvaluationBatchCreateRequest(RequestModel):
    """Evaluation batch create request."""

    input_session_ids: list[uuid.UUID] = Field(
        min_length=1, description="Sessions to score, all belonging to one agent."
    )
    evaluators: list[EvaluatorConfig] = Field(
        min_length=1, description="Evaluators run against every session."
    )

    @field_validator("input_session_ids")
    @classmethod
    def _check_unique(cls, value: list[uuid.UUID]) -> list[uuid.UUID]:
        """Reject duplicate session ids.

        Args:
            value: Session ids to check.

        Raises:
            ValueError: A session id appears more than once.

        Returns:
            Validated session ids.
        """
        if len(set(value)) != len(value):
            raise ValueError("input_session_ids must not contain duplicates")
        return value


class EvaluationListParams(FilterableListParams):
    """Evaluation list params."""


class EvaluationResponse(OwnedResponseModel):
    """Evaluation response."""

    id: uuid.UUID = Field(description="Evaluation id.")
    evaluator_version_id: uuid.UUID | None = Field(
        default=None, description="Evaluator version that produced the result."
    )
    evaluator_name: str | None = Field(
        default=None, description="Name of the evaluator that produced the result."
    )
    evaluator_version: int | None = Field(
        default=None, description="Version of the evaluator that produced the result."
    )
    session_id: uuid.UUID = Field(description="Session being scored.")
    task_id: uuid.UUID | None = Field(
        default=None, description="Evaluator task that produced the result."
    )
    name: str = Field(description="Evaluation name.")
    data_type: EvaluationDataType = Field(description="Data type of the result.")
    score: FiniteFloat | bool | None = Field(
        default=None, description="Numeric or boolean score."
    )
    value: str | None = Field(default=None, description="Label or string value.")
    explanation: str | None = Field(default=None, description="Free-form explanation.")
    passed: bool | None = Field(default=None, description="Pass or fail verdict.")
