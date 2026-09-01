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
"""Evaluation entity and errors."""

import uuid
from datetime import datetime
from typing import Any, Self

from pydantic import Field, model_validator

from kitaru.api_models.v1.base import FiniteFloat
from kitaru.api_models.v1.evaluation import EvaluationDataType
from kitaru.server.domain.base import (
    ConflictError,
    DomainModel,
    NotFoundError,
    ValidationError,
)
from kitaru.server.domain.ids import uuid7
from kitaru.server.domain.names import EvaluationName


class EvaluationNotFound(NotFoundError):
    """Raised when an evaluation lookup does not resolve."""

    def __init__(self, evaluation_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            evaluation_id: Id of the missing evaluation.
        """
        super().__init__(f"Evaluation {evaluation_id} was not found")


class InvalidEvaluation(ValidationError):
    """Raised when an evaluation's data type does not match its populated channels."""


class DuplicateEvaluationNameInBatch(ValidationError):
    """Raised when a batch of evaluations names the same evaluation twice."""

    def __init__(self, name: str) -> None:
        """Initialize the error.

        Args:
            name: Name that appears more than once in the batch.
        """
        super().__init__(
            f"Evaluation name '{name}' appears more than once in the request"
        )


class EvaluationNameConflict(ConflictError):
    """Raised when a manual evaluation name already exists for a session."""

    def __init__(self, name: str, session_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            name: Evaluation name that already exists.
            session_id: Id of the session.
        """
        super().__init__(f"Evaluation {name} already exists for session {session_id}")


class Evaluation(DomainModel):
    """Evaluation."""

    id: uuid.UUID = Field(default_factory=uuid7)
    owner_id: uuid.UUID
    evaluator_version_id: uuid.UUID | None = None
    session_id: uuid.UUID
    task_id: uuid.UUID | None = None
    name: EvaluationName
    data_type: EvaluationDataType
    score: FiniteFloat | bool | None = None
    value: str | None = None
    explanation: str | None = None
    passed: bool | None = None
    min_score: float | None = None
    max_score: float | None = None
    target_score: float | None = None
    evaluator_params: dict[str, Any] | None = None
    params_hash: str | None = None
    created: datetime | None = None
    updated: datetime | None = None

    @model_validator(mode="after")
    def _check_channels(self) -> Self:
        """Enforce that data_type matches which of score and value are set.

        Raises:
            InvalidEvaluation: Neither score nor value is set, the populated
                channels do not match data_type, or a scale field is set on
                an evaluation whose data_type is not float.

        Returns:
            The validated evaluation.
        """
        if self.score is None and self.value is None:
            raise InvalidEvaluation("At least one of score or value must be set")
        if self.data_type is not EvaluationDataType.FLOAT and (
            self.min_score is not None
            or self.max_score is not None
            or self.target_score is not None
        ):
            raise InvalidEvaluation(
                "min_score, max_score, and target_score require data_type float"
            )
        if self.data_type in (EvaluationDataType.FLOAT, EvaluationDataType.BOOL):
            if self.score is None or self.value is not None:
                raise InvalidEvaluation(
                    f"data_type {self.data_type.value} requires a lone score"
                )
            if isinstance(self.score, bool) != (
                self.data_type == EvaluationDataType.BOOL
            ):
                raise InvalidEvaluation(
                    f"data_type {self.data_type.value} does not match the score type"
                )
        elif self.data_type == EvaluationDataType.STR:
            if self.value is None or self.score is not None:
                raise InvalidEvaluation("data_type str requires a lone value")
        else:
            if self.score is None or self.value is None:
                raise InvalidEvaluation(
                    "data_type categorical requires both score and value"
                )
        return self
