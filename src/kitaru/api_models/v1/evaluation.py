"""Evaluation API models."""

import uuid
from enum import StrEnum
from typing import Any

from pydantic import Field, model_validator

from kitaru.api_models.v1.base import (
    FiniteFloat,
    ListParams,
    OwnedResponseModel,
    RequestModel,
)
from kitaru.api_models.v1.replay_config import EvaluatorConfig


class EvaluationDataType(StrEnum):
    """Evaluation value type."""

    FLOAT = "float"
    BOOL = "bool"
    STR = "str"
    CATEGORICAL = "categorical"


class EvaluationResult(RequestModel):
    """One named evaluation result."""

    name: str = Field(description="Evaluation name.")
    score: FiniteFloat | bool | None = Field(
        default=None, description="Numeric or boolean score."
    )
    value: str | None = Field(default=None, description="String or category value.")
    explanation: str | None = Field(default=None, description="Result explanation.")

    def __init__(self, *args: Any, **data: Any) -> None:
        """Initialize from fields or one positional bool, float, or string."""
        if args:
            if len(args) != 1:
                raise TypeError("EvaluationResult accepts one positional value")
            value = args[0]
            if isinstance(value, bool | float):
                data["score"] = value
            elif isinstance(value, str):
                data["value"] = value
            else:
                raise TypeError("positional value must be bool, float, or str")
        super().__init__(**data)

    @model_validator(mode="after")
    def _require_value(self) -> "EvaluationResult":
        if (
            "score" not in self.model_fields_set
            and "value" not in self.model_fields_set
        ):
            raise ValueError("score or value must be set")
        if self.score is None and self.value is None:
            raise ValueError("score or value must not both be null")
        return self

    @property
    def data_type(self) -> EvaluationDataType:
        """Derive the stored data type from the populated value channels."""
        if self.score is not None and self.value is not None:
            return EvaluationDataType.CATEGORICAL
        if isinstance(self.score, bool):
            return EvaluationDataType.BOOL
        if self.score is not None:
            return EvaluationDataType.FLOAT
        return EvaluationDataType.STR


class EvaluationBatchCreateRequest(RequestModel):
    """Evaluation batch create request."""

    input_session_ids: list[uuid.UUID] = Field(
        min_length=1, description="Unique input session ids."
    )
    evaluators: list[EvaluatorConfig] = Field(
        min_length=1, description="Evaluator configurations."
    )

    @model_validator(mode="after")
    def _unique_sessions(self) -> "EvaluationBatchCreateRequest":
        if len(set(self.input_session_ids)) != len(self.input_session_ids):
            raise ValueError("input_session_ids must be unique")
        return self


class EvaluationListParams(ListParams):
    """Evaluation list params."""

    session_id: uuid.UUID | None = Field(
        default=None, description="Filter on session id."
    )
    task_id: uuid.UUID | None = Field(default=None, description="Filter on task id.")
    evaluator_version_id: uuid.UUID | None = Field(
        default=None, description="Filter on evaluator version id."
    )
    name: str | None = Field(default=None, description="Filter on evaluation name.")
    data_type: EvaluationDataType | None = Field(
        default=None, description="Filter on data type."
    )


class EvaluationResponse(OwnedResponseModel):
    """Evaluation response."""

    id: uuid.UUID = Field(description="Evaluation id.")
    evaluator_version_id: uuid.UUID | None = Field(description="Evaluator version id.")
    evaluator_name: str | None = Field(description="Evaluator name.")
    evaluator_version: int | None = Field(description="Evaluator version number.")
    session_id: uuid.UUID = Field(description="Session id.")
    task_id: uuid.UUID | None = Field(description="Task id.")
    name: str = Field(description="Evaluation name.")
    data_type: EvaluationDataType = Field(description="Evaluation data type.")
    score: FiniteFloat | bool | None = Field(description="Numeric or boolean score.")
    value: str | None = Field(description="String or category value.")
    explanation: str | None = Field(description="Result explanation.")
