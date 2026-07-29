"""Evaluation entity and value types."""

import uuid
from datetime import datetime
from enum import StrEnum

from pydantic import Field

from kitaru.server.domain.base import DomainModel, NotFoundError
from kitaru.server.domain.ids import uuid7
from kitaru.server.domain.names import Name


class EvaluationDataType(StrEnum):
    """Stored evaluation value shape."""

    FLOAT = "float"
    BOOL = "bool"
    STR = "str"
    CATEGORICAL = "categorical"


class EvaluationNotFound(NotFoundError):
    """Raised when an evaluation lookup does not resolve."""

    def __init__(self, evaluation_id: uuid.UUID) -> None:
        super().__init__(f"Evaluation {evaluation_id} was not found")


class Evaluation(DomainModel):
    """One named evaluation attached to a session."""

    id: uuid.UUID = Field(default_factory=uuid7)
    owner_id: uuid.UUID
    evaluator_version_id: uuid.UUID | None = None
    session_id: uuid.UUID
    task_id: uuid.UUID | None = None
    name: Name
    data_type: EvaluationDataType
    score: float | bool | None = None
    value: str | None = None
    explanation: str | None = None
    created: datetime | None = None
    updated: datetime | None = None
