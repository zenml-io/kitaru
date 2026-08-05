#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Strict investigation and annotation tool inputs."""

import uuid
from typing import Annotated, Literal

from pydantic import Field, model_validator

from kitaru.api_models.v1.annotation import AnnotationSelector
from kitaru.api_models.v1.base import JsonValue
from kitaru.api_models.v1.filter import Filter
from kitaru.api_models.v1.investigation import (
    InvestigationSessionInput,
    QuestionItem,
)
from kitaru.mcp.models.common import MCPModel, PageOptions

ReviewKind = Literal["investigation", "annotation"]


class ReviewList(PageOptions):
    """List one bounded page of investigations or annotations."""

    operation: Literal["list"]
    kind: ReviewKind
    filter: Filter | None = None


class ReviewGet(MCPModel):
    """Get one investigation or annotation by exact UUID."""

    operation: Literal["get"]
    kind: ReviewKind
    id: uuid.UUID


class ReviewListSessions(MCPModel):
    """List one ordered page of sessions for an exact investigation."""

    operation: Literal["list_sessions"]
    investigation_id: uuid.UUID
    cursor: str | None = None
    size: int = Field(default=20, ge=1, le=100)


ReviewReadRequest = Annotated[
    ReviewList | ReviewGet | ReviewListSessions,
    Field(discriminator="operation"),
]


class InvestigationCreate(MCPModel):
    """Create an investigation and its ordered sessions."""

    operation: Literal["create_investigation"]
    agent_id: uuid.UUID
    name: str = Field(min_length=1)
    description: str | None = None
    questions: list[QuestionItem] = Field(max_length=100)
    sessions: list[InvestigationSessionInput] = Field(max_length=100)

    @model_validator(mode="after")
    def _validate_contents(self) -> "InvestigationCreate":
        question_keys = [item.key for item in self.questions]
        if len(set(question_keys)) != len(question_keys):
            raise ValueError("question keys must be unique")
        session_ids = [item.session_id for item in self.sessions]
        if len(set(session_ids)) != len(session_ids):
            raise ValueError("session ids must be unique")
        return self


class InvestigationUpdate(MCPModel):
    """Sparsely update investigation metadata."""

    operation: Literal["update_investigation"]
    investigation_id: uuid.UUID
    name: str | None = None
    description: str | None = None
    clear_description: bool = False

    @model_validator(mode="after")
    def _validate_update(self) -> "InvestigationUpdate":
        if "name" in self.model_fields_set and self.name is None:
            raise ValueError("name cannot be null")
        if (
            "description" in self.model_fields_set
            and self.description is None
            and not self.clear_description
        ):
            raise ValueError("description cannot be null without clear_description")
        if self.description is not None and self.clear_description:
            raise ValueError("description and clear_description conflict")
        if not ({"name", "description"} & self.model_fields_set) and not (
            self.clear_description
        ):
            raise ValueError("investigation update must change at least one field")
        return self


class SetInvestigationSessionStatus(MCPModel):
    """Set one linked session to a terminal status."""

    operation: Literal["set_session_status"]
    investigation_id: uuid.UUID
    session_id: uuid.UUID
    status: Literal["completed", "skipped"]


class ManualAnnotationCreate(MCPModel):
    """Create a manual annotation for one session."""

    operation: Literal["create_annotation"]
    session_id: uuid.UUID
    selector: AnnotationSelector | None = None
    value: JsonValue = Field()


class InvestigationAnswerCreate(MCPModel):
    """Answer one investigation question for one linked session."""

    operation: Literal["answer_question"]
    investigation_session_id: uuid.UUID
    question_key: str = Field(min_length=1)
    selector: AnnotationSelector | None = None
    value: JsonValue = Field()


class AnnotationUpdate(MCPModel):
    """Update one annotation value."""

    operation: Literal["update_annotation"]
    annotation_id: uuid.UUID
    value: JsonValue = Field()


ReviewManageRequest = Annotated[
    InvestigationCreate
    | InvestigationUpdate
    | SetInvestigationSessionStatus
    | ManualAnnotationCreate
    | InvestigationAnswerCreate
    | AnnotationUpdate,
    Field(discriminator="operation"),
]
