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
    InvestigationSessionVerdict,
)
from kitaru.api_models.v1.tag import TagResourceType
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
    sessions: list[InvestigationSessionInput] = Field(max_length=100)

    @model_validator(mode="after")
    def _validate_contents(self) -> "InvestigationCreate":
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
    status: Literal["pending", "in_progress", "completed"] | None = None

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
        if "status" in self.model_fields_set and self.status is None:
            raise ValueError("status cannot be null")
        if not ({"name", "description", "status"} & self.model_fields_set) and not (
            self.clear_description
        ):
            raise ValueError("investigation update must change at least one field")
        return self


class SetInvestigationSessionVerdict(MCPModel):
    """Set or clear one linked session's verdict."""

    operation: Literal["set_session_verdict"]
    investigation_id: uuid.UUID
    session_id: uuid.UUID
    verdict: InvestigationSessionVerdict | None


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
    question_key: str
    selector: AnnotationSelector | None = None
    value: JsonValue = Field()


class AnnotationUpdate(MCPModel):
    """Update one annotation value."""

    operation: Literal["update_annotation"]
    annotation_id: uuid.UUID
    value: JsonValue = Field()


class TagCreate(MCPModel):
    """Create a tag."""

    operation: Literal["create_tag"]
    name: str = Field(min_length=1)


class TagUpdate(MCPModel):
    """Rename one tag by exact UUID."""

    operation: Literal["update_tag"]
    tag_id: uuid.UUID
    name: str = Field(min_length=1)


class TagLink(MCPModel):
    """Link one tag to one exact resource."""

    operation: Literal["link_tag"]
    tag_id: uuid.UUID
    resource_type: TagResourceType
    resource_id: uuid.UUID


ReviewManageRequest = Annotated[
    InvestigationCreate
    | InvestigationUpdate
    | SetInvestigationSessionVerdict
    | ManualAnnotationCreate
    | InvestigationAnswerCreate
    | AnnotationUpdate
    | TagCreate
    | TagUpdate
    | TagLink,
    Field(discriminator="operation"),
]
