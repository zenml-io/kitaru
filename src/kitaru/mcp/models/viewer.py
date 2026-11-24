#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Trace viewer tool inputs and results."""

import uuid
from typing import Annotated, Literal

from pydantic import Field, model_validator

from kitaru.api_models.v1.annotation import AnnotationSelector
from kitaru.api_models.v1.base import JsonValue
from kitaru.api_models.v1.investigation import InvestigationSessionVerdict
from kitaru.mcp.models.common import IDEMPOTENCY_KEY_DESCRIPTION, MCPModel, ToolResult

VIEWER_NAME_PATTERN = r"^[a-z0-9][a-z0-9_-]{0,63}$"
ViewerName = Annotated[
    str,
    Field(
        pattern=VIEWER_NAME_PATTERN,
        description="Viewer to open: `default`, or the file name without `.html` "
        "of a viewer in the viewers folder.",
    ),
]
# Long enough for a selected paragraph, short enough that a quote cannot carry
# a whole payload back through the write tool.
MAX_QUOTE_CHARS = 20_000
MAX_NOTE_CHARS = 20_000
QUOTE_DESCRIPTION = (
    "Text the reviewer selected. The server compares it with the stored text at "
    "the selector's path and corrects or drops the span when they differ."
)
NoteText = Annotated[str, Field(min_length=1, max_length=MAX_NOTE_CHARS)]
Quote = Annotated[
    str | None,
    Field(default=None, max_length=MAX_QUOTE_CHARS, description=QUOTE_DESCRIPTION),
]
IdempotencyKey = Annotated[
    str | None, Field(default=None, description=IDEMPOTENCY_KEY_DESCRIPTION)
]


class SessionViewRequest(MCPModel):
    """Open one session, or one session of an investigation, in a trace viewer."""

    session_id: uuid.UUID | None = Field(default=None, description="Session to open.")
    investigation_id: uuid.UUID | None = Field(
        default=None,
        description="Investigation to review; the viewer shows its questions and "
        "saves answers to it.",
    )
    position: int = Field(
        default=0,
        ge=0,
        description="Zero-based position of the investigation session to open.",
    )
    viewer: ViewerName = "default"

    @model_validator(mode="after")
    def _validate_target(self) -> "SessionViewRequest":
        if (self.session_id is None) == (self.investigation_id is None):
            raise ValueError("pass exactly one of session_id or investigation_id")
        if self.session_id is not None and "position" in self.model_fields_set:
            raise ValueError("position needs investigation_id")
        return self


class ViewerViewRead(SessionViewRequest):
    """Read one session with its nodes, notes, and investigation for a viewer."""

    operation: Literal["view"]


class ViewerSourceRead(MCPModel):
    """Read the HTML of one viewer."""

    operation: Literal["viewer_source"]
    viewer: ViewerName


class ViewerNodesRead(MCPModel):
    """Read the next page of a session's nodes with their payloads."""

    operation: Literal["nodes"]
    session_id: uuid.UUID
    cursor: str = Field(min_length=1)
    size: int = Field(default=100, ge=1, le=500)


ViewerReadRequest = Annotated[
    ViewerViewRead | ViewerSourceRead | ViewerNodesRead,
    Field(discriminator="operation"),
]


class ViewerAnswer(MCPModel):
    """Answer one investigation question with a plain-text note."""

    operation: Literal["answer"]
    investigation_id: uuid.UUID
    investigation_session_id: uuid.UUID
    question_key: str = Field(min_length=1)
    selector: AnnotationSelector | None = None
    quote: Quote = None
    text: NoteText
    idempotency_key: IdempotencyKey = None


class ViewerNote(MCPModel):
    """Write a plain-text note on a session, a node, a field, or a text span."""

    operation: Literal["note"]
    session_id: uuid.UUID
    selector: AnnotationSelector | None = None
    quote: Quote = None
    text: NoteText
    idempotency_key: IdempotencyKey = None


class ViewerEdit(MCPModel):
    """Replace the text of one annotation; its selector stays as it was."""

    operation: Literal["edit"]
    annotation_id: uuid.UUID
    text: NoteText


class ViewerVerdict(MCPModel):
    """Set or clear the verdict of one investigation session."""

    operation: Literal["verdict"]
    investigation_id: uuid.UUID
    session_id: uuid.UUID
    verdict: InvestigationSessionVerdict | None


ViewerWriteRequest = Annotated[
    ViewerAnswer | ViewerNote | ViewerEdit | ViewerVerdict,
    Field(discriminator="operation"),
]


class ViewerCapabilities(MCPModel):
    """What the viewer may do under the server's capability mode."""

    mode: str
    can_write: bool


class InvestigationPosition(MCPModel):
    """One session's place in an investigation, for navigation."""

    position: int
    session_id: uuid.UUID
    verdict: InvestigationSessionVerdict | None


class ViewerInvestigation(MCPModel):
    """The investigation being reviewed and the session open in it."""

    id: uuid.UUID
    name: str
    description: str | None
    status: str
    position: int
    investigation_session: dict[str, JsonValue]
    sessions: list[InvestigationPosition]


# Session, node, and annotation records are passed through as JSON objects:
# the viewer page reads them, not the model, and their full API schemas would
# add tens of kilobytes to tool discovery.
class SessionViewData(MCPModel):
    """One session with its nodes and annotations, ready for a viewer page."""

    viewer: str
    session: dict[str, JsonValue]
    nodes: list[dict[str, JsonValue]]
    nodes_cursor: str | None
    annotations: list[dict[str, JsonValue]]
    annotations_truncated: bool
    investigation: ViewerInvestigation | None
    capabilities: ViewerCapabilities
    links: dict[str, str]


class ViewerQuestion(MCPModel):
    """One question the reviewer answers about the open session."""

    key: str
    question: str


class SessionViewOpened(MCPModel):
    """The session a viewer opened, without its payloads."""

    viewer: str
    session_id: uuid.UUID
    session_name: str | None
    llm_call_count: int
    tool_call_count: int
    investigation_id: uuid.UUID | None
    investigation_name: str | None
    position: int | None
    session_count: int | None = Field(
        description="Sessions in the investigation, when one is open."
    )
    questions: list[ViewerQuestion]
    can_write: bool


class ViewerSource(MCPModel):
    """The HTML of one viewer."""

    viewer: str
    html: str


class ViewerNodesPage(MCPModel):
    """One page of a session's nodes with their payloads."""

    nodes: list[dict[str, JsonValue]]
    nodes_cursor: str | None


class SessionViewResult(ToolResult):
    """Session view result."""

    data: SessionViewOpened | None = None


class ViewerReadResult(ToolResult):
    """Viewer read result."""

    data: SessionViewData | ViewerSource | ViewerNodesPage | None = None


class ViewerWriteResult(ToolResult):
    """Viewer write result: the stored annotation or investigation session."""

    data: dict[str, JsonValue] | None = None
