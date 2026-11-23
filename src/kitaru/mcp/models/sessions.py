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
"""Compact session transcript contracts."""

import uuid

from pydantic import Field, model_validator

from kitaru.mcp.models.common import MCPModel, ToolResult
from kitaru.session_transcript import DEFAULT_MAX_CHARS, SessionTranscript


class SessionReadRequest(MCPModel):
    """Read up to ten sessions as compact transcripts."""

    session_ids: list[uuid.UUID] = Field(min_length=1, max_length=10)
    max_chars: int = Field(
        default=DEFAULT_MAX_CHARS,
        ge=1,
        description="Longest text kept per field before it is cut.",
    )
    full: bool = Field(default=False, description="Keep every field uncut.")
    reasoning: bool = Field(
        default=False, description="Include the visible reasoning of model calls."
    )
    node: int | None = Field(
        default=None,
        ge=0,
        description="Show only the node at this zero-based position; needs one "
        "session.",
    )

    @model_validator(mode="after")
    def _validate_sessions(self) -> "SessionReadRequest":
        if len(set(self.session_ids)) != len(self.session_ids):
            raise ValueError("session_ids must be unique")
        return self


class SessionReadData(MCPModel):
    """Transcripts in request order."""

    transcripts: list[SessionTranscript]


class SessionReadResult(ToolResult):
    """Typed session transcript result."""

    data: SessionReadData | None = None
