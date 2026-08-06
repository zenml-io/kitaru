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
"""Session API models."""

import uuid
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

from pydantic import AwareDatetime, Field

from kitaru.api_models.v1.base import (
    JsonValue,
    OwnedResponseModel,
    RequestModel,
)
from kitaru.api_models.v1.evaluation import EvaluationResult
from kitaru.api_models.v1.filter import FilterableListParams


class SessionOrigin(StrEnum):
    """How a session came to exist."""

    IMPORTED = "imported"
    RECORDED = "recorded"
    REPLAY = "replay"


class SessionStatus(StrEnum):
    """Session status."""

    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"


class TokenUsage(RequestModel):
    """Token usage."""

    input_tokens: int | None = Field(default=None, description="Input tokens.")
    output_tokens: int | None = Field(default=None, description="Output tokens.")
    cached_input_tokens: int | None = Field(
        default=None, description="Cached input tokens."
    )
    reasoning_tokens: int | None = Field(default=None, description="Reasoning tokens.")


class SessionCreateRequest(RequestModel):
    """Session create request."""

    agent_id: uuid.UUID | None = Field(
        default=None,
        description="Agent the session belongs to, inferred from the task or "
        "the agent version when unset.",
    )
    agent_version_id: uuid.UUID | None = Field(
        default=None,
        description="Agent version recorded for the session, inferred from "
        "the task when unset.",
    )
    origin: SessionOrigin = Field(description="How the session came to exist.")
    status: SessionStatus | None = Field(
        default=None, description="Initial session status."
    )
    name: str | None = Field(default=None, description="Session name.")
    inputs: Any = Field(description="Session inputs.")
    outputs: Any = Field(description="Session outputs.")
    error: str | None = Field(default=None, description="Error from a failed session.")
    started_at: AwareDatetime | None = Field(
        default=None, description="Time the session started."
    )
    ended_at: AwareDatetime | None = Field(
        default=None, description="Time the session ended."
    )
    external_id: str | None = Field(
        default=None, description="Id from the source system."
    )
    metadata: dict[str, JsonValue] = Field(
        default_factory=dict, description="Arbitrary metadata."
    )
    imported_from: str | None = Field(
        default=None, description="Source system the session was imported from."
    )
    framework: str | None = Field(default=None, description="Agent framework used.")
    adapter_version: str | None = Field(
        default=None, description="Recording adapter version."
    )


class SessionUpdateRequest(RequestModel):
    """Session update request."""

    status: SessionStatus | None = Field(
        default=None, description="New session status."
    )
    outputs: Any = Field(default=None, description="New session outputs.")
    error: str | None = Field(default=None, description="New error.")
    ended_at: AwareDatetime | None = Field(default=None, description="New end time.")
    name: str | None = Field(default=None, description="New session name.")
    metadata: dict[str, JsonValue] | None = Field(
        default=None, description="New metadata."
    )


class SessionEvaluationsRequest(RequestModel):
    """Session evaluations request."""

    evaluations: list[EvaluationResult] = Field(
        min_length=1, description="Evaluations to merge into the session."
    )


class SessionListParams(FilterableListParams):
    """Session list params."""


class SessionResponse(OwnedResponseModel):
    """Session response."""

    id: uuid.UUID = Field(description="Session id.")
    agent_id: uuid.UUID = Field(description="Agent the session belongs to.")
    number: int = Field(description="Session number within the agent.")
    agent_version_id: uuid.UUID | None = Field(
        default=None, description="Agent version recorded for the session."
    )
    task_id: uuid.UUID | None = Field(
        default=None, description="Task the session was produced by."
    )
    origin: SessionOrigin = Field(description="How the session came to exist.")
    status: SessionStatus = Field(description="Session status.")
    name: str | None = Field(default=None, description="Session name.")
    inputs: Any = Field(description="Session inputs.")
    outputs: Any = Field(description="Session outputs.")
    error: str | None = Field(default=None, description="Error from a failed session.")
    started_at: datetime | None = Field(
        default=None, description="Time the session started."
    )
    ended_at: datetime | None = Field(
        default=None, description="Time the session ended."
    )
    external_id: str | None = Field(
        default=None, description="Id from the source system."
    )
    metadata: dict[str, JsonValue] = Field(description="Arbitrary metadata.")
    imported_from: str | None = Field(
        default=None, description="Source system the session was imported from."
    )
    framework: str | None = Field(default=None, description="Agent framework used.")
    adapter_version: str | None = Field(
        default=None, description="Recording adapter version."
    )
    cost: Decimal | None = Field(default=None, description="Total cost.")
    tokens: TokenUsage | None = Field(default=None, description="Total token usage.")
    llm_call_count: int = Field(description="Number of LLM call nodes.")
    tool_call_count: int = Field(description="Number of tool call nodes.")
