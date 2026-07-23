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

from kitaru.api_models.v1.base import RequestModel, ResponseModel


class SessionOrigin(StrEnum):
    """Session origin."""

    IMPORTED = "imported"
    RECORDED = "recorded"
    REPLAY = "replay"


class SessionStatus(StrEnum):
    """Session status."""

    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"


class SessionProvider(StrEnum):
    """Session provider."""

    LANGFUSE = "langfuse"
    BRAINTRUST = "braintrust"
    OTLP = "otlp"


class TokenUsage(RequestModel):
    """Token usage."""

    input_tokens: int | None = Field(default=None, description="Input token count.")
    output_tokens: int | None = Field(default=None, description="Output token count.")
    cached_input_tokens: int | None = Field(
        default=None, description="Cached input token count."
    )
    reasoning_tokens: int | None = Field(
        default=None, description="Reasoning token count."
    )


class SessionCreateRequest(RequestModel):
    """Session create request."""

    agent_id: uuid.UUID = Field(description="Id of the agent.")
    agent_version_id: uuid.UUID | None = Field(
        default=None, description="Id of the agent version."
    )
    origin: SessionOrigin = Field(description="Session origin.")
    status: SessionStatus | None = Field(
        default=None, description="Initial status, in progress when omitted."
    )
    name: str | None = Field(default=None, max_length=255, description="Display label.")
    inputs: Any = Field(default=None, description="Initial task inputs.")
    outputs: Any = Field(default=None, description="Final agent outputs.")
    expected: Any = Field(
        default=None, description="Ground truth for reference-based scorers."
    )
    error: str | None = Field(default=None, description="Error message.")
    started_at: AwareDatetime | None = Field(
        default=None, description="Execution start time."
    )
    ended_at: AwareDatetime | None = Field(
        default=None, description="Execution end time."
    )
    external_id: str | None = Field(
        default=None,
        max_length=255,
        description="External session or conversation id.",
    )
    metadata: dict[str, Any] = Field(default_factory=dict, description="User metadata.")
    provider: SessionProvider | None = Field(
        default=None, description="Source provider, imported sessions only."
    )
    framework: str | None = Field(
        default=None, max_length=64, description="Recording framework."
    )
    adapter_version: str | None = Field(
        default=None, max_length=64, description="Adapter version."
    )
    log_uri: str | None = Field(default=None, description="Log location.")
    replay_id: uuid.UUID | None = Field(
        default=None,
        description="Id of the replay this session is the result of.",
    )


class SessionUpdateRequest(RequestModel):
    """Session update request."""

    status: SessionStatus | None = Field(
        default=None, description="Terminal status finishing the session."
    )
    outputs: Any = Field(
        default=None, description="Final agent outputs, applied on finish."
    )
    error: str | None = Field(
        default=None, description="Error message, applied on finish."
    )
    ended_at: AwareDatetime | None = Field(
        default=None, description="Execution end time, applied on finish."
    )
    log_uri: str | None = Field(
        default=None, description="Log location, applied on finish."
    )
    name: str | None = Field(
        default=None, max_length=255, description="New display label."
    )
    expected: Any = Field(default=None, description="New expected outputs.")
    metadata: dict[str, Any] | None = Field(
        default=None, description="New user metadata."
    )


class SessionScoresRequest(RequestModel):
    """Session scores request."""

    scores: dict[str, float] = Field(
        description="Score values by scorer name, latest wins."
    )


class SessionResponse(ResponseModel):
    """Session response."""

    id: uuid.UUID = Field(description="Session id.")
    owner_id: uuid.UUID = Field(description="Id of the owning account.")
    agent_id: uuid.UUID = Field(description="Id of the agent.")
    agent_version_id: uuid.UUID | None = Field(description="Id of the agent version.")
    origin: SessionOrigin = Field(description="Session origin.")
    status: SessionStatus = Field(description="Session status.")
    name: str | None = Field(description="Display label.")
    inputs: Any = Field(description="Initial task inputs.")
    outputs: Any = Field(description="Final agent outputs.")
    expected: Any = Field(description="Ground truth for reference-based scorers.")
    error: str | None = Field(description="Error message.")
    started_at: datetime | None = Field(description="Execution start time.")
    ended_at: datetime | None = Field(description="Execution end time.")
    external_id: str | None = Field(description="External session or conversation id.")
    metadata: dict[str, Any] = Field(description="User metadata.")
    provider: SessionProvider | None = Field(
        description="Source provider, imported sessions only."
    )
    framework: str | None = Field(description="Recording framework.")
    adapter_version: str | None = Field(description="Adapter version.")
    log_uri: str | None = Field(description="Log location.")
    scores: dict[str, float] = Field(description="Score values by scorer name.")
    cost: Decimal | None = Field(description="Total cost rolled up from the nodes.")
    tokens: TokenUsage | None = Field(
        description="Token usage rolled up from the nodes."
    )
    llm_call_count: int = Field(description="Number of LLM call nodes.")
    tool_call_count: int = Field(description="Number of tool call nodes.")
    created: datetime = Field(description="Creation time.")
    updated: datetime = Field(description="Last modification time.")
