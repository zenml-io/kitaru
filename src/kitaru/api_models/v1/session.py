"""Session API models."""

import uuid
from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import AwareDatetime, Field

from kitaru.api_models.v1.base import (
    JsonValue,
    ListParams,
    OwnedResponseModel,
    RequestModel,
)
from kitaru.api_models.v1.evaluation import EvaluationResult


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


class TokenUsage(RequestModel):
    """Token usage totals."""

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

    agent_id: uuid.UUID = Field(description="Agent id.")
    agent_version_id: uuid.UUID | None = Field(
        default=None, description="Agent version id."
    )
    origin: SessionOrigin = Field(description="Session origin.")
    status: SessionStatus | None = Field(
        default=None, description="Initial session status."
    )
    name: str | None = Field(default=None, description="Session name.")
    inputs: JsonValue = Field(default_factory=dict, description="Session inputs.")
    outputs: JsonValue | None = Field(default=None, description="Session outputs.")
    expected: JsonValue | None = Field(default=None, description="Expected outputs.")
    error: str | None = Field(default=None, description="Failure detail.")
    started_at: AwareDatetime | None = Field(default=None, description="Start time.")
    ended_at: AwareDatetime | None = Field(default=None, description="End time.")
    external_id: str | None = Field(default=None, description="External session id.")
    metadata: dict[str, JsonValue] = Field(
        default_factory=dict, description="Session metadata."
    )
    provider: str | None = Field(default=None, description="Source provider.")
    framework: str | None = Field(default=None, description="Agent framework.")
    adapter_version: str | None = Field(default=None, description="Adapter version.")
    task_id: uuid.UUID | None = Field(default=None, description="Creating task id.")


class SessionUpdateRequest(RequestModel):
    """Session update request."""

    status: SessionStatus | None = Field(default=None, description="New status.")
    outputs: JsonValue | None = Field(default=None, description="Replacement outputs.")
    error: str | None = Field(default=None, description="Failure detail.")
    ended_at: AwareDatetime | None = Field(default=None, description="End time.")
    name: str | None = Field(default=None, description="New session name.")
    expected: JsonValue | None = Field(
        default=None, description="Replacement expected outputs."
    )
    metadata: dict[str, JsonValue] | None = Field(
        default=None, description="Replacement metadata."
    )


class SessionEvaluationsRequest(RequestModel):
    """Manual session evaluations."""

    evaluations: list[EvaluationResult] = Field(
        min_length=1, description="Evaluation results."
    )


class SessionListParams(ListParams):
    """Session list params."""

    agent_id: uuid.UUID | None = Field(default=None, description="Filter on agent id.")
    agent_version_id: uuid.UUID | None = Field(
        default=None, description="Filter on agent version id."
    )
    task_id: uuid.UUID | None = Field(default=None, description="Filter on task id.")
    origin: SessionOrigin | None = Field(default=None, description="Filter on origin.")
    status: SessionStatus | None = Field(default=None, description="Filter on status.")
    provider: str | None = Field(default=None, description="Filter on provider.")
    external_id: str | None = Field(default=None, description="Filter on external id.")
    name: str | None = Field(default=None, description="Filter on session name.")
    tag: str | None = Field(default=None, description="Filter on tag name.")
    started_after: AwareDatetime | None = Field(
        default=None, description="Minimum start time."
    )
    started_before: AwareDatetime | None = Field(
        default=None, description="Maximum start time."
    )
    ended_after: AwareDatetime | None = Field(
        default=None, description="Minimum end time."
    )
    ended_before: AwareDatetime | None = Field(
        default=None, description="Maximum end time."
    )
    has_evaluation: bool | None = Field(
        default=None, description="Filter on evaluation presence."
    )
    min_cost: Decimal | None = Field(default=None, description="Minimum session cost.")
    max_cost: Decimal | None = Field(default=None, description="Maximum session cost.")


class SessionResponse(OwnedResponseModel):
    """Session response."""

    id: uuid.UUID = Field(description="Session id.")
    agent_id: uuid.UUID = Field(description="Agent id.")
    agent_version_id: uuid.UUID | None = Field(description="Agent version id.")
    task_id: uuid.UUID | None = Field(description="Creating task id.")
    origin: SessionOrigin = Field(description="Session origin.")
    status: SessionStatus = Field(description="Session status.")
    name: str | None = Field(description="Session name.")
    inputs: JsonValue = Field(description="Session inputs.")
    outputs: JsonValue | None = Field(description="Session outputs.")
    expected: JsonValue | None = Field(description="Expected outputs.")
    error: str | None = Field(description="Failure detail.")
    started_at: datetime | None = Field(description="Start time.")
    ended_at: datetime | None = Field(description="End time.")
    external_id: str | None = Field(description="External session id.")
    metadata: dict[str, JsonValue] = Field(description="Session metadata.")
    provider: str | None = Field(description="Source provider.")
    framework: str | None = Field(description="Agent framework.")
    adapter_version: str | None = Field(description="Adapter version.")
    cost: Decimal | None = Field(description="Session cost.")
    tokens: TokenUsage | None = Field(description="Token usage.")
    llm_call_count: int = Field(description="LLM call count.")
    tool_call_count: int = Field(description="Tool call count.")
