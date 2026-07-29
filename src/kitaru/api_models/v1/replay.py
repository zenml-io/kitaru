"""Replay API models."""

import uuid
from enum import StrEnum

from pydantic import Field

from kitaru.api_models.v1.base import (
    JsonValue,
    ListParams,
    RequestModel,
    ResponseModel,
    TimestampedResponseModel,
)
from kitaru.api_models.v1.replay_config import (
    EvaluatorConfig,
    ReplayOverride,
    ToolPolicy,
)


class ReplayStatus(StrEnum):
    """Replay status."""

    PENDING = "pending"
    EVALUATING = "evaluating"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"


class ReplayCreateRequest(RequestModel):
    """Replay create request."""

    baseline_session_id: uuid.UUID = Field(description="Baseline session id.")
    agent_version_id: uuid.UUID | None = Field(
        default=None, description="Agent version id."
    )
    override: ReplayOverride | None = Field(
        default=None, description="Replay overrides."
    )
    tool_policy: ToolPolicy | None = Field(
        default=None, description="Replay tool policy."
    )
    evaluators: list[EvaluatorConfig] = Field(
        min_length=1, description="Evaluator configurations."
    )
    evaluate_baselines: bool = Field(
        default=False, description="Whether to evaluate the baseline."
    )


class ReplayListParams(ListParams):
    """Replay list params."""

    experiment_run_id: uuid.UUID | None = Field(
        default=None, description="Filter on experiment run id."
    )
    baseline_session_id: uuid.UUID | None = Field(
        default=None, description="Filter on baseline session id."
    )
    status: ReplayStatus | None = Field(default=None, description="Filter on status.")


class ReplayResponse(TimestampedResponseModel):
    """Replay response."""

    id: uuid.UUID = Field(description="Replay id.")
    job_id: uuid.UUID = Field(description="Job id.")
    experiment_run_id: uuid.UUID | None = Field(description="Experiment run id.")
    baseline_session_id: uuid.UUID = Field(description="Baseline session id.")
    result_session_id: uuid.UUID | None = Field(description="Result session id.")
    override: ReplayOverride | None = Field(description="Replay overrides.")
    tool_policy: ToolPolicy = Field(description="Replay tool policy.")
    evaluators: list[EvaluatorConfig] = Field(description="Evaluator configurations.")
    evaluate_baselines: bool = Field(description="Whether the baseline is evaluated.")
    status: ReplayStatus = Field(description="Replay status.")
    error: str | None = Field(description="Failure detail.")


class ToolLookupRequest(RequestModel):
    """Replay tool lookup request."""

    tool_name: str = Field(description="Tool name.")
    cache_key: str = Field(min_length=64, max_length=64, description="Call cache key.")


class ToolLookupResponse(ResponseModel):
    """Replay tool lookup response."""

    found: bool = Field(description="Whether a result was found.")
    result: JsonValue | None = Field(description="Recorded tool result.")
