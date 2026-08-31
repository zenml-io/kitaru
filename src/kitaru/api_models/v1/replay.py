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
"""Replay API models."""

import uuid
from enum import StrEnum
from typing import Any, Self

from pydantic import Field, model_validator

from kitaru.api_models.v1.base import (
    RequestModel,
    ResponseModel,
    TimestampedResponseModel,
)
from kitaru.api_models.v1.filter import FilterableListParams
from kitaru.api_models.v1.replay_config import (
    EvaluatorConfig,
    ReplayOverride,
    ToolPolicy,
)
from kitaru.api_models.v1.session_node import NodeStatus


class ReplayStatus(StrEnum):
    """Replay status."""

    PENDING = "pending"
    EVALUATING = "evaluating"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"


class BaselineEvaluationMode(StrEnum):
    """Baseline evaluation mode."""

    NONE = "none"
    IF_MISSING = "if_missing"
    FORCE = "force"


def check_baseline_evaluation_fields(fields_set: set[str]) -> None:
    """Forbid setting both evaluate_baselines and baseline_evaluation_mode.

    Args:
        fields_set: Names of the fields the caller explicitly set.

    Raises:
        ValueError: Both fields were explicitly set.
    """
    if {"evaluate_baselines", "baseline_evaluation_mode"} <= fields_set:
        raise ValueError(
            "evaluate_baselines and baseline_evaluation_mode are mutually exclusive"
        )


class ReplayCreateRequest(RequestModel):
    """Replay create request."""

    baseline_session_id: uuid.UUID = Field(description="Session to replay.")
    agent_version_id: uuid.UUID | None = Field(
        default=None,
        description="Agent version to replay with, the baseline session's "
        "recorded version when unset.",
    )
    override: ReplayOverride | None = Field(
        default=None, description="Override to apply."
    )
    tool_policy: ToolPolicy | None = Field(
        default=None, description="Tool policy to apply."
    )
    evaluators: list[EvaluatorConfig] = Field(
        min_length=1, description="Evaluators run against the result session."
    )
    evaluate_baselines: bool = Field(
        default=False,
        deprecated="Use baseline_evaluation_mode instead.",
        description="Whether to also score the baseline session.",
    )
    baseline_evaluation_mode: BaselineEvaluationMode | None = Field(
        default=None, description="How to score the baseline session."
    )

    @model_validator(mode="after")
    def _evaluate_baselines_xor_mode(self) -> Self:
        """Forbid setting both evaluate_baselines and baseline_evaluation_mode.

        Raises:
            ValueError: Both fields were explicitly set.

        Returns:
            The validated request.
        """
        check_baseline_evaluation_fields(self.model_fields_set)
        return self


class ReplayListParams(FilterableListParams):
    """Replay list params."""


class ReplayResponse(TimestampedResponseModel):
    """Replay response."""

    id: uuid.UUID = Field(description="Replay id.")
    job_id: uuid.UUID | None = Field(
        default=None, description="Job running the replay."
    )
    experiment_run_id: uuid.UUID | None = Field(
        default=None, description="Experiment run this replay belongs to."
    )
    baseline_session_id: uuid.UUID = Field(description="Session replayed.")
    result_session_id: uuid.UUID | None = Field(
        default=None, description="Session produced by the replay."
    )
    override: ReplayOverride | None = Field(description="Override applied.")
    tool_policy: ToolPolicy = Field(description="Tool policy applied.")
    evaluators: list[EvaluatorConfig] = Field(
        description="Evaluators run against the result session."
    )
    evaluate_baselines: bool = Field(
        deprecated="Use baseline_evaluation_mode instead.",
        description="Whether the baseline session is also scored.",
    )
    baseline_evaluation_mode: BaselineEvaluationMode = Field(
        description="How the baseline session is scored."
    )
    status: ReplayStatus = Field(description="Replay status.")
    error: str | None = Field(default=None, description="Error from a failed replay.")


class ToolLookupRequest(RequestModel):
    """Tool lookup request."""

    tool_name: str = Field(description="Tool being called.")
    cache_key: str = Field(min_length=64, max_length=64, description="Call cache key.")
    occurrence: int | None = Field(
        default=None,
        ge=0,
        description="Zero-based match position in baseline order, the newest "
        "match when unset.",
    )


class ToolLookupMatch(ResponseModel):
    """Tool lookup match."""

    result: Any = Field(description="Cached tool result.")
    status: NodeStatus = Field(description="Tool call status.")
    error: str | None = Field(
        default=None, description="Error from a failed tool call."
    )


class ToolLookupResponse(ResponseModel):
    """Tool lookup response."""

    match: ToolLookupMatch | None = Field(
        default=None, description="Matching recorded tool call."
    )
