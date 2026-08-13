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
from typing import Any

from pydantic import Field

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


class ReplayStatus(StrEnum):
    """Replay status."""

    PENDING = "pending"
    EVALUATING = "evaluating"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"


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
        default=False, description="Whether to also score the baseline session."
    )


class ReplayListParams(FilterableListParams):
    """Replay list params."""


class ReplayResponse(TimestampedResponseModel):
    """Replay response."""

    id: uuid.UUID = Field(description="Replay id.")
    job_id: uuid.UUID = Field(description="Job running the replay.")
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
        description="Whether the baseline session is also scored."
    )
    status: ReplayStatus = Field(description="Replay status.")
    error: str | None = Field(default=None, description="Error from a failed replay.")


class ToolLookupRequest(RequestModel):
    """Tool lookup request."""

    tool_name: str = Field(description="Tool being called.")
    cache_key: str = Field(min_length=64, max_length=64, description="Call cache key.")


class ToolLookupResponse(ResponseModel):
    """Tool lookup response."""

    found: bool = Field(description="Whether a cached result was found.")
    result: Any = Field(description="Cached tool result.")
