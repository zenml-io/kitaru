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
from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import ConfigDict, Field

from kitaru.api_models.v1.base import RequestModel, ResponseModel

SOURCE_REF_PATTERN = r"^[^:\s]+:[^:\s]+$"


class ReplayStatus(StrEnum):
    """Replay status."""

    PENDING = "pending"
    CLAIMED = "claimed"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMED_OUT = "timed_out"
    CANCELED = "canceled"


class HistoryScope(StrEnum):
    """History policy lookup scope."""

    ORIGINAL_SESSION = "original_session"
    COHORT = "cohort"
    AGENT = "agent"


class ToolPolicyOnMiss(StrEnum):
    """Tool policy miss behavior."""

    FAIL = "fail"
    PASSTHROUGH = "passthrough"
    ERROR_RESULT = "error_result"


class StaticMatchMode(StrEnum):
    """Static case match mode."""

    EXACT = "exact"
    SUBSET = "subset"


class ReplayOverride(RequestModel):
    """Execution override."""

    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    model: str | dict[str, str] | None = Field(
        default=None,
        description="Replacement model, or an old to new model map.",
    )
    system_prompt: str | None = Field(
        default=None, description="Replacement system prompt."
    )
    prompt: str | None = Field(default=None, description="Replacement session inputs.")
    model_params: dict[str, Any] | None = Field(
        default=None, description="Replacement model parameters."
    )


class ScorerConfig(RequestModel):
    """Scorer configuration."""

    name: str = Field(description="Scorer name, unique within the policy.")
    source: str = Field(
        pattern=SOURCE_REF_PATTERN,
        description="Scoring function reference as 'module:attribute'.",
    )
    params: dict[str, Any] = Field(
        default_factory=dict, description="Keyword arguments for the function."
    )
    weight: float = Field(default=1.0, ge=0, description="Weight in the average.")
    fail_below: float | None = Field(
        default=None,
        description="Score at or below which the replay fails outright.",
    )


class ScoringPolicy(RequestModel):
    """Scoring policy."""

    scorers: list[ScorerConfig] = Field(min_length=1, description="Scorers to run.")
    pass_threshold: float = Field(
        ge=0, le=1, description="Weighted average required to pass."
    )


class StaticCase(RequestModel):
    """Static tool result case."""

    match: dict[str, Any] | None = Field(
        default=None, description="Inputs to match, any inputs when omitted."
    )
    match_mode: StaticMatchMode = Field(
        default=StaticMatchMode.EXACT, description="Match mode."
    )
    result: Any = Field(default=None, description="Tool result to return.")


class PassthroughPolicy(RequestModel):
    """Passthrough tool policy."""

    type: Literal["passthrough"] = "passthrough"


class HistoryPolicy(RequestModel):
    """History tool policy."""

    type: Literal["history"] = "history"
    scope: HistoryScope = Field(
        default=HistoryScope.ORIGINAL_SESSION, description="Lookup scope."
    )
    on_miss: ToolPolicyOnMiss = Field(
        default=ToolPolicyOnMiss.FAIL, description="Miss behavior."
    )


class StaticPolicy(RequestModel):
    """Static tool policy."""

    type: Literal["static"] = "static"
    cases: list[StaticCase] = Field(
        description="Cases evaluated in order, first match wins."
    )
    on_miss: ToolPolicyOnMiss = Field(
        default=ToolPolicyOnMiss.FAIL, description="Miss behavior."
    )


class LLMPolicy(RequestModel):
    """LLM tool policy."""

    type: Literal["llm"] = "llm"
    model: str = Field(description="Model generating the tool results.")
    instructions: str | None = Field(
        default=None, description="Generation instructions."
    )


ToolPolicy = Annotated[
    PassthroughPolicy | HistoryPolicy | StaticPolicy | LLMPolicy,
    Field(discriminator="type"),
]


class ToolPolicyConfig(RequestModel):
    """Tool policy configuration."""

    default: ToolPolicy = Field(description="Policy for tools without an override.")
    tools: dict[str, ToolPolicy] = Field(
        default_factory=dict, description="Per-tool override by tool name."
    )


class ReplayCreateRequest(RequestModel):
    """Replay create request."""

    original_session_id: uuid.UUID = Field(description="Id of the session to replay.")
    agent_version_id: uuid.UUID | None = Field(
        default=None,
        description="Id of the agent version to execute, the latest runnable "
        "version when omitted.",
    )
    override: ReplayOverride | None = Field(
        default=None, description="Execution override."
    )
    tool_policy: ToolPolicyConfig | None = Field(
        default=None,
        description="Tool policy, a history policy scoped to the original "
        "session when omitted.",
    )
    scoring_policy: ScoringPolicy = Field(description="Scoring policy.")


class ReplayResponse(ResponseModel):
    """Replay response."""

    id: uuid.UUID = Field(description="Replay id.")
    experiment_run_id: uuid.UUID | None = Field(
        description="Id of the experiment run, null for standalone replays."
    )
    agent_version_id: uuid.UUID = Field(description="Id of the agent version.")
    original_session_id: uuid.UUID = Field(description="Id of the replayed session.")
    result_session_id: uuid.UUID | None = Field(description="Id of the result session.")
    status: ReplayStatus = Field(description="Replay status.")
    attempt: int = Field(description="Attempt counter.")
    worker_id: str | None = Field(description="Id of the claiming worker.")
    claimed_at: datetime | None = Field(description="Claim time.")
    heartbeat_at: datetime | None = Field(description="Last heartbeat time.")
    started_at: datetime | None = Field(description="Execution start time.")
    ended_at: datetime | None = Field(description="Execution end time.")
    error: str | None = Field(description="Error message.")
    passed: bool | None = Field(description="Scoring outcome, null until scored.")
    score: float | None = Field(description="Weighted average, null until scored.")
    scores: dict[str, float] | None = Field(
        description="Scores by scorer name, null until scored."
    )
    diff: dict[str, Any] | None = Field(
        description="Diff summary, written at completion."
    )
    override: ReplayOverride | None = Field(description="Execution override.")
    tool_policy: ToolPolicyConfig = Field(description="Tool policy.")
    scoring_policy: ScoringPolicy = Field(description="Scoring policy.")
    created: datetime = Field(description="Creation time.")
    updated: datetime = Field(description="Last modification time.")
