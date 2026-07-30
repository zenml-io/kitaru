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
"""Replay configuration API models, shared by experiments, replays, and evaluations."""

from enum import StrEnum
from typing import Annotated, Literal

from pydantic import Field

from kitaru.api_models.v1.base import DiscriminatedRequestModel, JsonValue, RequestModel


class HistoryScope(StrEnum):
    """Scope a history tool config draws recorded calls from."""

    BASELINE = "baseline"
    COHORT_VERSION = "cohort_version"
    AGENT = "agent"


class ToolPolicyOnMiss(StrEnum):
    """Behavior when a replayed tool call has no match."""

    FAIL = "fail"
    PASSTHROUGH = "passthrough"
    ERROR_RESULT = "error_result"


class StaticMatchMode(StrEnum):
    """How a static case matches a tool call."""

    EXACT = "exact"
    SUBSET = "subset"


class ReplayOverride(RequestModel):
    """Replay override."""

    model: str | dict[str, str] | None = Field(
        default=None, description="New model, or a map from old to new model."
    )
    system_prompt: str | None = Field(default=None, description="New system prompt.")
    prompt: str | None = Field(default=None, description="New prompt.")
    model_params: dict[str, JsonValue] | None = Field(
        default=None, description="New model parameters."
    )


class EvaluatorConfig(RequestModel):
    """Evaluator config."""

    evaluator: str = Field(description="Evaluator name.")
    version: int | None = Field(
        default=None,
        description="Evaluator version, an omitted value resolves to latest.",
    )
    params: dict[str, JsonValue] = Field(
        default_factory=dict, description="Parameters passed to the evaluator."
    )


class StaticCase(RequestModel):
    """Static tool call case."""

    match: JsonValue | None = Field(
        default=None, description="Call arguments to match, unset matches any call."
    )
    match_mode: StaticMatchMode = Field(
        description="How the call arguments are matched."
    )
    result: JsonValue = Field(description="Result returned for a matching call.")


class PassthroughConfig(DiscriminatedRequestModel):
    """Passthrough tool config."""

    type: Literal["passthrough"] = Field(default="passthrough")


class HistoryConfig(DiscriminatedRequestModel):
    """History tool config."""

    type: Literal["history"] = Field(default="history")
    scope: HistoryScope = Field(description="Source of recorded calls to replay from.")
    on_miss: ToolPolicyOnMiss = Field(
        description="Behavior when no recorded call matches."
    )


class StaticConfig(DiscriminatedRequestModel):
    """Static tool config."""

    type: Literal["static"] = Field(default="static")
    cases: list[StaticCase] = Field(
        description="Cases tried in order for a matching call."
    )
    on_miss: ToolPolicyOnMiss = Field(description="Behavior when no case matches.")


class LLMConfig(DiscriminatedRequestModel):
    """LLM tool config."""

    type: Literal["llm"] = Field(default="llm")
    model: str = Field(description="Model used to generate the tool result.")
    instructions: str | None = Field(
        default=None, description="Instructions guiding the generated result."
    )


ToolConfig = Annotated[
    PassthroughConfig | HistoryConfig | StaticConfig | LLMConfig,
    Field(discriminator="type"),
]


class ToolPolicy(RequestModel):
    """Tool policy."""

    default: ToolConfig = Field(
        description="Config applied to tools without an override."
    )
    tools: dict[str, ToolConfig] = Field(
        default_factory=dict, description="Per-tool config overrides."
    )
