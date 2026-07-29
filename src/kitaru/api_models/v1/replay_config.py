"""Replay configuration value objects."""

from enum import StrEnum
from typing import Annotated, Literal

from pydantic import Field

from kitaru.api_models.v1.base import (
    DiscriminatedRequestModel,
    JsonValue,
    RequestModel,
)


class HistoryScope(StrEnum):
    """History lookup scope."""

    BASELINE = "baseline"
    COHORT = "cohort"
    AGENT = "agent"


class ToolPolicyOnMiss(StrEnum):
    """Tool lookup behavior on a miss."""

    FAIL = "fail"
    PASSTHROUGH = "passthrough"
    ERROR_RESULT = "error_result"


class StaticMatchMode(StrEnum):
    """Static tool case matching mode."""

    EXACT = "exact"
    SUBSET = "subset"


class ReplayOverride(RequestModel):
    """Overrides applied during replay."""

    model: str | dict[str, str] | None = Field(
        default=None, description="Model replacement."
    )
    system_prompt: str | None = Field(
        default=None, description="System prompt replacement."
    )
    prompt: str | None = Field(default=None, description="Prompt replacement.")
    model_params: dict[str, JsonValue] | None = Field(
        default=None, description="Model parameter replacements."
    )


class EvaluatorConfig(RequestModel):
    """Resolved evaluator selection."""

    evaluator: str = Field(description="Evaluator name.")
    version: int | None = Field(default=None, description="Evaluator version.")
    params: dict[str, JsonValue] = Field(
        default_factory=dict, description="Evaluator parameters."
    )


class StaticCase(RequestModel):
    """Static tool result case."""

    match: dict[str, JsonValue] | None = Field(
        default=None, description="Arguments to match."
    )
    match_mode: StaticMatchMode = Field(
        default=StaticMatchMode.EXACT, description="Argument matching mode."
    )
    result: JsonValue = Field(description="Returned tool result.")


class PassthroughConfig(DiscriminatedRequestModel):
    """Call the real tool."""

    type: Literal["passthrough"] = "passthrough"


class HistoryConfig(DiscriminatedRequestModel):
    """Read a result from recorded history."""

    type: Literal["history"] = "history"
    scope: HistoryScope = Field(description="History lookup scope.")
    on_miss: ToolPolicyOnMiss = Field(description="Miss behavior.")


class StaticConfig(DiscriminatedRequestModel):
    """Read a result from static cases."""

    type: Literal["static"] = "static"
    cases: list[StaticCase] = Field(description="Static result cases.")
    on_miss: ToolPolicyOnMiss = Field(description="Miss behavior.")


class LLMConfig(DiscriminatedRequestModel):
    """Generate a result with an LLM."""

    type: Literal["llm"] = "llm"
    model: str = Field(description="Model name.")
    instructions: str | None = Field(
        default=None, description="Generation instructions."
    )


ToolConfig = Annotated[
    PassthroughConfig | HistoryConfig | StaticConfig | LLMConfig,
    Field(discriminator="type"),
]


class ToolPolicy(RequestModel):
    """Tool behavior policy."""

    default: ToolConfig = Field(description="Default tool configuration.")
    tools: dict[str, ToolConfig] = Field(
        default_factory=dict, description="Per-tool configurations."
    )
