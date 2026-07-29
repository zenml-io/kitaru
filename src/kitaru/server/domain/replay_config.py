"""Replay configuration value objects and entity."""

import uuid
from typing import Annotated, Any, Literal

from pydantic import Field

from kitaru.base import FrozenModel
from kitaru.server.domain.base import DomainModel, ValidationError
from kitaru.server.domain.ids import uuid7


class ReplayOverride(FrozenModel):
    """Agent input and model overrides."""

    model: str | dict[str, str] | None = None
    system_prompt: str | None = None
    prompt: str | None = None
    model_params: dict[str, Any] | None = None


class EvaluatorConfig(FrozenModel):
    """Resolved evaluator selection."""

    evaluator: str
    version: int | None = None
    params: dict[str, Any] = Field(default_factory=dict)
    evaluator_version_id: uuid.UUID | None = None


class StaticMatchMode(str):
    """Static tool input matching mode."""


class PassthroughConfig(FrozenModel):
    """Run the live tool when replaying."""

    type: Literal["passthrough"] = "passthrough"


class HistoryConfig(FrozenModel):
    """Return a recorded tool result."""

    type: Literal["history"] = "history"
    scope: Literal["baseline", "cohort", "agent"] = "baseline"
    on_miss: Literal["fail", "passthrough", "error_result"] = "fail"


class StaticCase(FrozenModel):
    """One static tool response case."""

    match: Any = None
    match_mode: Literal["exact", "subset"] = "exact"
    result: Any = None


class StaticConfig(FrozenModel):
    """Return a configured static tool result."""

    type: Literal["static"] = "static"
    cases: list[StaticCase] = Field(default_factory=list)
    on_miss: Literal["fail", "passthrough", "error_result"] = "fail"


class LLMConfig(FrozenModel):
    """Generate a tool result with a model."""

    type: Literal["llm"] = "llm"
    model: str
    instructions: str | None = None


ToolConfig = Annotated[
    PassthroughConfig | HistoryConfig | StaticConfig | LLMConfig,
    Field(discriminator="type"),
]


class ToolPolicy(FrozenModel):
    """Tool behavior by name with a default."""

    default: ToolConfig = Field(default_factory=PassthroughConfig)
    tools: dict[str, ToolConfig] = Field(default_factory=dict)


class InvalidReplayConfig(ValidationError):
    """Raised when replay configuration is invalid in its context."""


class ReplayConfig(DomainModel):
    """Persisted replay configuration."""

    id: uuid.UUID = Field(default_factory=uuid7)
    owner_id: uuid.UUID
    override: ReplayOverride | None = None
    tool_policy: ToolPolicy = Field(default_factory=ToolPolicy)
    evaluators: list[EvaluatorConfig] = Field(default_factory=list)

    def check_standalone(self) -> None:
        """Reject cohort history outside an experiment run."""
        configs = [self.tool_policy.default, *self.tool_policy.tools.values()]
        if any(
            isinstance(config, HistoryConfig) and config.scope == "cohort"
            for config in configs
        ):
            raise InvalidReplayConfig("Cohort history scope requires an experiment run")


def effective_inputs(inputs: Any, override: ReplayOverride | None) -> Any:
    """Apply the prompt override to otherwise opaque inputs."""
    if override is None or override.prompt is None:
        return inputs
    if isinstance(inputs, dict):
        return {**inputs, "prompt": override.prompt}
    return inputs
