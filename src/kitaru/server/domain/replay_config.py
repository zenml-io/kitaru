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
"""Replay config entity, value objects, and errors."""

import uuid
from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any, Literal, Self

from pydantic import ConfigDict, Field, model_validator

from kitaru.server.base import FrozenModel
from kitaru.server.domain.base import (
    DomainModel,
    NotFoundError,
    ValidationError,
)
from kitaru.server.domain.ids import uuid7


class ReplayConfigNotFound(NotFoundError):
    """Raised when a replay config lookup does not resolve."""

    def __init__(self, config_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            config_id: Id of the missing replay config.
        """
        super().__init__(f"Replay config {config_id} was not found")


class InvalidReplayConfig(ValidationError):
    """Raised when a replay config violates its shape rules."""


class SourceRef(FrozenModel):
    """Python source reference."""

    module: str
    attribute: str

    @classmethod
    def parse(cls, ref: str) -> "SourceRef":
        """Parse a ``module:attribute`` reference.

        Args:
            ref: Reference to parse.

        Raises:
            InvalidReplayConfig: ``ref`` is not of the form
                ``module:attribute``.

        Returns:
            Parsed source reference.
        """
        module, separator, attribute = ref.partition(":")
        if not separator or not module or not attribute or ":" in attribute:
            raise InvalidReplayConfig(
                f"Source reference '{ref}' is not of the form 'module:attribute'"
            )
        return cls(module=module, attribute=attribute)

    def render(self) -> str:
        """Render the reference as ``module:attribute``.

        Returns:
            Rendered reference.
        """
        return f"{self.module}:{self.attribute}"


class ReplayOverride(FrozenModel):
    """Execution override."""

    model_config = ConfigDict(frozen=True, extra="forbid", protected_namespaces=())

    model: str | dict[str, str] | None = None
    system_prompt: str | None = None
    prompt: str | None = None
    model_params: dict[str, Any] | None = None


def effective_inputs(inputs: Any, override: ReplayOverride | None) -> Any:
    """Apply a prompt override to session inputs.

    Args:
        inputs: Original session inputs.
        override: Execution override.

    Returns:
        The override prompt when set, the inputs otherwise.
    """
    if override is not None and override.prompt is not None:
        return override.prompt
    return inputs


class ScorerConfig(FrozenModel):
    """Scorer configuration."""

    name: str
    source: SourceRef
    params: dict[str, Any] = Field(default_factory=dict)
    weight: float = Field(default=1.0, ge=0)
    fail_below: float | None = None


class ScoringPolicy(FrozenModel):
    """Scoring policy."""

    scorers: list[ScorerConfig]
    pass_threshold: float = Field(ge=0, le=1)

    @model_validator(mode="after")
    def validate_scorers(self) -> Self:
        """Validate the scorer list.

        Raises:
            InvalidReplayConfig: The scorer list is empty or contains
                duplicate names.

        Returns:
            The validated policy.
        """
        if not self.scorers:
            raise InvalidReplayConfig("Scoring policy requires at least one scorer")
        names = [scorer.name for scorer in self.scorers]
        if len(set(names)) != len(names):
            raise InvalidReplayConfig("Scorer names contain duplicates")
        return self


class ScoringResult(FrozenModel):
    """Scoring result."""

    passed: bool
    score: float
    scores: dict[str, float]


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


class StaticCase(FrozenModel):
    """Static tool result case."""

    match: dict[str, Any] | None = None
    match_mode: StaticMatchMode = StaticMatchMode.EXACT
    result: Any = None


class PassthroughPolicy(FrozenModel):
    """Passthrough tool policy."""

    type: Literal["passthrough"] = "passthrough"


class HistoryPolicy(FrozenModel):
    """History tool policy."""

    type: Literal["history"] = "history"
    scope: HistoryScope = HistoryScope.ORIGINAL_SESSION
    on_miss: ToolPolicyOnMiss = ToolPolicyOnMiss.FAIL


class StaticPolicy(FrozenModel):
    """Static tool policy."""

    type: Literal["static"] = "static"
    cases: list[StaticCase]
    on_miss: ToolPolicyOnMiss = ToolPolicyOnMiss.FAIL


class LLMPolicy(FrozenModel):
    """LLM tool policy."""

    type: Literal["llm"] = "llm"
    model: str
    instructions: str | None = None


ToolPolicy = Annotated[
    PassthroughPolicy | HistoryPolicy | StaticPolicy | LLMPolicy,
    Field(discriminator="type"),
]


class ToolPolicyConfig(FrozenModel):
    """Tool policy configuration."""

    default: ToolPolicy
    tools: dict[str, ToolPolicy] = Field(default_factory=dict)


class ReplayConfig(DomainModel):
    """Replay config."""

    id: uuid.UUID = Field(default_factory=uuid7)
    owner_id: uuid.UUID
    override: ReplayOverride | None = None
    tool_policy: ToolPolicyConfig
    scoring_policy: ScoringPolicy
    created: datetime | None = None
    updated: datetime | None = None

    def check_standalone(self) -> None:
        """Check that the config is valid for a standalone replay.

        Raises:
            InvalidReplayConfig: A history policy scopes to a cohort.
        """
        policies = [self.tool_policy.default, *self.tool_policy.tools.values()]
        for policy in policies:
            if (
                isinstance(policy, HistoryPolicy)
                and policy.scope is HistoryScope.COHORT
            ):
                raise InvalidReplayConfig(
                    "Standalone replays cannot use history scope 'cohort'"
                )
