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
"""Replay configuration value objects, entity, and errors."""

import uuid
from datetime import datetime
from typing import Annotated, Any, Literal

from pydantic import Field

from kitaru.api_models.v1.replay_config import (
    HistoryScope,
    StaticMatchMode,
    ToolPolicyOnMiss,
)
from kitaru.base import FrozenModel
from kitaru.server.domain.agent_version import RuntimeCapabilities
from kitaru.server.domain.base import (
    ConflictError,
    DomainModel,
    NotFoundError,
    ValidationError,
)
from kitaru.server.domain.ids import uuid7
from kitaru.server.domain.names import NamespacedName


class ReplayConfigNotFound(NotFoundError):
    """Raised when a replay config lookup does not resolve."""

    def __init__(self, replay_config_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            replay_config_id: Id of the missing replay config.
        """
        super().__init__(f"Replay config {replay_config_id} was not found")


class ReplayConfigInUse(ConflictError):
    """Raised when a replay config is referenced by a replay and cannot be deleted."""

    def __init__(self, replay_config_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            replay_config_id: Id of the replay config.
        """
        super().__init__(f"Replay config {replay_config_id} is in use by a replay")


class ReplayOverride(FrozenModel):
    """Replay override."""

    model: str | dict[str, str] | None = None
    system_prompt: str | None = None
    prompt: str | None = None
    model_params: dict[str, Any] | None = None


class EvaluatorConfig(FrozenModel):
    """Evaluator config."""

    evaluator: NamespacedName
    version: int
    params: dict[str, Any] = Field(default_factory=dict)
    evaluator_version_id: uuid.UUID


class AnalyzerConfig(FrozenModel):
    """Analyzer config."""

    analyzer: NamespacedName
    version: int
    params: dict[str, Any] = Field(default_factory=dict)
    analyzer_version_id: uuid.UUID


class StaticCase(FrozenModel):
    """Static tool call case."""

    match: Any | None = None
    match_mode: StaticMatchMode
    result: Any


class PassthroughConfig(FrozenModel):
    """Passthrough tool config."""

    type: Literal["passthrough"] = "passthrough"


class HistoryConfig(FrozenModel):
    """History tool config."""

    type: Literal["history"] = "history"
    scope: HistoryScope
    on_miss: ToolPolicyOnMiss


class StaticConfig(FrozenModel):
    """Static tool config."""

    type: Literal["static"] = "static"
    cases: list[StaticCase]
    on_miss: ToolPolicyOnMiss


class LLMConfig(FrozenModel):
    """LLM tool config."""

    type: Literal["llm"] = "llm"
    model: str
    instructions: str | None = None


ToolConfig = Annotated[
    PassthroughConfig | HistoryConfig | StaticConfig | LLMConfig,
    Field(discriminator="type"),
]


class ToolPolicy(FrozenModel):
    """Tool policy."""

    default: ToolConfig
    tools: dict[str, ToolConfig] = Field(default_factory=dict)


def default_tool_policy() -> ToolPolicy:
    """Build the tool policy applied when a create request omits one.

    Returns:
        Tool policy passing every tool call through unmodified.
    """
    return ToolPolicy(default=PassthroughConfig(), tools={})


class ReplayConfig(DomainModel):
    """Replay config."""

    id: uuid.UUID = Field(default_factory=uuid7)
    owner_id: uuid.UUID
    override: ReplayOverride | None = None
    tool_policy: ToolPolicy
    evaluators: list[EvaluatorConfig]
    created: datetime | None = None
    updated: datetime | None = None

    def check_standalone(self) -> None:
        """Reject a cohort-version-scoped history tool config.

        A standalone replay has no cohort version to draw history from,
        unlike an experiment, which allows cohort_version scope.

        Raises:
            ValidationError: A tool config uses cohort-version-scoped history.
        """
        configs = [self.tool_policy.default, *self.tool_policy.tools.values()]
        for config in configs:
            if (
                isinstance(config, HistoryConfig)
                and config.scope is HistoryScope.COHORT_VERSION
            ):
                raise ValidationError(
                    "A standalone replay cannot use cohort-version-scoped history"
                )

    def check_capabilities(self, capabilities: RuntimeCapabilities) -> None:
        """Reject settings the agent version's runtime capabilities cannot apply.

        Args:
            capabilities: Runtime capabilities of the agent version.

        Raises:
            ValidationError: The config carries an override or a
                non-passthrough tool config the capabilities do not declare.
        """
        if not capabilities.overrides and self.override is not None:
            raise ValidationError("The agent version does not support replay overrides")
        if not capabilities.tool_policies:
            configs = [self.tool_policy.default, *self.tool_policy.tools.values()]
            if any(not isinstance(config, PassthroughConfig) for config in configs):
                raise ValidationError(
                    "The agent version does not support replay tool policies"
                )
