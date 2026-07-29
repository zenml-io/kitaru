"""Experiment filters and commands."""

from kitaru.server.base import FrozenModel, ListFilter
from kitaru.server.domain.replay_config import (
    EvaluatorConfig,
    ReplayOverride,
    ToolPolicy,
)


class ExperimentFilter(ListFilter):
    """Experiment list filter."""

    name: str | None = None
    tag: str | None = None


class ExperimentCreate(FrozenModel):
    """Experiment creation command."""

    name: str
    description: str | None = None
    override: ReplayOverride | None = None
    tool_policy: ToolPolicy = ToolPolicy()
    evaluators: list[EvaluatorConfig]


class ExperimentUpdate(FrozenModel):
    """Partial experiment update."""

    name: str | None = None
    description: str | None = None
    override: ReplayOverride | None = None
    tool_policy: ToolPolicy | None = None
    evaluators: list[EvaluatorConfig] | None = None
