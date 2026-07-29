"""Replay filters and creation command."""

import uuid

from kitaru.server.base import FrozenModel, ListFilter
from kitaru.server.domain.replay import ReplayStatus
from kitaru.server.domain.replay_config import (
    EvaluatorConfig,
    ReplayOverride,
    ToolPolicy,
)


class ReplayFilter(ListFilter):
    """Replay list filter."""

    experiment_run_id: uuid.UUID | None = None
    baseline_session_id: uuid.UUID | None = None
    status: ReplayStatus | None = None


class ReplayCreate(FrozenModel):
    """Standalone replay creation command."""

    baseline_session_id: uuid.UUID
    agent_version_id: uuid.UUID | None = None
    override: ReplayOverride | None = None
    tool_policy: ToolPolicy | None = None
    evaluators: list[EvaluatorConfig]
    evaluate_baselines: bool = False
