"""Agent version filters and commands."""

import uuid

from kitaru.server.base import FrozenModel, ListFilter
from kitaru.server.domain.agent_version import AgentCapabilities, RunSpec


class AgentVersionFilter(ListFilter):
    """Agent version list filter."""

    agent_id: uuid.UUID


class AgentVersionUpdate(FrozenModel):
    """Partial agent version update."""

    display_version: str | None = None
    description: str | None = None
    run_spec: RunSpec | None = None
    capabilities: AgentCapabilities | None = None
