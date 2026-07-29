"""Agent filters and commands."""

from kitaru.server.base import FrozenModel, ListFilter


class AgentFilter(ListFilter):
    """Agent list filter."""

    name: str | None = None


class AgentUpdate(FrozenModel):
    """Partial agent update."""

    name: str | None = None
    description: str | None = None
