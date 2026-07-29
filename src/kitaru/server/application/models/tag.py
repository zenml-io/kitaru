"""Tag filters and update command."""

from kitaru.server.base import FrozenModel, ListFilter


class TagFilter(ListFilter):
    """Tag list filter."""

    name: str | None = None


class TagUpdate(FrozenModel):
    """Partial tag update."""

    name: str | None = None
