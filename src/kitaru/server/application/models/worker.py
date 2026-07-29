"""Worker list filter."""

from datetime import datetime

from kitaru.server.base import ListFilter


class WorkerFilter(ListFilter):
    """Worker list and internal liveness filter."""

    name: str | None = None
    seen_after: datetime | None = None
