"""Task filters and executor update."""

import uuid
from datetime import datetime
from typing import Any

from kitaru.server.base import FrozenModel, ListFilter
from kitaru.server.domain.task import TaskKind, TaskStatus


class TaskFilter(ListFilter):
    """Task list and internal sweep filter."""

    job_id: uuid.UUID | None = None
    kind: TaskKind | None = None
    status: TaskStatus | None = None
    worker_id: uuid.UUID | None = None
    stale_before: datetime | None = None


class TaskUpdate(FrozenModel):
    """Attempt-fenced executor update."""

    status: TaskStatus | None = None
    attempt: int | None = None
    error: str | None = None
    result: Any = None
