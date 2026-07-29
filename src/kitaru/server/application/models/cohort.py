"""Cohort filters and commands."""

import uuid

from kitaru.server.base import FrozenModel, ListFilter


class CohortFilter(ListFilter):
    """Cohort list filter."""

    name: str | None = None
    tag: str | None = None


class CohortSessionsFilter(ListFilter):
    """Ordered cohort session filter."""

    cohort_id: uuid.UUID


class CohortCreate(FrozenModel):
    """Cohort creation command."""

    name: str
    description: str | None = None
    agent_id: uuid.UUID
    session_ids: list[uuid.UUID]


class CohortUpdate(FrozenModel):
    """Partial cohort update."""

    name: str | None = None
    description: str | None = None
