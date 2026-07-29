"""Cohort repository interface."""

import uuid
from typing import Protocol

from kitaru.server.application.models.cohort import CohortFilter
from kitaru.server.domain.cohort import Cohort


class CohortRepository(Protocol):
    """Cohort persistence operations."""

    async def create(self, cohort: Cohort, session_ids: list[uuid.UUID]) -> Cohort: ...
    async def get(self, cohort_id: uuid.UUID) -> Cohort: ...
    async def get_session_ids(self, cohort_id: uuid.UUID) -> list[uuid.UUID]: ...
    async def query(
        self, cohort_filter: CohortFilter
    ) -> tuple[list[Cohort], str | None]: ...
    async def update(self, cohort: Cohort) -> Cohort: ...
    async def delete(self, cohort_id: uuid.UUID) -> None: ...
