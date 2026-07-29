"""Evaluation repository interface."""

import uuid
from typing import Protocol

from kitaru.server.application.models.evaluation import EvaluationFilter
from kitaru.server.domain.evaluation import Evaluation


class EvaluationRepository(Protocol):
    """Evaluation persistence operations."""

    async def create_many(self, evaluations: list[Evaluation]) -> list[Evaluation]: ...
    async def upsert_manual(
        self, evaluations: list[Evaluation]
    ) -> list[Evaluation]: ...
    async def get(self, evaluation_id: uuid.UUID) -> Evaluation: ...
    async def query(
        self, evaluation_filter: EvaluationFilter
    ) -> tuple[list[Evaluation], str | None]: ...
    async def delete_for_task(self, task_id: uuid.UUID) -> None: ...
