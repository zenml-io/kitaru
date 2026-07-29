"""Evaluation filters and batch command."""

import uuid

from kitaru.server.base import FrozenModel, ListFilter
from kitaru.server.domain.evaluation import EvaluationDataType
from kitaru.server.domain.replay_config import EvaluatorConfig


class EvaluationFilter(ListFilter):
    """Evaluation list filter."""

    session_id: uuid.UUID | None = None
    task_id: uuid.UUID | None = None
    evaluator_version_id: uuid.UUID | None = None
    name: str | None = None
    data_type: EvaluationDataType | None = None


class EvaluationBatchCreate(FrozenModel):
    """Standalone evaluation batch command."""

    input_session_ids: list[uuid.UUID]
    evaluators: list[EvaluatorConfig]
