"""Evaluation DTO conversions."""

from kitaru.api_models.v1.evaluation import (
    EvaluationListParams,
    EvaluationResponse,
)
from kitaru.server.application.models.evaluation import EvaluationFilter
from kitaru.server.domain.evaluation import Evaluation
from kitaru.server.domain.plugin import PluginVersion


def evaluation_to_response(
    evaluation: Evaluation,
    evaluator_name: str | None = None,
    evaluator_version: PluginVersion | None = None,
) -> EvaluationResponse:
    """Convert an evaluation and optional denormalized evaluator data."""
    assert evaluation.created is not None
    assert evaluation.updated is not None
    return EvaluationResponse(
        id=evaluation.id,
        owner_id=evaluation.owner_id,
        evaluator_version_id=evaluation.evaluator_version_id,
        evaluator_name=evaluator_name,
        evaluator_version=(
            evaluator_version.version if evaluator_version is not None else None
        ),
        session_id=evaluation.session_id,
        task_id=evaluation.task_id,
        name=evaluation.name,
        data_type=evaluation.data_type,
        score=evaluation.score,
        value=evaluation.value,
        explanation=evaluation.explanation,
        created=evaluation.created,
        updated=evaluation.updated,
    )


def evaluation_list_params_to_filter(
    params: EvaluationListParams,
) -> EvaluationFilter:
    """Convert evaluation list query parameters."""
    return EvaluationFilter(**params.model_dump(mode="python"))
