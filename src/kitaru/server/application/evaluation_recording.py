"""Record evaluator task results as evaluation rows."""

from typing import Any

from kitaru.server.application.events import TaskTerminal
from kitaru.server.application.interfaces.evaluation_repository import (
    EvaluationRepository,
)
from kitaru.server.application.interfaces.session_repository import SessionRepository
from kitaru.server.domain.evaluation import Evaluation, EvaluationDataType
from kitaru.server.domain.task import EvaluationTask, TaskStatus


async def record_task_evaluations(
    event: TaskTerminal,
    evaluation_repository: EvaluationRepository,
    session_repository: SessionRepository,
) -> None:
    """Persist every result emitted by a completed evaluator task."""
    task = event.task
    if not isinstance(task, EvaluationTask) or task.status is not TaskStatus.COMPLETED:
        return
    session = await session_repository.get(task.input_session_id)
    results = task.result
    assert isinstance(results, list)
    evaluations = [_to_evaluation(result, task, session.owner_id) for result in results]
    await evaluation_repository.create_many(evaluations)


def _to_evaluation(result: Any, task: EvaluationTask, owner_id: Any) -> Evaluation:
    get = result.get if isinstance(result, dict) else lambda key: getattr(result, key)
    score = get("score")
    value = get("value")
    if score is not None and value is not None:
        data_type = EvaluationDataType.CATEGORICAL
    elif isinstance(score, bool):
        data_type = EvaluationDataType.BOOL
    elif score is not None:
        data_type = EvaluationDataType.FLOAT
    else:
        data_type = EvaluationDataType.STR
    return Evaluation(
        owner_id=owner_id,
        evaluator_version_id=task.plugin_version_id,
        session_id=task.input_session_id,
        task_id=task.id,
        name=get("name"),
        data_type=data_type,
        score=score,
        value=value,
        explanation=get("explanation"),
    )
