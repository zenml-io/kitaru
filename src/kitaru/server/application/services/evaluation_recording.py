#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Evaluation row writer, the single subscriber turning task results into rows."""

from kitaru.api_models.v1.evaluation import EvaluationResult
from kitaru.api_models.v1.task import TaskStatus
from kitaru.server.application.events import TaskTerminal
from kitaru.server.application.interfaces.evaluation_repository import (
    EvaluationRepository,
)
from kitaru.server.application.interfaces.job_repository import JobRepository
from kitaru.server.application.interfaces.session_repository import SessionRepository
from kitaru.server.domain.base import NotFoundError
from kitaru.server.domain.evaluation import Evaluation
from kitaru.server.domain.task import EvaluationTask


async def record_task_evaluations(
    event: TaskTerminal,
    evaluation_repository: EvaluationRepository,
    job_repository: JobRepository,
    session_repository: SessionRepository,
) -> None:
    """Write one evaluation row per result of a completed evaluator task.

    Applies uniformly to standalone, result, and baseline evaluator tasks,
    the single writer of task-born evaluation rows. A no-op when the
    terminal task is not an evaluator task or did not complete.

    Args:
        event: TaskTerminal event.
        evaluation_repository: Evaluation repository.
        job_repository: Job repository, for the owning job's owner id.
        session_repository: Session repository, to lock the scored session.
    """
    task = event.task
    if not isinstance(task, EvaluationTask) or task.status is not TaskStatus.COMPLETED:
        return
    job = await job_repository.get(task.job_id)
    results = task.result if isinstance(task.result, list) else []
    evaluations = [
        Evaluation(
            owner_id=job.owner_id,
            evaluator_version_id=task.plugin_version_id,
            session_id=task.input_session_id,
            task_id=task.id,
            name=result.name,
            data_type=result.data_type,
            score=result.score,
            value=result.value,
            explanation=result.explanation,
            passed=result.passed,
        )
        for result in (EvaluationResult.model_validate(entry) for entry in results)
    ]
    if not evaluations:
        return
    # Lock the scored session so a concurrent delete cannot land between this
    # check and the insert. A deleted session cascade-removes its evaluation
    # rows, so a vanished session leaves nothing to record.
    try:
        await session_repository.get(task.input_session_id, exclusive=True)
    except NotFoundError:
        return
    # The evaluator can be deleted while its task runs, cascading its version
    # away. The insert then references a missing evaluator version, which
    # leaves nothing to record.
    try:
        await evaluation_repository.create_task_evaluations(evaluations)
    except NotFoundError:
        return
