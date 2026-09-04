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
from kitaru.server.application.interfaces.replay_repository import ReplayRepository
from kitaru.server.application.interfaces.session_repository import SessionRepository
from kitaru.server.domain.base import NotFoundError
from kitaru.server.domain.evaluation import Evaluation
from kitaru.server.domain.ids import uuid7
from kitaru.server.domain.task import EvaluationTask
from kitaru.server.utils import hash_params


async def record_task_evaluations(
    event: TaskTerminal,
    evaluation_repository: EvaluationRepository,
    job_repository: JobRepository,
    replay_repository: ReplayRepository,
    session_repository: SessionRepository,
) -> None:
    """Write one evaluation row per result of a completed evaluator task.

    Applies uniformly to standalone, result, and baseline evaluator tasks,
    the single writer of task-born evaluation rows. Each row carries the
    task's params and their hash, and links to the replay of the task's job
    when it belongs to one. A no-op when the terminal task is not an
    evaluator task or did not complete.

    Args:
        event: TaskTerminal event.
        evaluation_repository: Evaluation repository.
        job_repository: Job repository, for the owning job's owner id.
        replay_repository: Replay repository, to link the rows to their job's
            replay.
        session_repository: Session repository, to lock the scored session.
    """
    task = event.task
    if not isinstance(task, EvaluationTask) or task.status is not TaskStatus.COMPLETED:
        return
    job = await job_repository.get(task.job_id)
    results = task.result if isinstance(task.result, list) else []
    params_hash = hash_params(task.params)
    # One id per call, shared by every result it produced, so the whole set
    # can be adopted together instead of only the row a ranking query picks.
    invocation_id = uuid7()
    evaluations = [
        Evaluation(
            owner_id=job.owner_id,
            evaluator_version_id=task.plugin_version_id,
            session_id=task.input_session_id,
            task_id=task.id,
            invocation_id=invocation_id,
            name=result.name,
            data_type=result.data_type,
            score=result.score,
            value=result.value,
            explanation=result.explanation,
            passed=result.passed,
            min_score=result.min_score,
            max_score=result.max_score,
            target_score=result.target_score,
            evaluator_params=task.params,
            params_hash=params_hash,
        )
        for result in (EvaluationResult.model_validate(entry) for entry in results)
    ]
    if not evaluations:
        return
    # Lock the scored session so a concurrent delete cannot land between this
    # check and the insert. A deleted session cascade-removes its evaluation
    # rows, so a vanished session leaves nothing to record.
    try:
        await session_repository.get(
            task.input_session_id, include_payloads=False, exclusive=True
        )
    except NotFoundError:
        return
    replay = await replay_repository.get_by_job_id(task.job_id)
    # The evaluator can be deleted while its task runs. The existence check
    # ahead of the insert then finds the version gone, which leaves nothing
    # to record.
    try:
        await evaluation_repository.create_task_evaluations(
            evaluations, replay.id if replay is not None else None
        )
    except NotFoundError:
        return
