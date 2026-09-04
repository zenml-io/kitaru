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
"""Import outcome recording and evaluator fan-out."""

from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.imports import ImportStats
from kitaru.api_models.v1.task import TaskOnFailure, TaskStatus
from kitaru.server.application.events import TaskTerminal
from kitaru.server.application.interfaces.import_repository import ImportRepository
from kitaru.server.application.interfaces.session_repository import SessionRepository
from kitaru.server.application.interfaces.task_repository import TaskRepository
from kitaru.server.application.models.session import SessionFilter
from kitaru.server.domain.imports import ImportNotFound
from kitaru.server.domain.session import SessionNotEvaluatable
from kitaru.server.domain.task import EvaluationTask, ImportTask, Task
from kitaru.server.filtering import FilterCondition
from kitaru.server.utils import paginate_all


async def record_import_outcome(
    event: TaskTerminal,
    import_repository: ImportRepository,
    session_repository: SessionRepository,
    task_repository: TaskRepository,
) -> None:
    """Record the import's outcome and append its evaluator tasks when its task ends.

    A no-op when the terminal task is not an import task or its import row
    is gone. A completed task stamps the import's stats from its result, any
    other terminal status stamps the task's error. Evaluator tasks are
    appended only for a completed import naming evaluators, one per imported
    session and evaluator, skipping sessions still in progress. Inserts them
    without locking the job row. The completing task's own transition settles
    the job afterward, in the same transaction, and its drained scan reads
    every task including these, so the job can never be judged drained before
    they exist.

    Args:
        event: TaskTerminal event.
        import_repository: Import repository.
        session_repository: Session repository, for the imported sessions.
        task_repository: Task repository.
    """
    task = event.task
    if not isinstance(task, ImportTask):
        return
    try:
        import_ = await import_repository.get(task.import_id)
    except ImportNotFound:
        return
    if task.status is TaskStatus.COMPLETED:
        import_.record_stats(ImportStats.model_validate(task.result))
    else:
        import_.record_error(task.error)
    await import_repository.update(import_)
    if task.status is not TaskStatus.COMPLETED or not import_.evaluators:
        return
    membership = FilterCondition(field="import_id", op=FilterOp.EQ, value=import_.id)
    sessions = await paginate_all(
        lambda cursor: session_repository.query(
            SessionFilter(expression=membership, cursor=cursor, size=1000),
            include_payloads=False,
        )
    )
    evaluator_tasks: list[Task] = []
    for session in sessions:
        try:
            session.check_evaluate()
        except SessionNotEvaluatable:
            continue
        for evaluator in import_.evaluators:
            evaluator_tasks.append(
                EvaluationTask(
                    job_id=task.job_id,
                    plugin_version_id=evaluator.evaluator_version_id,
                    input_session_id=session.id,
                    params=evaluator.params,
                    on_failure=TaskOnFailure.CONTINUE,
                )
            )
    if evaluator_tasks:
        await task_repository.create_many(evaluator_tasks)
