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
"""Import outcome recording and evaluator and analyzer fan-out."""

from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.imports import ImportStats
from kitaru.api_models.v1.task import TaskOnFailure, TaskStatus
from kitaru.server.application.events import TaskTerminal
from kitaru.server.application.interfaces.import_repository import ImportRepository
from kitaru.server.application.interfaces.plugin_repository import PluginRepository
from kitaru.server.application.interfaces.session_repository import SessionRepository
from kitaru.server.application.interfaces.task_repository import TaskRepository
from kitaru.server.application.models.session import SessionFilter
from kitaru.server.application.services.plugin_resolution import (
    get_plugin_task_labels,
)
from kitaru.server.domain.imports import ImportNotFound
from kitaru.server.domain.plugin import PluginKind, PluginNotFound
from kitaru.server.domain.session import SessionNotEvaluatable
from kitaru.server.domain.task import AnalysisTask, EvaluationTask, ImportTask, Task
from kitaru.server.filtering import FilterCondition
from kitaru.server.utils import paginate_all


async def record_import_outcome(
    event: TaskTerminal,
    import_repository: ImportRepository,
    session_repository: SessionRepository,
    task_repository: TaskRepository,
    plugin_repository: PluginRepository,
) -> None:
    """Record the import's outcome and append its evaluator and analyzer tasks.

    A no-op when the terminal task is not an import task or its import row
    is gone. A completed task stamps the import's stats from its result, any
    other terminal status stamps the task's error. Evaluator and analysis
    tasks are appended only for a completed import that created at least
    one session and names evaluators or analyzers, skipping sessions still
    in progress: one evaluator task per imported session and evaluator, and
    one analysis task per analyzer scoped to the import. No analysis task
    is appended when no session is evaluatable. Inserts them without
    locking the job row. The
    completing task's own transition settles the job afterward, in the same
    transaction, and its drained scan reads every task including these, so
    the job can never be judged drained before they exist.

    Args:
        event: TaskTerminal event.
        import_repository: Import repository.
        session_repository: Session repository, for the imported sessions.
        task_repository: Task repository.
        plugin_repository: Plugin repository, for the analyzers' connection
            schemas.
    """
    task = event.task
    if not isinstance(task, ImportTask):
        return
    try:
        import_ = await import_repository.get(task.import_id)
    except ImportNotFound:
        return
    stats = None
    if task.status is TaskStatus.COMPLETED:
        stats = ImportStats.model_validate(task.result)
        import_.record_stats(stats)
    else:
        import_.record_error(task.error)
    await import_repository.update(import_)
    if (
        stats is None
        or stats.created == 0
        or (not import_.evaluators and not import_.analyzers)
    ):
        return
    membership = FilterCondition(field="import_id", op=FilterOp.EQ, value=import_.id)
    sessions = await paginate_all(
        lambda cursor: session_repository.query(
            SessionFilter(expression=membership, cursor=cursor, size=1000),
            include_payloads=False,
        )
    )
    evaluatable = False
    fan_out_tasks: list[Task] = []
    for session in sessions:
        try:
            session.check_evaluate()
        except SessionNotEvaluatable:
            continue
        evaluatable = True
        for evaluator in import_.evaluators:
            fan_out_tasks.append(
                EvaluationTask(
                    job_id=task.job_id,
                    plugin_version_id=evaluator.evaluator_version_id,
                    input_session_id=session.id,
                    labels=get_plugin_task_labels(evaluator.evaluator),
                    params=evaluator.params,
                    on_failure=TaskOnFailure.CONTINUE,
                )
            )
    if evaluatable:
        for analyzer in import_.analyzers:
            fan_out_tasks.append(
                AnalysisTask(
                    job_id=task.job_id,
                    plugin_version_id=analyzer.analyzer_version_id,
                    agent_id=import_.agent_id,
                    import_id=import_.id,
                    connection_id=analyzer.connection_id,
                    params=analyzer.params,
                    labels=get_plugin_task_labels(
                        analyzer.analyzer,
                        analyzer.provider,
                        analyzer.connection_id is None
                        and await _has_connection_schema(
                            analyzer.analyzer, plugin_repository
                        ),
                    ),
                    on_failure=TaskOnFailure.CONTINUE,
                )
            )
    if fan_out_tasks:
        await task_repository.create_many(fan_out_tasks)


async def _has_connection_schema(
    name: str, plugin_repository: PluginRepository
) -> bool:
    """Report whether the named analyzer declares a connection schema.

    Args:
        name: Analyzer name.
        plugin_repository: Plugin repository.

    Returns:
        Whether the analyzer declares a connection schema, False once the
        analyzer is deleted.
    """
    try:
        plugin = await plugin_repository.get_by_name(PluginKind.ANALYZER, name)
    except PluginNotFound:
        return False
    return plugin.connection_schema is not None
