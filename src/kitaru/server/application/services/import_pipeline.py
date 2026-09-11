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

import uuid
from datetime import UTC, datetime

from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.imports import ImportStats
from kitaru.api_models.v1.task import TaskOnFailure, TaskStatus
from kitaru.server.application.events import TaskTerminal
from kitaru.server.application.interfaces.import_repository import ImportRepository
from kitaru.server.application.interfaces.plugin_repository import PluginRepository
from kitaru.server.application.interfaces.session_repository import SessionRepository
from kitaru.server.application.interfaces.task_repository import TaskRepository
from kitaru.server.application.models.session import SessionFilter
from kitaru.server.application.services.evaluator_resolution import (
    get_evaluator_task_labels,
)
from kitaru.server.application.services.plugin_resolution import (
    get_plugin_task_labels,
    has_connection_schema,
)
from kitaru.server.domain.imports import Import, ImportNotFound
from kitaru.server.domain.plugin import PluginKind
from kitaru.server.domain.replay_config import AnalyzerConfig
from kitaru.server.domain.session import Session, SessionNotEvaluatable
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
    tasks are appended only for a completed import that names evaluators or
    analyzers, skipping sessions still in progress: one evaluator task per
    imported session and evaluator, and
    one analysis task per analyzer scoped to the import. Analyzers below
    their minimum session count are recorded as skipped without execution.
    Inserts them without locking the job row. The completing task's own
    transition settles the job afterward, in the same
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
    if stats is None or (not import_.evaluators and not import_.analyzers):
        return
    # A retry can report zero new sessions after an earlier attempt stored them.
    sessions = await query_evaluatable_sessions(import_.id, session_repository)
    labels = {
        evaluator.evaluator_version_id: await get_evaluator_task_labels(
            evaluator, plugin_repository
        )
        for evaluator in import_.evaluators
    }
    fan_out_tasks: list[Task] = []
    for session in sessions:
        for evaluator in import_.evaluators:
            fan_out_tasks.append(
                EvaluationTask(
                    job_id=task.job_id,
                    plugin_version_id=evaluator.evaluator_version_id,
                    input_session_id=session.id,
                    connection_id=evaluator.connection_id,
                    labels=labels[evaluator.evaluator_version_id],
                    params=evaluator.params,
                    on_failure=TaskOnFailure.CONTINUE,
                )
            )
    for analyzer in import_.analyzers:
        fan_out_tasks.append(
            await build_analysis_task(
                analyzer, import_, task.job_id, plugin_repository, len(sessions)
            )
        )
    if fan_out_tasks:
        await task_repository.create_many(fan_out_tasks)


async def query_evaluatable_sessions(
    import_id: uuid.UUID, session_repository: SessionRepository
) -> list[Session]:
    """Load the sessions of an import that accept evaluations.

    Args:
        import_id: Id of the import.
        session_repository: Session repository.

    Returns:
        Sessions the import created that are not in progress.
    """
    membership = FilterCondition(field="import_id", op=FilterOp.EQ, value=import_id)
    sessions = await paginate_all(
        lambda cursor: session_repository.query(
            SessionFilter(expression=membership, cursor=cursor, size=1000),
            include_payloads=False,
        )
    )
    evaluatable: list[Session] = []
    for session in sessions:
        try:
            session.check_evaluate()
        except SessionNotEvaluatable:
            continue
        evaluatable.append(session)
    return evaluatable


async def build_analysis_task(
    analyzer: AnalyzerConfig,
    import_: Import,
    job_id: uuid.UUID,
    plugin_repository: PluginRepository,
    eligible_session_count: int,
) -> AnalysisTask:
    """Build the task running an analyzer over an import's sessions.

    Args:
        analyzer: Resolved analyzer config.
        import_: Import the analyzer reads.
        job_id: Job the task belongs to.
        plugin_repository: Plugin repository, for the analyzer's connection
            schema.
        eligible_session_count: Number of eligible sessions in this import.

    Returns:
        Analysis task, not yet stored.
    """
    task = AnalysisTask(
        job_id=job_id,
        plugin_version_id=analyzer.analyzer_version_id,
        agent_id=import_.agent_id,
        import_id=import_.id,
        connection_id=analyzer.connection_id,
        params=analyzer.params,
        labels=get_plugin_task_labels(
            analyzer.analyzer,
            analyzer.provider,
            analyzer.connection_id is None
            and await has_connection_schema(
                PluginKind.ANALYZER, analyzer.analyzer, plugin_repository
            ),
        ),
        on_failure=TaskOnFailure.CONTINUE,
    )
    task.skip_if_insufficient_sessions(
        eligible_session_count, analyzer.get_min_sessions(), datetime.now(UTC)
    )
    return task
