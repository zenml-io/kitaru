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
"""Insight row writer, the single subscriber turning task results into rows."""

from kitaru.api_models.v1.insight import InsightInput
from kitaru.api_models.v1.task import TaskStatus
from kitaru.server.application.events import TaskTerminal
from kitaru.server.application.interfaces.agent_repository import AgentRepository
from kitaru.server.application.interfaces.insight_repository import InsightRepository
from kitaru.server.application.interfaces.job_repository import JobRepository
from kitaru.server.domain.base import NotFoundError
from kitaru.server.domain.insight import Insight
from kitaru.server.domain.task import AnalysisTask
from kitaru.server.utils import hash_params


async def record_task_insights(
    event: TaskTerminal,
    insight_repository: InsightRepository,
    job_repository: JobRepository,
    agent_repository: AgentRepository,
) -> None:
    """Write one insight row per result of a completed analysis task.

    The single writer of task-born insight rows. Each row carries the task's
    params and their hash. A no-op when the terminal task is not an analysis
    task or did not complete.

    Args:
        event: TaskTerminal event.
        insight_repository: Insight repository.
        job_repository: Job repository, for the owning job's owner id.
        agent_repository: Agent repository, to check the task's agent still
            exists.
    """
    task = event.task
    if not isinstance(task, AnalysisTask) or task.status is not TaskStatus.COMPLETED:
        return
    job = await job_repository.get(task.job_id)
    results = task.result if isinstance(task.result, list) else []
    params_hash = hash_params(task.params)
    insights = [
        Insight(
            owner_id=job.owner_id,
            agent_id=task.agent_id,
            analyzer_version_id=task.plugin_version_id,
            task_id=task.id,
            name=result.name,
            title=result.title,
            description=result.description,
            data=result.data,
            metadata=result.metadata,
            analyzer_params=task.params,
            params_hash=params_hash,
        )
        for result in (InsightInput.model_validate(entry) for entry in results)
    ]
    if not insights:
        return
    # The task's agent can be deleted while it runs. A vanished agent leaves
    # nothing to record.
    try:
        await agent_repository.get(task.agent_id)
    except NotFoundError:
        return
    # The analyzer can be deleted while its task runs. The existence check
    # ahead of the insert then finds the version gone, which leaves nothing
    # to record.
    try:
        await insight_repository.create_many(insights)
    except NotFoundError:
        return
