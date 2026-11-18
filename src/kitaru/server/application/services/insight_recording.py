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
from kitaru.server.application.interfaces.account_repository import AccountRepository
from kitaru.server.application.interfaces.insight_repository import InsightRepository
from kitaru.server.application.interfaces.job_repository import JobRepository
from kitaru.server.application.services.insight_analytics import (
    track_first_insight_generated,
)
from kitaru.server.application.services.server_analytics import ServerAnalytics
from kitaru.server.domain.account import AccountNotFound
from kitaru.server.domain.base import NotFoundError
from kitaru.server.domain.ids import uuid7
from kitaru.server.domain.insight import Insight
from kitaru.server.domain.task import AnalysisTask
from kitaru.server.utils import hash_params


async def record_task_insights(
    event: TaskTerminal,
    insight_repository: InsightRepository,
    job_repository: JobRepository,
    account_repository: AccountRepository | None = None,
    analytics: ServerAnalytics | None = None,
) -> None:
    """Write one insight row per result of a completed analysis task.

    The single writer of task-born insight rows. Each row carries the task's
    params and their hash. A no-op when the terminal task is not an analysis
    task or did not complete.

    Args:
        event: TaskTerminal event.
        insight_repository: Insight repository.
        job_repository: Job repository, for the owning job's owner id.
        account_repository: Accounts used to resolve the owner's analytics identity.
        analytics: Analytics tracker, None skips tracking.
    """
    task = event.task
    if not isinstance(task, AnalysisTask) or task.status is not TaskStatus.COMPLETED:
        return
    job = await job_repository.get(task.job_id)
    results = task.result if isinstance(task.result, list) else []
    params_hash = hash_params(task.params)
    invocation_id = uuid7()
    insights = [
        Insight(
            owner_id=job.owner_id,
            agent_id=task.agent_id,
            analyzer_version_id=task.plugin_version_id,
            task_id=task.id,
            import_id=task.import_id,
            invocation_id=invocation_id,
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
    # Related resources can disappear while the task runs. A foreign-key
    # violation then leaves nothing to record.
    try:
        insights = await insight_repository.create_many(insights)
    except NotFoundError:
        return
    if analytics is not None and account_repository is not None:
        try:
            owner = await account_repository.get(job.owner_id)
        except AccountNotFound:
            return
        await track_first_insight_generated(
            insight_repository, insights, owner, analytics
        )
