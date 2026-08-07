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
"""Background job settlement loop."""

import asyncio
import logging
from functools import partial

from kitaru.analytics.client import AnalyticsClient
from kitaru.server.adapters.rest.dependencies import (
    get_server_analytics,
    get_task_transitions,
)
from kitaru.server.api.background_loop import (
    start_background_loop,
    stop_background_loop,
)
from kitaru.server.api.config import APISettings
from kitaru.server.database.service import DatabaseService

logger = logging.getLogger(__name__)


async def settle_once(
    database: DatabaseService,
    settings: APISettings,
    analytics: AnalyticsClient,
) -> None:
    """Drain the settlement check queue, one claimed batch per transaction.

    Builds the transition dispatch the same way a request does, so a
    settlement dispatches through the same event subscribers. A failing
    batch rolls back, logs, and ends the drain until the next tick.

    Args:
        database: Database service the batches open sessions against.
        settings: API settings for this process.
        analytics: Analytics client for this process.
    """
    while True:
        advanced = 0
        async for session in database.get_async_session():
            try:
                tracker = get_server_analytics(session, analytics)
                transitions = get_task_transitions(session, tracker)
                advanced = await transitions.settle_queued_jobs(
                    settings.JOB_SETTLEMENT_BATCH_LIMIT
                )
                await session.commit()
            except Exception as exc:
                await session.rollback()
                logger.warning("Job settlement batch failed: %s", exc)
                return
        if not advanced:
            return


def start_job_settler(
    database: DatabaseService,
    settings: APISettings,
    analytics: AnalyticsClient,
) -> asyncio.Task[None] | None:
    """Start the background settlement loop unless it is disabled.

    Args:
        database: Database service the batches open sessions against.
        settings: API settings for this process.
        analytics: Analytics client for this process.

    Returns:
        Running settlement task, or ``None`` when the interval setting is
        zero.
    """
    return start_background_loop(
        partial(settle_once, database, settings, analytics),
        settings.JOB_SETTLEMENT_INTERVAL_SECONDS,
        "Job settlement",
    )


async def stop_job_settler(task: asyncio.Task[None] | None) -> None:
    """Cancel the background settlement task and wait for it to finish.

    Args:
        task: Running settlement task, or ``None`` when the settler was
            disabled.
    """
    await stop_background_loop(task)
