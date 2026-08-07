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
import contextlib
import logging

from kitaru.analytics.client import AnalyticsClient
from kitaru.server.adapters.rest.dependencies import (
    get_server_analytics,
    get_task_service,
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

    Builds the task service the same way a request does, so a settlement
    dispatches through the same event subscribers. A failing batch rolls
    back, logs, and ends the drain until the next tick.

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
                service = get_task_service(session, database.engine, settings, tracker)
                advanced = await service.settle_queued_jobs()
                await session.commit()
            except Exception as exc:
                await session.rollback()
                logger.warning("Job settlement batch failed: %s", exc)
                return
        if not advanced:
            return


async def _run_settle_loop(
    database: DatabaseService,
    settings: APISettings,
    analytics: AnalyticsClient,
    interval_seconds: float,
) -> None:
    """Run settle_once on a fixed interval, logging and continuing on failure.

    Args:
        database: Database service the batches open sessions against.
        settings: API settings for this process.
        analytics: Analytics client for this process.
        interval_seconds: Delay between drains.
    """
    while True:
        try:
            await settle_once(database, settings, analytics)
        except Exception as exc:
            logger.warning("Job settlement tick failed: %s", exc)
        await asyncio.sleep(interval_seconds)


def _log_settler_exit(task: asyncio.Task[None]) -> None:
    """Log a settlement loop that stopped running.

    Args:
        task: Finished settlement task.
    """
    if task.cancelled():
        return
    exception = task.exception()
    if exception is None:
        logger.error("Job settlement loop exited without an error.")
    else:
        logger.error("Job settlement loop died: %s", exception, exc_info=exception)


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
    if settings.JOB_SETTLEMENT_INTERVAL_SECONDS <= 0:
        return None
    task = asyncio.create_task(
        _run_settle_loop(
            database,
            settings,
            analytics,
            settings.JOB_SETTLEMENT_INTERVAL_SECONDS,
        )
    )
    task.add_done_callback(_log_settler_exit)
    return task


async def stop_job_settler(task: asyncio.Task[None] | None) -> None:
    """Cancel the background settlement task and wait for it to finish.

    Args:
        task: Running settlement task, or ``None`` when the settler was
            disabled.
    """
    if task is None:
        return
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
