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
"""Background stale-task sweep loop."""

import asyncio
import contextlib
import logging
from datetime import UTC, datetime

from kitaru.analytics.client import AnalyticsClient
from kitaru.server.adapters.rest.dependencies import (
    get_server_analytics,
    get_task_service,
)
from kitaru.server.api.config import APISettings
from kitaru.server.database.service import DatabaseService

logger = logging.getLogger(__name__)


async def sweep_once(
    database: DatabaseService, settings: APISettings, analytics: AnalyticsClient
) -> None:
    """Sweep stale tasks once, in one session, and commit the result.

    Builds the task service the same way a request does, so a settlement
    the sweep applies dispatches through the same event subscribers.

    Args:
        database: Database service the sweep opens a session against.
        settings: API settings for this process.
        analytics: Analytics client for this process.
    """
    async for session in database.get_async_session():
        try:
            tracker = get_server_analytics(session, settings, analytics)
            service = get_task_service(session, settings, tracker)
            await service.sweep_stale_tasks(datetime.now(UTC))
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def _run_sweep_loop(
    database: DatabaseService,
    settings: APISettings,
    analytics: AnalyticsClient,
    interval_seconds: int,
) -> None:
    """Run sweep_once on a fixed interval, logging and continuing on failure.

    Args:
        database: Database service the sweep opens sessions against.
        settings: API settings for this process.
        analytics: Analytics client for this process.
        interval_seconds: Delay between sweeps.
    """
    while True:
        try:
            await sweep_once(database, settings, analytics)
        except Exception as exc:
            logger.warning("Stale task sweep tick failed: %s", exc)
        await asyncio.sleep(interval_seconds)


def start_task_sweeper(
    database: DatabaseService, settings: APISettings, analytics: AnalyticsClient
) -> asyncio.Task[None] | None:
    """Start the background stale-task sweep loop unless it is disabled.

    Args:
        database: Database service the sweep opens sessions against.
        settings: API settings for this process.
        analytics: Analytics client for this process.

    Returns:
        Running sweep task, or ``None`` when the interval setting is zero.
    """
    if settings.TASK_SWEEP_INTERVAL_SECONDS <= 0:
        return None
    return asyncio.create_task(
        _run_sweep_loop(
            database, settings, analytics, settings.TASK_SWEEP_INTERVAL_SECONDS
        )
    )


async def stop_task_sweeper(task: asyncio.Task[None] | None) -> None:
    """Cancel the background sweep task and wait for it to finish.

    Args:
        task: Running sweep task, or ``None`` when the sweeper was disabled.
    """
    if task is None:
        return
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
