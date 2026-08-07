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
"""Background stale-task and cancel-propagation sweep loop."""

import asyncio
import contextlib
import logging
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from functools import partial

from sqlalchemy.exc import DBAPIError

from kitaru.analytics.client import AnalyticsClient
from kitaru.server.adapters.db.errors import is_lock_not_available
from kitaru.server.adapters.rest.dependencies import (
    get_server_analytics,
    get_task_service,
)
from kitaru.server.api.config import APISettings
from kitaru.server.application.services.task_service import TaskService
from kitaru.server.database.service import DatabaseService

logger = logging.getLogger(__name__)

SweepUnit = Callable[[TaskService], Awaitable[None]]


async def _run_unit(
    database: DatabaseService,
    settings: APISettings,
    analytics: AnalyticsClient,
    unit: SweepUnit,
    label: str,
) -> None:
    """Run one sweep unit in its own transaction and commit it.

    Builds the task service the same way a request does, so a settlement the
    unit applies dispatches through the same event subscribers. A failure
    rolls the unit back and leaves the remaining units to run.

    Args:
        database: Database service the unit opens a session against.
        settings: API settings for this process.
        analytics: Analytics client for this process.
        unit: Work to run against the service.
        label: Name of the unit for the failure log.
    """
    async for session in database.get_async_session():
        try:
            tracker = get_server_analytics(session, analytics)
            await unit(get_task_service(session, database.engine, settings, tracker))
            await session.commit()
        except Exception as exc:
            await session.rollback()
            if isinstance(exc, DBAPIError) and is_lock_not_available(exc):
                logger.debug("Sweep unit %s skipped a contended row.", label)
            else:
                logger.warning("Sweep unit %s failed: %s", label, exc)


async def _read_candidates(
    database: DatabaseService,
    settings: APISettings,
    analytics: AnalyticsClient,
    now: datetime,
) -> tuple[list[uuid.UUID], list[uuid.UUID], list[uuid.UUID]]:
    """Read the sweep candidate ids without locking.

    Args:
        database: Database service the read opens a session against.
        settings: API settings for this process.
        analytics: Analytics client for this process.
        now: Current time.

    Returns:
        Stale task ids, job ids owing a cancel propagation, and drained job
        ids owing a settlement.
    """
    async for session in database.get_async_session():
        try:
            tracker = get_server_analytics(session, analytics)
            service = get_task_service(session, database.engine, settings, tracker)
            task_ids = await service.list_stale_task_ids(now)
            job_ids = await service.list_unpropagated_cancel_job_ids()
            drained_ids = await service.list_drained_unsettled_job_ids(now)
            return task_ids, job_ids, drained_ids
        finally:
            await session.rollback()
    return [], [], []


async def sweep_once(
    database: DatabaseService,
    settings: APISettings,
    analytics: AnalyticsClient,
) -> None:
    """Propagate pending job cancels and rescue stale tasks, one item per transaction.

    A candidate another replica already holds is skipped and picked up on a
    later tick. A failing item logs and leaves the remaining items to run.

    Args:
        database: Database service the sweep opens sessions against.
        settings: API settings for this process.
        analytics: Analytics client for this process.
    """
    now = datetime.now(UTC)
    task_ids, job_ids, drained_ids = await _read_candidates(
        database, settings, analytics, now
    )
    # Propagate first. The rescue chooses between canceling and requeuing by
    # reading the task's own cancel_requested_at, so a stale task of a
    # canceling job whose stamp has not landed yet is requeued instead of
    # canceled, burning a retry attempt and leaving a window for a worker to
    # claim it.
    for job_id in job_ids:
        await _run_unit(
            database,
            settings,
            analytics,
            partial(TaskService.propagate_job_cancel, job_id=job_id),
            f"cancel propagation for job {job_id}",
        )
    for task_id in task_ids:
        await _run_unit(
            database,
            settings,
            analytics,
            partial(TaskService.sweep_stale_task, task_id=task_id, now=now),
            f"stale task {task_id}",
        )
    for drained_id in drained_ids:
        await _run_unit(
            database,
            settings,
            analytics,
            partial(TaskService.sweep_drained_job, job_id=drained_id),
            f"drained job {drained_id}",
        )


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


def _log_sweeper_exit(task: asyncio.Task[None]) -> None:
    """Log a sweep loop that stopped running.

    Args:
        task: Finished sweep task.
    """
    if task.cancelled():
        return
    exception = task.exception()
    if exception is None:
        logger.error("Stale task sweep loop exited without an error.")
    else:
        logger.error("Stale task sweep loop died: %s", exception, exc_info=exception)


def start_task_sweeper(
    database: DatabaseService,
    settings: APISettings,
    analytics: AnalyticsClient,
) -> asyncio.Task[None] | None:
    """Start the background sweep loop unless it is disabled.

    Args:
        database: Database service the sweep opens sessions against.
        settings: API settings for this process.
        analytics: Analytics client for this process.

    Returns:
        Running sweep task, or ``None`` when the interval setting is zero.
    """
    if settings.TASK_SWEEP_INTERVAL_SECONDS <= 0:
        return None
    task = asyncio.create_task(
        _run_sweep_loop(
            database, settings, analytics, settings.TASK_SWEEP_INTERVAL_SECONDS
        )
    )
    task.add_done_callback(_log_sweeper_exit)
    return task


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
