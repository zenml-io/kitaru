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
"""Background loop scaffolding for periodic server work."""

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable

logger = logging.getLogger(__name__)

Tick = Callable[[], Awaitable[None]]


async def _run_loop(tick: Tick, interval_seconds: float, name: str) -> None:
    """Run a tick on a fixed interval, logging and continuing on failure.

    Args:
        tick: Work to run each interval.
        interval_seconds: Delay between ticks.
        name: Loop name for the failure logs.
    """
    while True:
        try:
            await tick()
        except Exception as exc:
            logger.warning("%s tick failed: %s", name, exc)
        await asyncio.sleep(interval_seconds)


def _log_loop_exit(name: str, task: asyncio.Task[None]) -> None:
    """Log a background loop that stopped running.

    Args:
        name: Loop name for the logs.
        task: Finished loop task.
    """
    if task.cancelled():
        return
    exception = task.exception()
    if exception is None:
        logger.error("%s loop exited without an error.", name)
    else:
        logger.error("%s loop died: %s", name, exception, exc_info=exception)


def start_background_loop(
    tick: Tick, interval_seconds: float, name: str
) -> asyncio.Task[None] | None:
    """Start a background loop unless its interval disables it.

    Args:
        tick: Work to run each interval.
        interval_seconds: Delay between ticks, zero or below disables the
            loop.
        name: Loop name for the logs.

    Returns:
        Running loop task, or ``None`` when the interval disables the loop.
    """
    if interval_seconds <= 0:
        return None
    task = asyncio.create_task(_run_loop(tick, interval_seconds, name))
    task.add_done_callback(lambda finished: _log_loop_exit(name, finished))
    return task


async def stop_background_loop(task: asyncio.Task[None] | None) -> None:
    """Cancel a background loop task and wait for it to finish.

    Args:
        task: Running loop task, or ``None`` when the loop was disabled.
    """
    if task is None:
        return
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
