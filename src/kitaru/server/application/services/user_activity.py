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
"""Track and report when a user last used the server."""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime
from typing import Any

from kitaru.server.application.interfaces.server_settings_repository import (
    ServerSettingsRepository,
)
from kitaru.server.application.interfaces.task_repository import TaskRepository
from kitaru.server.utils import is_stale

logger = logging.getLogger(__name__)

PERSIST_INTERVAL_SECONDS = 30
CLOSE_TIMEOUT_SECONDS = 5
READ_TIMEOUT_SECONDS = 2


class UserActivityRecorder:
    """Keep the latest user activity time in memory and persist it in the background.

    A persist starts at most once per interval and never blocks the caller, so
    the request that triggered it neither waits on nor fails with the write.
    """

    def __init__(
        self,
        persist: Callable[[datetime], Awaitable[None]],
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        """Initialize the recorder.

        Args:
            persist: Stores an activity time, never moving a stored one backward.
            clock: Source of the current time.
        """
        self._persist = persist
        self._clock = clock
        self._latest: datetime | None = None
        self._last_persist_started: datetime | None = None
        self._persisted: datetime | None = None
        self._pending: asyncio.Task[None] | None = None

    @property
    def latest(self) -> datetime | None:
        """Time of the latest activity this process recorded."""
        return self._latest

    def record(self) -> None:
        """Note user activity now and start a persist when one is due."""
        now = self._clock()
        self._latest = now
        if self._pending is not None or not is_stale(
            self._last_persist_started, PERSIST_INTERVAL_SECONDS, now
        ):
            return
        self._last_persist_started = now
        self._pending = asyncio.create_task(self._persist_quietly(now))
        self._pending.add_done_callback(self._clear_pending)

    async def aclose(self) -> None:
        """Wait for a running persist, then store activity not stored yet.

        Work still running after the close timeout is canceled and awaited.
        """
        try:
            async with asyncio.timeout(CLOSE_TIMEOUT_SECONDS):
                await self.drain()
                if self._latest is not None and self._latest != self._persisted:
                    await self._persist_quietly(self._latest)
        except TimeoutError:
            logger.warning("Timed out persisting user activity at shutdown.")

    async def drain(self) -> None:
        """Wait for the running persist to finish."""
        if self._pending is not None:
            await self._pending

    def _clear_pending(self, task: asyncio.Task[None]) -> None:
        if self._pending is task:
            self._pending = None

    async def _persist_quietly(self, at: datetime) -> None:
        try:
            await self._persist(at)
        except Exception:
            logger.warning("Failed to persist user activity.", exc_info=True)
        else:
            self._persisted = at


class UserActivityService:
    """Report when a user last used the server."""

    def __init__(
        self,
        open_repositories: Callable[
            [],
            AbstractAsyncContextManager[
                tuple[ServerSettingsRepository, TaskRepository]
            ],
        ],
        recorder: UserActivityRecorder | None,
    ) -> None:
        """Initialize the service.

        Args:
            open_repositories: Opens the repositories on a session of their own
                and closes the session on exit.
            recorder: Recorder of this process, absent before startup created it.
        """
        self._open_repositories = open_repositories
        self._recorder = recorder

    async def get_last_user_activity(self) -> datetime | None:
        """Report the time of the last user activity.

        A task in flight counts as activity happening now. Reading, including
        opening and closing the session, is bounded by a timeout.

        Returns:
            The current time while a task is in flight, otherwise the latest of
            the stored and in-memory times. ``None`` when neither exists or the
            read failed or timed out.
        """
        # The read runs as its own task so the caller stops waiting at the
        # timeout even when canceling it stalls on a dead connection. Report
        # no activity instead of a stale time on a failed read, because a
        # caller treats an unknown time as "do not idle".
        read = asyncio.ensure_future(self._read_stored_activity())
        read.add_done_callback(_consume_result)
        try:
            in_flight, stored = await asyncio.wait_for(
                asyncio.shield(read), READ_TIMEOUT_SECONDS
            )
        except Exception:
            logger.warning("Failed to read user activity.", exc_info=True)
            return None
        finally:
            if not read.done():
                read.cancel()
        if in_flight:
            return datetime.now(UTC)
        remembered = self._recorder.latest if self._recorder is not None else None
        known = [time for time in (stored, remembered) if time is not None]
        return max(known) if known else None

    async def _read_stored_activity(self) -> tuple[bool, datetime | None]:
        async with self._open_repositories() as (settings_repository, task_repository):
            if await task_repository.has_in_flight():
                return True, None
            return False, await settings_repository.get_last_user_activity()


def _consume_result(task: asyncio.Task[Any]) -> None:
    """Mark a finished task's outcome as retrieved so asyncio does not warn."""
    if not task.cancelled():
        task.exception()
