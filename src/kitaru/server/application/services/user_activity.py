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
from datetime import UTC, datetime

from kitaru.server.application.interfaces.server_settings_repository import (
    ServerSettingsRepository,
)
from kitaru.server.application.interfaces.task_repository import TaskRepository
from kitaru.server.utils import is_stale

logger = logging.getLogger(__name__)

PERSIST_INTERVAL_SECONDS = 30


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
        self._pending: set[asyncio.Task[None]] = set()

    @property
    def latest(self) -> datetime | None:
        """Time of the latest activity this process recorded."""
        return self._latest

    def record(self) -> None:
        """Note user activity now and start a persist when one is due."""
        now = self._clock()
        self._latest = now
        if self._pending or not is_stale(
            self._last_persist_started, PERSIST_INTERVAL_SECONDS, now
        ):
            return
        self._last_persist_started = now
        task = asyncio.create_task(self._persist_quietly(now))
        self._pending.add(task)
        task.add_done_callback(self._pending.discard)

    async def aclose(self) -> None:
        """Wait for a running persist, then store activity not stored yet."""
        await self.drain()
        if self._latest is not None and self._latest != self._persisted:
            await self._persist_quietly(self._latest)

    async def drain(self) -> None:
        """Wait for every running persist to finish."""
        if self._pending:
            await asyncio.gather(*self._pending)

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
        settings_repository: ServerSettingsRepository,
        task_repository: TaskRepository,
        recorder: UserActivityRecorder | None,
    ) -> None:
        """Initialize the service.

        Args:
            settings_repository: Server settings repository.
            task_repository: Task repository.
            recorder: Recorder of this process, absent before startup created it.
        """
        self._settings_repository = settings_repository
        self._task_repository = task_repository
        self._recorder = recorder

    async def get_last_user_activity(self) -> datetime | None:
        """Report the time of the last user activity.

        A task in flight counts as activity happening now. A failed read
        reports no activity rather than a stale time, because callers treat
        an unknown time as "do not idle".

        Returns:
            The current time while a task is in flight, otherwise the latest of
            the stored and in-memory times, or ``None`` when neither exists.
        """
        try:
            if await self._task_repository.has_in_flight():
                return datetime.now(UTC)
            stored = await self._settings_repository.get_last_user_activity()
        except Exception:
            logger.warning("Failed to read user activity.", exc_info=True)
            return None
        remembered = self._recorder.latest if self._recorder is not None else None
        known = [time for time in (stored, remembered) if time is not None]
        return max(known) if known else None
