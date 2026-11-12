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
"""Record when a user last used the server."""

import asyncio
import logging
from datetime import UTC, datetime

from kitaru.server.adapters.db.repositories.server_settings_repository import (
    SQLServerSettingsRepository,
)
from kitaru.server.database.service import DatabaseService
from kitaru.server.utils import is_stale

logger = logging.getLogger(__name__)

PERSIST_INTERVAL_SECONDS = 30
CLOSE_TIMEOUT_SECONDS = 5


class UserActivityRecorder:
    """Keep the latest user activity time in memory and persist it in the background.

    A persist starts at most once per interval, and a failed one is logged.
    """

    def __init__(self, database: DatabaseService) -> None:
        """Initialize the recorder.

        Args:
            database: Database service each persist opens a session against.
        """
        self._database = database
        self._latest: datetime | None = None
        self._last_persist_started: datetime | None = None
        self._pending: asyncio.Task[None] | None = None

    @property
    def latest(self) -> datetime | None:
        """Time of the latest activity this process recorded."""
        return self._latest

    def record(self) -> None:
        """Note user activity now and start a persist when one is due."""
        now = datetime.now(UTC)
        self._latest = now
        if self._pending is not None or not is_stale(
            self._last_persist_started, PERSIST_INTERVAL_SECONDS, now
        ):
            return
        self._last_persist_started = now
        self._pending = asyncio.create_task(self._persist(now))
        self._pending.add_done_callback(self._clear_pending)

    async def aclose(self) -> None:
        """Wait for a running persist, then store the latest activity time.

        Work still running after the close timeout is canceled and awaited.
        """
        try:
            async with asyncio.timeout(CLOSE_TIMEOUT_SECONDS):
                await self.drain()
                if self._latest is not None:
                    await self._persist(self._latest)
        except TimeoutError:
            logger.warning("Timed out persisting user activity at shutdown.")

    async def drain(self) -> None:
        """Wait for the running persist to finish."""
        if self._pending is not None:
            await self._pending

    def _clear_pending(self, _task: asyncio.Task[None]) -> None:
        self._pending = None

    async def _persist(self, at: datetime) -> None:
        try:
            async for session in self._database.get_async_session():
                await SQLServerSettingsRepository(session).record_user_activity(at)
                await session.commit()
        except Exception:
            logger.warning("Failed to persist user activity.", exc_info=True)
