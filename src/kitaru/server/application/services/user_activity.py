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
"""Report when a user last used the server."""

import asyncio
import logging
from datetime import UTC, datetime

from kitaru.server.application.interfaces.server_settings_repository import (
    ServerSettingsRepository,
)
from kitaru.server.application.interfaces.task_repository import TaskRepository

logger = logging.getLogger(__name__)

READ_TIMEOUT_SECONDS = 2


class UserActivityService:
    """Report when a user last used the server."""

    def __init__(
        self,
        settings_repository: ServerSettingsRepository,
        task_repository: TaskRepository,
        in_memory_time: datetime | None,
    ) -> None:
        """Initialize the service.

        Args:
            settings_repository: Repository holding the stored activity time.
            task_repository: Repository used to look for tasks in flight.
            in_memory_time: Latest activity time this process holds in memory.
        """
        self._settings_repository = settings_repository
        self._task_repository = task_repository
        self._in_memory_time = in_memory_time

    async def get_last_user_activity(self) -> datetime | None:
        """Report the time of the last user activity.

        A task in flight counts as activity happening now.

        Returns:
            The current time while a task is in flight, otherwise the latest of
            the stored and in-memory times. ``None`` when neither exists or the
            read failed or timed out.
        """
        # Report no activity instead of a stale time on a failed read, because
        # a caller treats an unknown time as "do not idle".
        try:
            async with asyncio.timeout(READ_TIMEOUT_SECONDS):
                if await self._task_repository.has_in_flight():
                    return datetime.now(UTC)
                stored = await self._settings_repository.get_last_user_activity()
        except Exception:
            logger.warning("Failed to read user activity.", exc_info=True)
            return None
        known = [time for time in (stored, self._in_memory_time) if time is not None]
        return max(known) if known else None
