#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Batched heartbeat for a worker's in-flight tasks."""

import asyncio
import logging
import uuid

from kitaru.api_models.v1.worker import WorkerHeartbeatRequest
from kitaru.client.api_client import KitaruAPIClient

DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 10.0

logger = logging.getLogger(__name__)


class WorkerHeartbeat:
    """Send batched liveness updates and route cancellation requests."""

    def __init__(
        self,
        client: KitaruAPIClient,
        worker_id: uuid.UUID,
        interval: float = DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    ) -> None:
        """Initialize the heartbeat.

        Args:
            client: API client.
            worker_id: Registered worker id.
            interval: Seconds between heartbeats.
        """
        self._client = client
        self._worker_id = worker_id
        self._interval = interval
        self._canceled: dict[uuid.UUID, asyncio.Event] = {}

    def register(self, task_id: uuid.UUID) -> asyncio.Event:
        """Report a task as in flight until it is unregistered.

        Args:
            task_id: Claimed task id.

        Returns:
            Event set when the server requests task cancellation.
        """
        canceled = asyncio.Event()
        self._canceled[task_id] = canceled
        return canceled

    def unregister(self, task_id: uuid.UUID) -> None:
        """Stop reporting a task as in flight.

        Args:
            task_id: Claimed task id.
        """
        self._canceled.pop(task_id, None)

    async def run(self) -> None:
        """Send one heartbeat per interval until task cancellation."""
        while True:
            await asyncio.sleep(self._interval)
            task_ids = list(self._canceled)
            if not task_ids:
                continue
            try:
                response = await self._client.workers.heartbeat(
                    self._worker_id,
                    WorkerHeartbeatRequest(task_ids=task_ids),
                )
            except Exception:
                logger.exception("Heartbeat for worker %s failed", self._worker_id)
                continue
            for task_id in response.cancel_task_ids:
                if canceled := self._canceled.get(task_id):
                    canceled.set()
