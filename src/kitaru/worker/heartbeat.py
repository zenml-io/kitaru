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
"""Batched task heartbeating with server-driven cancellation."""

import asyncio
import logging
import uuid

import httpx

from kitaru.api_models.v1.worker import WorkerHeartbeatRequest
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError
from kitaru.worker.inflight import InflightTasks

logger = logging.getLogger(__name__)

DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 10.0


class WorkerHeartbeat:
    """Batches in-flight task ids into one heartbeat request per interval."""

    def __init__(
        self,
        client: KitaruAPIClient,
        worker_id: uuid.UUID,
        inflight: InflightTasks,
        interval: float = DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    ) -> None:
        """Initialize the heartbeat.

        Args:
            client: API client used to send heartbeats.
            worker_id: Id of the heartbeating worker.
            inflight: Registry of tasks held by the worker.
            interval: Seconds between heartbeat requests.
        """
        self._client = client
        self._worker_id = worker_id
        self._inflight = inflight
        self._interval = interval

    async def run(self) -> None:
        """Send batched heartbeats on the interval until canceled."""
        while True:
            await asyncio.sleep(self._interval)
            task_ids = self._inflight.get_ids()
            if not task_ids:
                continue
            try:
                response = await self._client.workers.heartbeat(
                    self._worker_id, WorkerHeartbeatRequest(task_ids=task_ids)
                )
            except (APIError, httpx.TransportError) as exc:
                logger.warning("Heartbeat failed: %s", exc)
                continue
            logger.debug("Heartbeat sent for %d task(s).", len(task_ids))
            for task_id in response.cancel_task_ids:
                logger.info("Server requested cancellation of task %s.", task_id)
                self._inflight.cancel(task_id)
