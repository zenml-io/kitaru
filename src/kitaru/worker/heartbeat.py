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
"""Batched heartbeat of a worker's in-flight jobs."""

import asyncio
import logging
import uuid

from kitaru.api_models.v1.workers import WorkerHeartbeatRequest
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError

logger = logging.getLogger(__name__)

DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 10.0


class WorkerHeartbeat:
    """Batched heartbeat of a worker's in-flight jobs."""

    def __init__(
        self,
        client: KitaruAPIClient,
        worker_id: uuid.UUID,
        interval: float = DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    ) -> None:
        """Initialize the heartbeat.

        Args:
            client: API client.
            worker_id: Id of the registered worker.
            interval: Seconds between heartbeats.
        """
        self._client = client
        self._worker_id = worker_id
        self._interval = interval
        self._canceled: dict[uuid.UUID, asyncio.Event] = {}

    def register(self, job_id: uuid.UUID) -> asyncio.Event:
        """Report a job as in flight until it is unregistered.

        Args:
            job_id: Id of the job.

        Returns:
            Event set once the server asks the worker to abandon the job.
        """
        canceled = asyncio.Event()
        self._canceled[job_id] = canceled
        return canceled

    def unregister(self, job_id: uuid.UUID) -> None:
        """Stop reporting a job as in flight.

        Args:
            job_id: Id of the job.
        """
        self._canceled.pop(job_id, None)

    async def run(self) -> None:
        """Send one heartbeat per interval until cancellation."""
        while True:
            await asyncio.sleep(self._interval)
            job_ids = list(self._canceled)
            if not job_ids:
                continue
            try:
                response = await self._client.workers.heartbeat(
                    self._worker_id, WorkerHeartbeatRequest(job_ids=job_ids)
                )
            except APIError as exc:
                logger.warning(
                    "Heartbeat for worker %s failed: %s", self._worker_id, exc
                )
                continue
            for job_id in response.abandon:
                canceled = self._canceled.get(job_id)
                if canceled is not None:
                    canceled.set()
