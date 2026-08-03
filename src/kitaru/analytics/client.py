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
"""Async analytics client."""

import asyncio
import logging
from typing import Any, Literal
from uuid import UUID

import httpx
from pydantic import BaseModel

from kitaru.analytics.events import AnalyticsEvent
from kitaru.analytics.source import AnalyticsSource, current_source

logger = logging.getLogger(__name__)

ANALYTICS_SERVER_URL = "https://analytics.zenml.io"
SOURCE_CONTEXT_HEADER = "Source-Context"


class TrackMessage(BaseModel):
    """Track message."""

    user_id: UUID
    event: str
    properties: dict[str, Any]
    type: Literal["track"] = "track"
    debug: bool


class IdentifyMessage(BaseModel):
    """Identify message."""

    user_id: UUID
    traits: dict[str, Any]
    type: Literal["identify"] = "identify"
    debug: bool


AnalyticsMessage = TrackMessage | IdentifyMessage


class AnalyticsClient:
    """Async analytics client."""

    def __init__(
        self,
        enabled: bool = True,
        debug: bool = False,
        url: str = ANALYTICS_SERVER_URL,
        timeout: float = 15.0,
        max_queue_size: int = 1000,
        max_batch_size: int = 100,
    ) -> None:
        """Initialize the client.

        Args:
            enabled: Whether messages are sent.
            debug: Whether messages are marked as debug messages.
            url: Analytics server base URL.
            timeout: Request timeout in seconds.
            max_queue_size: Queued messages before new ones are dropped.
            max_batch_size: Messages sent in a single request.
        """
        self._enabled = enabled
        self._debug = debug
        self._max_batch_size = max_batch_size
        self._queue: asyncio.Queue[tuple[AnalyticsSource, AnalyticsMessage] | None] = (
            asyncio.Queue(maxsize=max_queue_size)
        )
        self._http = httpx.AsyncClient(
            base_url=url,
            headers={"Accept": "application/json"},
            timeout=timeout,
        )
        self._worker: asyncio.Task[None] | None = None
        self._closed = False

    @property
    def enabled(self) -> bool:
        """Whether messages are sent.

        Returns:
            Whether messages are sent.
        """
        return self._enabled

    def track(
        self,
        user_id: UUID,
        event: AnalyticsEvent | str,
        properties: dict[str, Any] | None = None,
    ) -> None:
        """Queue a track message.

        Args:
            user_id: User ID.
            event: Event name.
            properties: Event properties.
        """
        self._enqueue(
            TrackMessage(
                user_id=user_id,
                event=event,
                properties=properties or {},
                debug=self._debug,
            )
        )

    def identify(self, user_id: UUID, traits: dict[str, Any] | None = None) -> None:
        """Queue an identify message.

        Args:
            user_id: User ID.
            traits: User traits.
        """
        self._enqueue(
            IdentifyMessage(user_id=user_id, traits=traits or {}, debug=self._debug)
        )

    async def aclose(self) -> None:
        """Flush queued messages and close the client."""
        if self._closed:
            return
        self._closed = True
        if self._worker is not None:
            await self._queue.put(None)
            await self._worker
        await self._http.aclose()

    def _enqueue(self, message: AnalyticsMessage) -> None:
        """Queue a message together with the current source.

        Args:
            message: Message to queue.
        """
        if not self._enabled or self._closed:
            return
        if self._worker is None:
            self._worker = asyncio.get_running_loop().create_task(
                self._deliver_messages()
            )
        try:
            self._queue.put_nowait((current_source.get(), message))
        except asyncio.QueueFull:
            logger.debug("Analytics queue is full, dropping message")

    async def _deliver_messages(self) -> None:
        """Deliver queued messages in batches until the stop sentinel arrives."""
        while True:
            item = await self._queue.get()
            if item is None:
                return
            batch = [item]
            while len(batch) < self._max_batch_size:
                try:
                    item = self._queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                if item is None:
                    await self._post_batch(batch)
                    return
                batch.append(item)
            await self._post_batch(batch)

    async def _post_batch(
        self, batch: list[tuple[AnalyticsSource, AnalyticsMessage]]
    ) -> None:
        """Post a batch of messages, one request per source.

        Args:
            batch: Messages with the source they were queued under.
        """
        groups: dict[AnalyticsSource, list[AnalyticsMessage]] = {}
        for source, message in batch:
            groups.setdefault(source, []).append(message)
        for source, messages in groups.items():
            try:
                response = await self._http.post(
                    "/batch",
                    json=[message.model_dump(mode="json") for message in messages],
                    headers={SOURCE_CONTEXT_HEADER: source.value},
                )
                response.raise_for_status()
            except Exception as exc:
                logger.debug("Failed to send analytics batch: %s", exc)
