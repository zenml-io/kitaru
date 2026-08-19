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
"""Retrying HTTP transport."""

import asyncio
import random
import uuid
from typing import ClassVar

import httpx

IDEMPOTENCY_KEY_HEADER = "Idempotency-Key"
_IDEMPOTENT_METHODS = {"POST", "PATCH"}


class RetryTransport(httpx.AsyncBaseTransport):
    """Retrying HTTP transport."""

    _RETRY_STATUS_CODES: ClassVar[set[int]] = {408, 429, 502, 503, 504}

    def __init__(
        self,
        transport: httpx.AsyncBaseTransport,
        retries: int = 3,
        backoff: float = 0.5,
    ) -> None:
        """Initialize the transport.

        Args:
            transport: Transport sending the requests.
            retries: Retry count for failed requests.
            backoff: Base backoff delay in seconds.
        """
        self._transport = transport
        self._retries = retries
        self._backoff_seconds = backoff

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        """Send a request, retrying transport errors and retryable statuses.

        Requests with a non-replayable body are sent exactly once. POST and
        PATCH requests carry an idempotency key held stable across attempts.

        Args:
            request: Request to send.

        Returns:
            HTTP response.
        """
        if request.method in _IDEMPOTENT_METHODS:
            request.headers.setdefault(IDEMPOTENCY_KEY_HEADER, str(uuid.uuid4()))
        retries = self._retries if self._is_replayable(request) else 0
        attempt = 0
        while True:
            try:
                response = await self._transport.handle_async_request(request)
            except httpx.TransportError:
                if attempt == retries:
                    raise
            else:
                if (
                    attempt == retries
                    or response.status_code not in self._RETRY_STATUS_CODES
                ):
                    return response
                await response.aclose()
            await self._backoff(attempt)
            attempt += 1

    async def aclose(self) -> None:
        """Close the wrapped transport."""
        await self._transport.aclose()

    @staticmethod
    def _is_replayable(request: httpx.Request) -> bool:
        # A streaming body is consumed by the first attempt, so a retry would
        # resend it empty. In-memory bodies expose their bytes through
        # content, while streaming bodies raise RequestNotRead.
        try:
            _ = request.content
        except httpx.RequestNotRead:
            return False
        return True

    async def _backoff(self, index: int) -> None:
        base = self._backoff_seconds
        await asyncio.sleep(base * (2**index) + random.uniform(0, base))


def build_async_client(
    base_url: str,
    headers: dict[str, str],
    timeout: float,
    retries: int,
    pool_size: int,
) -> httpx.AsyncClient:
    """Build a retrying HTTP client the way this SDK builds them.

    Args:
        base_url: Base URL requests are sent relative to.
        headers: Headers sent on every request.
        timeout: Request timeout in seconds.
        retries: Retry count for failed requests.
        pool_size: Connection pool size.

    Returns:
        HTTP client wrapped in a retrying transport.
    """
    limits = httpx.Limits(
        max_connections=pool_size, max_keepalive_connections=pool_size
    )
    return httpx.AsyncClient(
        base_url=base_url,
        headers=headers,
        timeout=timeout,
        transport=RetryTransport(
            httpx.AsyncHTTPTransport(limits=limits), retries=retries
        ),
    )
