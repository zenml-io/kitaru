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
"""Synchronous Kitaru client."""

import asyncio
import threading
from collections.abc import Coroutine
from types import TracebackType
from typing import Any, TypeVar

from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.client import KitaruClient

T = TypeVar("T")


class KitaruSyncClient:
    """Synchronous Kitaru client."""

    def __init__(self, api_client: KitaruAPIClient | None = None) -> None:
        """Initialize the client.

        Args:
            api_client: API client used to send requests.
        """
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, daemon=True)
        self._thread.start()
        self._client = KitaruClient(api_client=api_client)

    def _run(self, coro: Coroutine[Any, Any, T]) -> T:
        """Run a coroutine on the event loop thread and wait for the result.

        Args:
            coro: Coroutine to run.

        Returns:
            Coroutine result.
        """
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result()

    def close(self) -> None:
        """Close the underlying client and stop the event loop thread."""
        self._run(self._client.close())
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join()
        self._loop.close()

    def __enter__(self) -> "KitaruSyncClient":
        """Enter the context manager.

        Returns:
            The client.
        """
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Exit the context manager and close the client.

        Args:
            exc_type: Exception type.
            exc: Exception instance.
            traceback: Exception traceback.
        """
        self.close()
