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
"""Kitaru client."""

from types import TracebackType

from kitaru.client.api_client import KitaruAPIClient


class KitaruClient:
    """Kitaru client."""

    def __init__(self, api_client: KitaruAPIClient | None = None) -> None:
        """Initialize the client.

        Args:
            api_client: API client used to send requests.
        """
        self._api_client = api_client or KitaruAPIClient()

    async def close(self) -> None:
        """Close the underlying API client."""
        await self._api_client.close()

    async def __aenter__(self) -> "KitaruClient":
        """Enter the context manager.

        Returns:
            The client.
        """
        return self

    async def __aexit__(
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
        await self.close()
