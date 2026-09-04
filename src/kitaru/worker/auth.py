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
"""Worker bearer token renewal."""

import uuid

from kitaru.client.api_client import KitaruAPIClient


class WorkerTokenSource:
    """Worker token renewed through an API key client."""

    def __init__(
        self, client: KitaruAPIClient, worker_id: uuid.UUID, token: str | None = None
    ) -> None:
        """Initialize the source.

        Args:
            client: Client authenticating with the account API key, used to
                renew the worker token.
            worker_id: Id of the registered worker.
            token: Worker token issued by the registration, renewed on first
                use when omitted.
        """
        self._client = client
        self._worker_id = worker_id
        self._token = token

    def get_cached_token(self) -> str | None:
        """Return the worker token issued last.

        Returns:
            Worker token, or None before the first renewal.
        """
        return self._token

    async def fetch_token(self) -> str | None:
        """Renew the worker token and return it.

        Raises:
            APIError: The renewal request failed.

        Returns:
            Freshly issued worker token.
        """
        response = await self._client.workers.renew_token(self._worker_id)
        self._token = response.token.get_secret_value()
        return self._token

    async def close(self) -> None:
        """Close no resources, since the API key client has its own owner."""
