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
"""Worker bearer token renewal through re-registration."""

from kitaru.api_models.v1.worker import WorkerCreateRequest
from kitaru.client.api_client import KitaruAPIClient


class WorkerTokenSource:
    """Worker token renewed by re-registering through an API key client."""

    def __init__(
        self, client: KitaruAPIClient, request: WorkerCreateRequest, token: str
    ) -> None:
        """Initialize the source.

        Args:
            client: Client authenticating with the account API key, used to
                re-register the worker.
            request: Worker registration request sent on every registration.
            token: Worker token issued by the initial registration.
        """
        self._client = client
        self._request = request
        self._token = token

    def get_cached_token(self) -> str | None:
        """Return the worker token issued by the last registration.

        Returns:
            Worker token.
        """
        return self._token

    async def fetch_token(self) -> str | None:
        """Re-register the worker and return the freshly issued token.

        Registering is an upsert by worker name, so re-registering only
        refreshes the token rather than creating a second worker.

        Raises:
            APIError: The registration request failed.

        Returns:
            Worker token issued by the fresh registration.
        """
        response = await self._client.workers.create(self._request)
        self._token = response.token.get_secret_value()
        return self._token

    async def close(self) -> None:
        """Close no resources, since the registration client has its own owner."""
