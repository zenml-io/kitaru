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
"""Kitaru API client."""

from types import TracebackType
from typing import Any

import httpx

from kitaru.analytics.source import (
    CLIENT_HEADER,
    AnalyticsSource,
    format_client_header,
)
from kitaru.client.exceptions import raise_for_response
from kitaru.client.resources.accounts import AccountsResource
from kitaru.client.resources.api_keys import ApiKeysResource
from kitaru.client.resources.auth import AuthResource
from kitaru.client.resources.secrets import SecretsResource


class KitaruAPIClient:
    """Kitaru API client."""

    def __init__(
        self,
        base_url: str,
        api_key: str | None = None,
        timeout: float = 30.0,
        retries: int = 3,
    ) -> None:
        """Initialize the client.

        Args:
            base_url: Server base URL.
            api_key: API key sent as a bearer token.
            timeout: Request timeout in seconds.
            retries: Connect retry count.
        """
        identification = format_client_header(AnalyticsSource.PYTHON)
        headers = {"User-Agent": identification, CLIENT_HEADER: identification}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        self._http = httpx.AsyncClient(
            base_url=base_url,
            headers=headers,
            timeout=timeout,
            transport=httpx.AsyncHTTPTransport(retries=retries),
        )
        self.accounts = AccountsResource(self)
        self.api_keys = ApiKeysResource(self)
        self.auth = AuthResource(self)
        self.secrets = SecretsResource(self)

    async def request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        json: Any = None,
        data: dict[str, str] | None = None,
    ) -> httpx.Response:
        """Send a request and raise a typed error on failure.

        Args:
            method: HTTP method.
            path: Request path relative to the base URL.
            params: Query parameters.
            json: JSON request body.
            data: Form request body.

        Raises:
            APIError: The response has an error status code.

        Returns:
            HTTP response.
        """
        response = await self._http.request(
            method, path, params=params, json=json, data=data
        )
        raise_for_response(response)
        return response

    async def close(self) -> None:
        """Close the underlying HTTP client."""
        await self._http.aclose()

    async def __aenter__(self) -> "KitaruAPIClient":
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
