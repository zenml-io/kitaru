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
"""Auth resource."""

from typing import TYPE_CHECKING

from kitaru.api_models.v1.auth import TokenResponse

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class AuthResource:
    """Auth API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        """Initialize the resource.

        Args:
            client: API client used to send requests.
        """
        self._client = client

    async def login(self, username: str, password: str) -> TokenResponse:
        """Log in with a username and password.

        Args:
            username: Account name.
            password: Login password.

        Raises:
            APIError: The request failed, including 401 for invalid
                credentials.

        Returns:
            Issued token.
        """
        response = await self._client.request(
            "POST",
            "/v1/login",
            data={"username": username, "password": password},
        )
        return TokenResponse.model_validate(response.json())

    async def logout(self) -> None:
        """Log out and clear the auth cookie.

        Raises:
            APIError: The request failed.
        """
        await self._client.request("POST", "/v1/logout")
