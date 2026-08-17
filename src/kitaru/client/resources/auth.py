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
"""Auth SDK resource."""

import uuid
from typing import TYPE_CHECKING

from kitaru.api_models.v1.auth import (
    DeviceAuthorizationResponse,
    GrantType,
    TokenResponse,
)

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
            "/api/v1/login",
            data={
                "grant_type": GrantType.PASSWORD.value,
                "username": username,
                "password": password,
            },
            authenticate=False,
        )
        return TokenResponse.model_validate(response.json())

    async def device_authorization(
        self,
        hostname: str | None = None,
        os: str | None = None,
        python_version: str | None = None,
        client_version: str | None = None,
    ) -> DeviceAuthorizationResponse:
        """Start a device authorization.

        Args:
            hostname: Host this client runs on.
            os: Operating system this client runs on.
            python_version: Python version this client runs.
            client_version: Kitaru version this client runs.

        Raises:
            APIError: The request failed, including 400 when this server does
                not authenticate requests.

        Returns:
            Device authorization carrying the plaintext codes.
        """
        data = {
            "hostname": hostname,
            "os": os,
            "python_version": python_version,
            "client_version": client_version,
        }
        response = await self._client.request(
            "POST",
            "/api/v1/device_authorization",
            data={key: value for key, value in data.items() if value is not None},
            authenticate=False,
        )
        return DeviceAuthorizationResponse.model_validate(response.json())

    async def exchange_device_code(
        self, device_id: uuid.UUID, device_code: str
    ) -> TokenResponse:
        """Exchange a device code for a session token.

        Args:
            device_id: Id of the device.
            device_code: Device code issued with the authorization.

        Raises:
            TokenGrantError: The authorization is not confirmed yet, which
                carries the ``authorization_pending`` error code, or it can no
                longer be confirmed.
            APIError: The request failed for another reason.

        Returns:
            Issued token.
        """
        response = await self._client.request(
            "POST",
            "/api/v1/login",
            data={
                "grant_type": GrantType.DEVICE_CODE.value,
                "device_id": str(device_id),
                "device_code": device_code,
            },
            authenticate=False,
        )
        return TokenResponse.model_validate(response.json())

    async def exchange_api_key(self, api_key: str) -> TokenResponse:
        """Exchange an API key for a session token.

        Args:
            api_key: API key.

        Raises:
            APIError: The request failed, including 400 when this server does
                not run the local auth scheme and 401 when the key is not
                valid.

        Returns:
            Issued token.
        """
        return await self._exchange_credential(GrantType.API_KEY, api_key)

    async def exchange_control_plane_credential(self, credential: str) -> TokenResponse:
        """Exchange a control plane credential for a session token.

        Args:
            credential: Control plane session token or API key.

        Raises:
            APIError: The request failed, including 400 when this server does
                not run the control plane auth scheme and 401 when the control
                plane refuses the credential.

        Returns:
            Issued token.
        """
        return await self._exchange_credential(GrantType.CONTROL_PLANE, credential)

    async def _exchange_credential(
        self, grant_type: GrantType, credential: str
    ) -> TokenResponse:
        """Run a grant that presents its credential in the authorization header.

        Args:
            grant_type: Grant type to run.
            credential: Credential to present.

        Raises:
            APIError: The request failed.

        Returns:
            Issued token.
        """
        response = await self._client.request(
            "POST",
            "/api/v1/login",
            data={"grant_type": grant_type.value},
            headers={"Authorization": f"Bearer {credential}"},
            authenticate=False,
        )
        return TokenResponse.model_validate(response.json())

    async def logout(self) -> None:
        """Log out and clear the auth cookie.

        Raises:
            APIError: The request failed.
        """
        await self._client.request("POST", "/api/v1/logout", authenticate=False)
