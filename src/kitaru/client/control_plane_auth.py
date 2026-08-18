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
"""Control plane login flow."""

from collections.abc import Callable
from typing import Literal

from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.control_plane import (
    ControlPlaneDeviceAuthorization,
    ControlPlaneLoginError,
    ControlPlaneSession,
)
from kitaru.client.credential_store import CredentialStore
from kitaru.client.credentials import ApiToken
from kitaru.client.exceptions import AuthenticationError

ControlPlaneLoginMethod = Literal["api_key", "stored", "device"]


async def control_plane_login(
    api_client: KitaruAPIClient,
    base_url: str,
    store: CredentialStore,
    api_key: str | None = None,
    open_browser: bool = True,
    prompt: Callable[[ControlPlaneDeviceAuthorization], None] | None = None,
    refresh: bool = False,
) -> tuple[ApiToken, ControlPlaneLoginMethod]:
    """Log in to a server that delegates identity to a control plane.

    Without an API key, a stored control plane credential that still produces
    an accepted token is reused. Otherwise the call blocks until a signed-in
    account confirms the user code in a browser, or until the authorization
    expires.

    Args:
        api_client: API client pointed at the server.
        base_url: Server base URL credentials are stored under.
        store: Credential store the credentials are written to.
        api_key: Control plane API key. Reuses a stored credential or runs
            the device authorization flow when omitted.
        open_browser: Whether to open the verification page.
        prompt: Called with the authorization so the caller can show the user
            code.
        refresh: Whether to skip stored credentials and force a new device
            authorization flow.

    Raises:
        ControlPlaneLoginError: The server does not delegate to a control
            plane.
        DeviceLoginError: The authorization expired or was refused.

    Returns:
        Session token issued by the server and the method that produced it.
    """
    info = await api_client.info.get()
    if not info.control_plane_api_url:
        raise ControlPlaneLoginError(
            f"Server {base_url} does not authenticate against a control plane"
        )
    control_plane_api_url = info.control_plane_api_url
    session = ControlPlaneSession(control_plane_api_url, store)
    try:
        if api_key is not None:
            credential = (await session.login_with_api_key(api_key)).access_token
            token = await _exchange_credential(
                api_client, base_url, store, control_plane_api_url, credential
            )
            return token, "api_key"
        if not refresh:
            # A stored credential the control plane or the server no longer
            # accepts falls through to the device flow.
            try:
                credential = await session.get_token()
            except AuthenticationError:
                credential = None
            if credential is not None:
                try:
                    token = await _exchange_credential(
                        api_client, base_url, store, control_plane_api_url, credential
                    )
                    return token, "stored"
                except AuthenticationError:
                    pass
        credential = (
            await session.device_login(
                open_browser=open_browser,
                prompt=prompt,
                workspace_id=str(info.id) if info.id else None,
            )
        ).access_token
        token = await _exchange_credential(
            api_client, base_url, store, control_plane_api_url, credential
        )
        return token, "device"
    finally:
        await session.close()


async def _exchange_credential(
    api_client: KitaruAPIClient,
    base_url: str,
    store: CredentialStore,
    control_plane_api_url: str,
    credential: str,
) -> ApiToken:
    """Exchange a control plane credential for a stored server token.

    Args:
        api_client: API client pointed at the server.
        base_url: Server base URL the token is stored under.
        store: Credential store the token is written to.
        control_plane_api_url: Control plane that issued the credential.
        credential: Control plane bearer token.

    Returns:
        Session token issued by the server.
    """
    response = await api_client.auth.exchange_control_plane_credential(credential)
    token = ApiToken.from_response(response)
    store.set_token(base_url, token, control_plane_api_url=control_plane_api_url)
    return token
