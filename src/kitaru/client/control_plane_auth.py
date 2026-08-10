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

from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.control_plane import (
    ControlPlaneDeviceAuthorization,
    ControlPlaneLoginError,
    ControlPlaneSession,
)
from kitaru.client.credential_store import CredentialStore
from kitaru.client.credentials import ApiToken


async def control_plane_login(
    api_client: KitaruAPIClient,
    base_url: str,
    store: CredentialStore,
    api_key: str | None = None,
    open_browser: bool = True,
    prompt: Callable[[ControlPlaneDeviceAuthorization], None] | None = None,
) -> ApiToken:
    """Log in to a server that delegates identity to a control plane.

    Without an API key the call blocks until a signed-in account confirms the
    user code in a browser, or until the authorization expires.

    Args:
        api_client: API client pointed at the server.
        base_url: Server base URL credentials are stored under.
        store: Credential store the credentials are written to.
        api_key: Control plane API key. Runs the device authorization flow
            when omitted.
        open_browser: Whether to open the verification page.
        prompt: Called with the authorization so the caller can show the user
            code.

    Raises:
        ControlPlaneLoginError: The server does not delegate to a control
            plane.
        DeviceLoginError: The authorization expired or was refused.

    Returns:
        Session token issued by the server.
    """
    info = await api_client.info.get()
    if not info.control_plane_api_url:
        raise ControlPlaneLoginError(
            f"Server {base_url} does not authenticate against a control plane"
        )
    session = ControlPlaneSession(info.control_plane_api_url, store)
    try:
        if api_key is not None:
            credential = await session.login_with_api_key(api_key)
        else:
            credential = await session.device_login(
                open_browser=open_browser,
                prompt=prompt,
                workspace_id=str(info.id) if info.id else None,
            )
    finally:
        await session.close()
    response = await api_client.auth.exchange_control_plane_credential(
        credential.access_token
    )
    token = ApiToken.from_response(response)
    store.set_token(base_url, token, control_plane_api_url=info.control_plane_api_url)
    return token
