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
"""Device authorization login flow."""

import logging
import webbrowser
from collections.abc import Callable

from kitaru.api_models.v1.auth import (
    DeviceAuthorizationResponse,
    TokenResponse,
)
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.credential_store import CredentialStore
from kitaru.client.credentials import ApiToken
from kitaru.client.device_grant import describe_this_device, poll_for_token

logger = logging.getLogger(__name__)


async def device_login(
    api_client: KitaruAPIClient,
    base_url: str,
    store: CredentialStore,
    open_browser: bool = True,
    prompt: Callable[[DeviceAuthorizationResponse], None] | None = None,
) -> ApiToken:
    """Authorize this machine against a server and store the token it gets.

    The call blocks until a signed-in account confirms the user code in a
    browser, or until the authorization expires.

    Args:
        api_client: API client pointed at the server.
        base_url: Server base URL credentials are stored under.
        store: Credential store the device authorization is written to.
        open_browser: Whether to open the verification page.
        prompt: Called with the authorization so the caller can show the user
            code. Defaults to logging it.

    Raises:
        DeviceLoginError: The authorization expired or was refused.

    Returns:
        Token issued for the authorized device.
    """
    fingerprint = describe_this_device()
    authorization = await api_client.auth.device_authorization(
        hostname=fingerprint.hostname,
        os=fingerprint.os,
        python_version=fingerprint.python_version,
        client_version=fingerprint.client_version,
    )
    if prompt is not None:
        prompt(authorization)
    else:
        logger.info(
            "Open %s and confirm the code %s.",
            authorization.verification_uri_complete,
            authorization.user_code,
        )
    if open_browser:
        webbrowser.open(authorization.verification_uri_complete)
    response = await _poll_for_token(api_client, authorization)
    token = ApiToken.from_response(response)
    store.set_device(base_url, authorization.device_id, authorization.device_code)
    store.set_token(base_url, token)
    return token


async def _poll_for_token(
    api_client: KitaruAPIClient, authorization: DeviceAuthorizationResponse
) -> TokenResponse:
    """Poll the token endpoint until the authorization is confirmed.

    Args:
        api_client: API client pointed at the server.
        authorization: Device authorization being confirmed.

    Raises:
        DeviceLoginError: The authorization expired or was refused.

    Returns:
        Token response issued for the device.
    """

    async def exchange() -> TokenResponse:
        return await api_client.auth.exchange_device_code(
            authorization.device_id, authorization.device_code
        )

    return await poll_for_token(
        exchange, authorization.expires_in, authorization.interval
    )
