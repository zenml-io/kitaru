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
"""Control plane login for clients of a control plane backed server."""

import logging
import uuid
import webbrowser
from collections.abc import Callable
from dataclasses import dataclass
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import httpx
from pydantic import BaseModel, ConfigDict

from kitaru.client.config import get_client_id, normalize_server_url
from kitaru.client.credential_store import CredentialStore
from kitaru.client.credentials import ApiToken, ApiType
from kitaru.client.device_grant import describe_this_device, poll_for_token
from kitaru.client.exceptions import raise_for_response
from kitaru.transport import build_async_client

logger = logging.getLogger(__name__)

DEVICE_AUTHORIZATION_PATH = "/auth/device_authorization"
LOGIN_PATH = "/auth/login"
WORKSPACE_PATH = "/workspaces/{workspace_id}"

MANAGED_CLOUD_API_URL = "https://cloudapi.zenml.io"

# Grant the control plane accepts an API key under.
API_KEY_GRANT_TYPE = "zenml_api_key"
DEVICE_CODE_GRANT_TYPE = "urn:ietf:params:oauth:grant-type:device_code"

# Product the verification page is told to render for.
PRODUCT = "kitaru"


class ControlPlaneLoginError(Exception):
    """Raised when a control plane login cannot be started."""


class ControlPlaneDeviceAuthorization(BaseModel):
    """Control plane device authorization response."""

    model_config = ConfigDict(extra="ignore")

    device_code: str
    user_code: str
    verification_uri: str
    verification_uri_complete: str | None = None
    expires_in: int
    interval: int


class ControlPlaneToken(BaseModel):
    """Control plane login response."""

    model_config = ConfigDict(extra="ignore")

    access_token: str
    expires_in: int
    device_metadata: dict[str, str] | None = None


@dataclass(frozen=True, slots=True)
class ControlPlaneDeviceLogin:
    """Token and workspace selection returned by a device login."""

    token: ApiToken
    device_metadata: dict[str, str]


class ControlPlaneKitaruServiceStatus(BaseModel):
    """Deployment details needed to connect to a Kitaru workspace."""

    model_config = ConfigDict(extra="ignore")

    server_url: str | None = None


class ControlPlaneKitaruService(BaseModel):
    """Kitaru service attached to a managed workspace."""

    model_config = ConfigDict(extra="ignore")

    status: ControlPlaneKitaruServiceStatus | None = None


class ControlPlaneWorkspace(BaseModel):
    """Managed workspace fields required by the Kitaru login flow."""

    model_config = ConfigDict(extra="ignore")

    id: uuid.UUID
    name: str
    workspace_type: str
    status: str
    kitaru_service: ControlPlaneKitaruService | None = None

    @property
    def server_url(self) -> str | None:
        """Return the deployed Kitaru server URL, if available."""
        if self.kitaru_service is None or self.kitaru_service.status is None:
            return None
        return self.kitaru_service.status.server_url


class ControlPlaneSession:
    """Control plane tokens for one control plane, refreshed as they expire."""

    def __init__(
        self,
        api_url: str,
        store: CredentialStore,
        timeout: float = 30.0,
        retries: int = 3,
        pool_size: int = 20,
    ) -> None:
        """Initialize the session.

        Args:
            api_url: Control plane API base URL.
            store: Credential store holding the token and the API key that
                renews it.
            timeout: Request timeout in seconds.
            retries: Retry count for failed requests.
            pool_size: Connection pool size.
        """
        self._url = normalize_server_url(api_url)
        self._store = store
        self._http = build_async_client(
            self._url,
            {"Accept": "application/json"},
            timeout=timeout,
            retries=retries,
            pool_size=pool_size,
        )

    async def get_token(self) -> str | None:
        """Return a usable control plane token, renewing an expiring one first.

        Returns:
            Bearer token, or None when nothing stored can produce one.
        """
        token = self._store.get_token(self._url)
        if token is not None:
            return token.access_token
        credentials = self._store.get_control_plane(self._url)
        if credentials is None or credentials.api_key is None:
            return None
        return (await self.login_with_api_key(credentials.api_key)).access_token

    async def login_with_api_key(self, api_key: str) -> ApiToken:
        """Exchange a control plane API key for a control plane token.

        Args:
            api_key: Control plane API key.

        Raises:
            APIError: The request failed, including 401 for an invalid key.

        Returns:
            Token issued by the control plane.
        """
        response = await self._post(
            LOGIN_PATH,
            data={"grant_type": API_KEY_GRANT_TYPE, "password": api_key},
            headers={"Authorization": f"Bearer {api_key}"},
        )
        credentials = self._store.get_control_plane(self._url)
        # Stored only once the control plane has accepted it, so a rejected key
        # never displaces a working one.
        if credentials is None or credentials.api_key != api_key:
            self._store.set_api_key(self._url, api_key, type=ApiType.CONTROL_PLANE)
        return self._store_token(response)

    async def device_login(
        self,
        open_browser: bool = True,
        prompt: Callable[[ControlPlaneDeviceAuthorization], None] | None = None,
        workspace_id: str | None = None,
    ) -> ApiToken:
        """Authorize this machine and return its cached control plane token."""
        result = await self.device_login_with_metadata(
            open_browser=open_browser,
            prompt=prompt,
            workspace_id=workspace_id,
        )
        return result.token

    async def device_login_with_metadata(
        self,
        open_browser: bool = True,
        prompt: Callable[[ControlPlaneDeviceAuthorization], None] | None = None,
        workspace_id: str | None = None,
    ) -> ControlPlaneDeviceLogin:
        """Authorize this machine against the control plane.

        The call blocks until a signed-in account confirms the user code in a
        browser, or until the authorization expires.

        Args:
            open_browser: Whether to open the verification page.
            prompt: Called with the authorization so the caller can show the
                user code. Defaults to logging it.
            workspace_id: Workspace ID preselected on the verification page.

        Raises:
            DeviceLoginError: The authorization expired or was refused.

        Returns:
            Token and workspace selection issued for the authorized device.
        """
        client_id = get_client_id()
        device = describe_this_device()
        response = await self._post(
            DEVICE_AUTHORIZATION_PATH,
            data={"client_id": str(client_id)},
            headers={"User-Agent": f"Host/{device.hostname} OS/{device.os}"},
        )
        authorization = ControlPlaneDeviceAuthorization.model_validate(response.json())
        uri = authorization.verification_uri_complete or authorization.verification_uri
        verification_uri = self._url + uri if uri.startswith("/") else uri
        parsed = urlparse(verification_uri)
        query_params = dict(parse_qsl(parsed.query))
        query_params["product"] = PRODUCT
        if workspace_id is not None:
            query_params["workspace"] = workspace_id
        verification_uri = urlunparse(parsed._replace(query=urlencode(query_params)))
        # Written back so the prompt shows the same URL the browser opens.
        authorization.verification_uri_complete = verification_uri
        if prompt is not None:
            prompt(authorization)
        else:
            logger.info(
                "Open %s and confirm the code %s.",
                verification_uri,
                authorization.user_code,
            )
        if open_browser:
            webbrowser.open(verification_uri)

        async def exchange() -> httpx.Response:
            return await self._post(
                LOGIN_PATH,
                data={
                    "grant_type": DEVICE_CODE_GRANT_TYPE,
                    "client_id": str(client_id),
                    "device_code": authorization.device_code,
                },
            )

        confirmed = await poll_for_token(
            exchange, authorization.expires_in, authorization.interval
        )
        issued = ControlPlaneToken.model_validate(confirmed.json())
        token = self._store_issued_token(issued)
        return ControlPlaneDeviceLogin(
            token=token,
            device_metadata=issued.device_metadata or {},
        )

    async def get_workspace(
        self, workspace_id: uuid.UUID, access_token: str
    ) -> ControlPlaneWorkspace:
        """Get the managed workspace selected during device authorization.

        Args:
            workspace_id: Selected workspace ID.
            access_token: Control plane bearer token.

        Returns:
            Managed workspace connection details.
        """
        response = await self._http.get(
            WORKSPACE_PATH.format(workspace_id=workspace_id),
            headers={"Authorization": f"Bearer {access_token}"},
        )
        raise_for_response(response)
        return ControlPlaneWorkspace.model_validate(response.json())

    async def close(self) -> None:
        """Close the underlying HTTP client."""
        await self._http.aclose()

    def _store_token(self, response: httpx.Response) -> ApiToken:
        """Cache the token a control plane login response carries.

        Args:
            response: Control plane login response.

        Returns:
            Cached token.
        """
        issued = ControlPlaneToken.model_validate(response.json())
        return self._store_issued_token(issued)

    def _store_issued_token(self, issued: ControlPlaneToken) -> ApiToken:
        """Cache a validated control plane token response."""
        token = ApiToken.issued(issued.access_token, issued.expires_in)
        self._store.set_token(self._url, token, type=ApiType.CONTROL_PLANE)
        return token

    async def _post(
        self,
        path: str,
        data: dict[str, str],
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        """Send a form request to the control plane.

        Args:
            path: Request path relative to the control plane API base URL.
            data: Form request body.
            headers: Additional request headers.

        Raises:
            APIError: The response has an error status code.

        Returns:
            HTTP response.
        """
        response = await self._http.post(path, data=data, headers=headers)
        raise_for_response(response)
        return response
