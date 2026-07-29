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
"""Control plane API client and the authentication it backs."""

import asyncio
import logging
import random
import uuid
from collections.abc import Mapping
from importlib.metadata import version
from typing import Any, ClassVar

import httpx
from pydantic import BaseModel, ConfigDict, ValidationError

from kitaru.server.api.config import APISettings
from kitaru.server.application.interfaces.account_repository import (
    AccountRepository,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.domain.account import (
    Account,
    AccountNotFound,
    DuplicateAccountName,
)
from kitaru.server.domain.names import InvalidName, validate_account_name

logger = logging.getLogger(__name__)

# Distinguishes control plane API keys from locally issued Kitaru API keys.
CONTROL_PLANE_API_KEY_PREFIX = "ZENPROKEY_"

# Headers the control plane reads to track a server it does not manage.
SERVER_ID_HEADER = "zenml-server-id"
SERVER_VERSION_HEADER = "zenml-server-version"
SERVER_URL_HEADER = "zenml-server-url"


class ControlPlaneError(Exception):
    """Raised when the control plane API cannot authorize a request."""


class ControlPlaneAuthorizationError(ControlPlaneError):
    """Raised when the control plane rejects the supplied credential."""


class ControlPlaneUnavailableError(ControlPlaneError):
    """Raised when the control plane cannot produce an authorization decision."""


class ControlPlaneHTTPError(Exception):
    """Internal HTTP status error used for retry decisions."""

    def __init__(self, status_code: int) -> None:
        """Create an HTTP status error.

        Args:
            status_code: HTTP response status code.
        """
        self.status_code = status_code
        super().__init__(f"Control plane API returned HTTP {status_code}.")


def _get_server_headers(settings: APISettings) -> dict[str, str]:
    """Describe this server to the control plane.

    The control plane records the version and URL it is told here, so it can
    report the server as reachable without polling it.

    Args:
        settings: Runtime settings identifying this server.

    Returns:
        Headers sent on every control plane API request.
    """
    headers = {
        SERVER_ID_HEADER: str(settings.SERVER_ID),
        SERVER_VERSION_HEADER: version("kitaru"),
    }
    if settings.SERVER_URL:
        headers[SERVER_URL_HEADER] = settings.SERVER_URL
    return headers


class ControlPlaneUser(BaseModel):
    """Control plane API user accepted for server access."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    id: uuid.UUID
    username: str | None = None
    email: str | None = None
    is_active: bool = True
    is_service_account: bool = False
    is_superuser: bool = False


class ControlPlaneClient:
    """Call control plane API endpoints needed by a Kitaru server."""

    _RETRY_STATUS_CODES: ClassVar[set[int]] = {408, 429, 502, 503, 504}
    _RETRY_METHODS: ClassVar[set[str]] = {
        "HEAD",
        "GET",
        "POST",
        "PUT",
        "PATCH",
        "DELETE",
        "OPTIONS",
    }

    def __init__(self, settings: APISettings) -> None:
        """Create a control plane API client from server settings.

        Args:
            settings: Runtime settings with the control plane API URL and the
                identity of this server.
        """
        self._settings = settings
        limits = httpx.Limits(
            max_connections=settings.CONTROL_PLANE_CONNECTION_POOL_SIZE,
            max_keepalive_connections=(settings.CONTROL_PLANE_CONNECTION_POOL_SIZE),
        )
        transport = httpx.AsyncHTTPTransport(
            limits=limits,
            retries=settings.CONTROL_PLANE_RETRY_CONNECT,
        )
        self._client = httpx.AsyncClient(
            base_url=settings.CONTROL_PLANE_API_URL.rstrip("/"),
            timeout=settings.CONTROL_PLANE_TIMEOUT_SECONDS,
            transport=transport,
            headers={"Accept": "application/json", **_get_server_headers(settings)},
        )

    async def close(self) -> None:
        """Close pooled control plane API connections."""
        await self._client.aclose()

    async def authorize_user(
        self, credential: str, server_id: uuid.UUID
    ) -> ControlPlaneUser:
        """Validate a caller credential for direct server access.

        Args:
            credential: Bearer token supplied by the caller.
            server_id: Server instance this API represents.

        Raises:
            ControlPlaneError: The response does not describe a user.

        Returns:
            Control plane user the credential identifies.
        """
        params: dict[str, str] = {"server_id": str(server_id)}
        payload = await self._request_json(
            "GET",
            "/users/authorize_server",
            token=credential,
            query=params,
        )
        try:
            return ControlPlaneUser.model_validate(payload)
        except ValidationError as exc:
            raise ControlPlaneError(
                "Control plane API returned no recognizable user."
            ) from exc

    async def authorize_server(
        self, credential: str, server_id: uuid.UUID, action: str
    ) -> ControlPlaneUser:
        """Authorize a caller for one managed workspace request.

        Args:
            credential: Bearer token supplied by the caller.
            server_id: Managed workspace represented by this server.
            action: CRUD action requested by the caller.

        Returns:
            Control plane user accepted for the request.
        """
        payload = await self._request_json(
            "GET",
            "/users/authorize_server",
            token=credential,
            query={"server_id": str(server_id), "action": action},
        )
        return ControlPlaneUser.model_validate(payload)

    async def _request_json(
        self,
        method: str,
        path: str,
        token: str | None = None,
        query: Mapping[str, str | list[str]] | None = None,
        json_body: object | None = None,
        form_body: dict[str, str] | None = None,
    ) -> Any:
        """Run one control plane API request and decode its JSON response.

        Args:
            method: HTTP method.
            path: Absolute API path.
            token: Optional bearer token.
            query: Optional query parameters.
            json_body: Optional JSON request body.
            form_body: Optional form request body.

        Raises:
            ControlPlaneAuthorizationError: The control plane rejects the
                credential.
            ControlPlaneUnavailableError: The request fails, returns an
                unexpected status, or contains invalid JSON.

        Returns:
            Decoded JSON response, or an empty object for empty responses.
        """
        method = method.upper()
        if method not in self._RETRY_METHODS:
            raise ControlPlaneUnavailableError(
                f"Unsupported control plane API method: {method}."
            )
        attempts = max(
            1,
            self._settings.CONTROL_PLANE_RETRY_READ
            + self._settings.CONTROL_PLANE_RETRY_STATUS
            + self._settings.CONTROL_PLANE_RETRY_OTHER
            + 1,
        )
        for index in range(attempts):
            try:
                return await self._send(
                    method,
                    path,
                    token,
                    query,
                    json_body,
                    form_body,
                )
            except ControlPlaneHTTPError as exc:
                if exc.status_code in {401, 403}:
                    raise ControlPlaneAuthorizationError(
                        f"Control plane API returned HTTP {exc.status_code}."
                    ) from exc
                if exc.status_code not in self._RETRY_STATUS_CODES:
                    raise ControlPlaneUnavailableError(
                        f"Control plane API returned HTTP {exc.status_code}."
                    ) from exc
                last_exc: Exception = exc
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last_exc = exc
            if index == attempts - 1:
                raise ControlPlaneUnavailableError(
                    "Control plane API request failed."
                ) from last_exc
            await self._backoff(index)
        raise ControlPlaneUnavailableError("Control plane API request failed.")

    async def _send(
        self,
        method: str,
        path: str,
        token: str | None,
        query: Mapping[str, str | list[str]] | None,
        json_body: object | None,
        form_body: dict[str, str] | None,
    ) -> Any:
        """Send one control plane API request.

        Args:
            method: HTTP method.
            path: Absolute API path.
            token: Optional bearer token.
            query: Optional query parameters.
            json_body: Optional JSON request body.
            form_body: Optional form request body.

        Raises:
            ControlPlaneHTTPError: The control plane returns an error status.
            ControlPlaneUnavailableError: The response contains invalid JSON.

        Returns:
            Decoded response body.
        """
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        if json_body is not None:
            response = await self._client.request(
                method,
                path,
                params=query,
                headers=headers,
                json=json_body,
            )
        elif form_body is not None:
            response = await self._client.request(
                method,
                path,
                params=query,
                headers=headers,
                data=form_body,
            )
        else:
            response = await self._client.request(
                method,
                path,
                params=query,
                headers=headers,
            )
        if response.status_code >= 400:
            raise ControlPlaneHTTPError(response.status_code)
        if not response.content:
            return {}
        try:
            return response.json()
        except ValueError as exc:
            raise ControlPlaneUnavailableError(
                "Control plane API returned invalid JSON."
            ) from exc

    async def _backoff(self, index: int) -> None:
        base = self._settings.CONTROL_PLANE_RETRY_BACKOFF_SECONDS
        await asyncio.sleep(float(base * (2**index) + random.uniform(0, base)))


class ControlPlaneAuthenticator:
    """Resolve control plane credentials into locally mirrored accounts."""

    def __init__(
        self,
        client: ControlPlaneClient,
        account_repository: AccountRepository,
        server_id: uuid.UUID,
    ) -> None:
        """Create a control plane authenticator.

        Args:
            client: Control plane API client.
            account_repository: Account repository holding mirrored accounts.
            server_id: Server instance this API represents.
        """
        self._client = client
        self._account_repository = account_repository
        self._server_id = server_id

    async def authenticate(self, credential: str) -> AuthContext:
        """Authorize a control plane credential and mirror its user.

        Args:
            credential: Control plane session token or API key.

        Raises:
            ControlPlaneError: The credential does not identify a user this
                server accepts.

        Returns:
            Request context for the mirrored account.
        """
        user = await self._client.authorize_user(credential, self._server_id)
        account = await self._mirror_account(user)
        return AuthContext(account=account)

    async def _mirror_account(self, user: ControlPlaneUser) -> Account:
        name = self._get_account_name(user)
        # An account is external from creation onwards. A local account is
        # never claimed by a control plane user, not even on a name match.
        try:
            account = await self._account_repository.get_by_external_id(
                user.id, user.is_service_account
            )
        except AccountNotFound:
            return await self._create_account(user, name)
        account.update_identity(name, user.email)
        account.update_active(True)
        try:
            return await self._account_repository.update(account)
        except DuplicateAccountName as exc:
            raise ControlPlaneError(self._get_name_taken_message(name)) from exc

    async def _create_account(self, user: ControlPlaneUser, name: str) -> Account:
        logger.info("Creating account mirroring control plane user %s.", user.id)
        try:
            return await self._account_repository.create(
                Account(
                    is_service_account=user.is_service_account,
                    external_id=user.id,
                    name=name,
                    email=user.email,
                )
            )
        except DuplicateAccountName as exc:
            raise ControlPlaneError(self._get_name_taken_message(name)) from exc

    @staticmethod
    def _get_name_taken_message(name: str) -> str:
        return f"Account name '{name}' is already registered to another account."

    @staticmethod
    def _get_account_name(user: ControlPlaneUser) -> str:
        if not user.username:
            raise ControlPlaneError(
                f"Control plane user {user.id} has no username to mirror."
            )
        try:
            return validate_account_name(user.username)
        except InvalidName as exc:
            raise ControlPlaneError(
                f"Control plane username '{user.username}' is not a valid account name."
            ) from exc
