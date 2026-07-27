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
import time
import uuid
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any, ClassVar

import httpx
from pydantic import BaseModel, ConfigDict

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
from kitaru.server.domain.names import InvalidName, validate_name

logger = logging.getLogger(__name__)

# Distinguishes control plane API keys from locally issued Kitaru API keys.
CONTROL_PLANE_API_KEY_PREFIX = "ZENPROKEY_"


class ControlPlaneError(Exception):
    """Raised when the control plane API cannot authorize a request."""


class ControlPlaneUser(BaseModel):
    """Control plane API user accepted for server access."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    id: uuid.UUID
    username: str | None = None
    email: str | None = None
    is_service_account: bool = False
    is_superuser: bool = False


class ServerAuthorization(BaseModel):
    """Control plane API authorization result for one server caller."""

    user: ControlPlaneUser | None = None
    server_id: uuid.UUID
    expires_at: datetime | None = None


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
            settings: Runtime settings with control plane API URL and server
                enrollment credentials.
        """
        self._settings = settings
        self._m2m_token: str | None = None
        self._m2m_expires_at: datetime | None = None
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
            headers={"Accept": "application/json"},
        )

    async def close(self) -> None:
        """Close pooled control plane API connections."""
        await self._client.aclose()

    async def authorize_server(
        self, credential: str, server_id: uuid.UUID
    ) -> ServerAuthorization:
        """Validate a caller credential for direct server access.

        Args:
            credential: Bearer token supplied by the caller.
            server_id: Server instance this API represents.

        Returns:
            Control plane API authorization context for this server.
        """
        params: dict[str, str] = {"server_id": str(server_id)}
        payload = await self._request_json(
            "GET",
            "/users/authorize_server",
            token=credential,
            query=params,
        )
        return ServerAuthorization.model_validate(payload)

    async def _get_m2m_token(self, force: bool = False) -> str:
        """Return a cached server M2M token.

        Args:
            force: When true, fetch a new token even if the cache is fresh.

        Raises:
            ControlPlaneError: Enrollment settings are missing or login fails.

        Returns:
            Bearer token for server service calls.
        """
        if not force and self._m2m_token and self._m2m_expires_at:
            leeway = self._settings.CONTROL_PLANE_TOKEN_REFRESH_LEEWAY_SECONDS
            if time.time() + leeway < self._m2m_expires_at.timestamp():
                return self._m2m_token

        server_id = self._settings.SERVER_ID
        form_body = {
            "grant_type": "client_credentials",
            "client_id": str(server_id),
            "client_secret": self._settings.ENROLLMENT_KEY,
        }
        if self._settings.CONTROL_PLANE_AUDIENCE:
            form_body["audience"] = self._settings.CONTROL_PLANE_AUDIENCE
        payload = await self._request_json(
            "POST",
            "/auth/login",
            form_body=form_body,
        )
        token = payload.get("access_token") if isinstance(payload, dict) else None
        if not isinstance(token, str) or not token:
            raise ControlPlaneError(
                "Control plane API login did not return access_token."
            )
        self._m2m_token = token
        self._m2m_expires_at = self._token_expires_at(payload)
        return token

    async def _request_json(
        self,
        method: str,
        path: str,
        token: str | None = None,
        query: Mapping[str, str | list[str]] | None = None,
        json_body: object | None = None,
        form_body: dict[str, str] | None = None,
        retry_auth: bool = False,
    ) -> Any:
        """Run one control plane API request and decode its JSON response.

        Args:
            method: HTTP method.
            path: Absolute API path.
            token: Optional bearer token.
            query: Optional query parameters.
            json_body: Optional JSON request body.
            form_body: Optional form request body.
            retry_auth: When true, refresh M2M auth once after HTTP 401.

        Raises:
            ControlPlaneError: The request fails after retries or returns
                invalid JSON.

        Returns:
            Decoded JSON response, or an empty object for empty responses.
        """
        method = method.upper()
        if method not in self._RETRY_METHODS:
            raise ControlPlaneError(f"Unsupported control plane API method: {method}.")
        tried_auth_refresh = False
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
                if exc.status_code == 401 and retry_auth and not tried_auth_refresh:
                    token = await self._get_m2m_token(force=True)
                    tried_auth_refresh = True
                    continue
                if exc.status_code not in self._RETRY_STATUS_CODES:
                    raise ControlPlaneError(
                        f"Control plane API returned HTTP {exc.status_code}."
                    ) from exc
                last_exc: Exception = exc
            except (
                httpx.TimeoutException,
                httpx.TransportError,
            ) as exc:
                last_exc = exc
            if index == attempts - 1:
                raise ControlPlaneError(
                    "Control plane API request failed."
                ) from last_exc
            await self._backoff(index)
        raise ControlPlaneError("Control plane API request failed.")

    async def _send(
        self,
        method: str,
        path: str,
        token: str | None,
        query: Mapping[str, str | list[str]] | None,
        json_body: object | None,
        form_body: dict[str, str] | None,
    ) -> Any:
        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"
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
            raise ControlPlaneError("Control plane API returned invalid JSON.") from exc

    def _token_expires_at(self, payload: dict[str, Any]) -> datetime:
        expires_at = payload.get("expires_at")
        if isinstance(expires_at, str):
            return datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
        expires_in = payload.get("expires_in")
        if isinstance(expires_in, int | float):
            return datetime.now(UTC) + timedelta(seconds=float(expires_in))
        return datetime.now(UTC)

    async def _backoff(self, index: int) -> None:
        base = self._settings.CONTROL_PLANE_RETRY_BACKOFF_SECONDS
        await asyncio.sleep(float(base * (2**index) + random.uniform(0, base)))


class ControlPlaneHTTPError(Exception):
    """Internal HTTP status error used for retry decisions."""

    def __init__(self, status_code: int) -> None:
        """Create an HTTP status error.

        Args:
            status_code: HTTP response status code.
        """
        self.status_code = status_code
        super().__init__(f"Control plane API returned HTTP {status_code}.")


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
        authorization = await self._client.authorize_server(credential, self._server_id)
        if authorization.user is None:
            raise ControlPlaneError("Control plane credential identifies no user.")
        account = await self._mirror_account(authorization.user)
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
            return validate_name(user.username)
        except InvalidName as exc:
            raise ControlPlaneError(
                f"Control plane username '{user.username}' is not a valid account name."
            ) from exc
