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
"""Bearer token acquisition and refresh for the API client."""

import asyncio
import uuid
from collections.abc import AsyncGenerator
from typing import Protocol

import httpx

from kitaru.api_models.v1.auth import TokenResponse
from kitaru.client.control_plane import ControlPlaneSession
from kitaru.client.credential_store import CredentialStore, normalize_server_url
from kitaru.client.credentials import ApiToken, ServerCredentials


class TokenExchange(Protocol):
    """Login endpoint calls that turn a stored credential into a token."""

    async def exchange_device_code(
        self, device_id: uuid.UUID, device_code: str
    ) -> TokenResponse:
        """Exchange a device code for a session token.

        Args:
            device_id: Id of the device.
            device_code: Device code issued with the authorization.

        Returns:
            Issued token.
        """
        ...

    async def exchange_control_plane_credential(self, credential: str) -> TokenResponse:
        """Exchange a control plane credential for a session token.

        Args:
            credential: Control plane session token or API key.

        Returns:
            Issued token.
        """
        ...


class TokenSource(Protocol):
    """Source of bearer tokens for a renewing auth flow."""

    def get_cached_token(self) -> str | None:
        """Return the cached bearer token.

        Returns:
            Bearer token, or None when nothing usable is cached.
        """
        ...

    async def fetch_token(self) -> str | None:
        """Fetch a fresh bearer token.

        Returns:
            Bearer token, or None when the source cannot produce one.
        """
        ...

    async def close(self) -> None:
        """Close any resources this source opened."""
        ...


class TokenAuth(httpx.Auth):
    """Bearer token auth flow."""

    async def close(self) -> None:
        """Close any resources this auth flow opened."""


class StaticTokenAuth(TokenAuth):
    """Fixed bearer token that never renews."""

    def __init__(self, token: str) -> None:
        """Initialize the auth flow.

        Args:
            token: Bearer token attached to every request.
        """
        self._header = f"Bearer {token}"

    async def async_auth_flow(
        self, request: httpx.Request
    ) -> AsyncGenerator[httpx.Request, httpx.Response]:
        """Attach the fixed bearer token to the request.

        Args:
            request: Outgoing HTTP request.

        Yields:
            The request with the bearer token attached.
        """
        request.headers["Authorization"] = self._header
        yield request


class RenewingTokenAuth(TokenAuth):
    """Bearer token auth flow renewing its token through a source on HTTP 401."""

    def __init__(self, source: TokenSource) -> None:
        """Initialize the auth flow.

        Args:
            source: Source producing and renewing the bearer token.
        """
        self._source = source
        self._lock = asyncio.Lock()
        self._generation = 0

    async def async_auth_flow(
        self, request: httpx.Request
    ) -> AsyncGenerator[httpx.Request, httpx.Response]:
        """Attach a bearer token and retry once with a renewed one on HTTP 401.

        A request rejected with HTTP 401 is retried once with a renewed token,
        unless another caller renewed it first, in which case the retry uses
        theirs.

        Args:
            request: Outgoing HTTP request.

        Yields:
            The request with a bearer token attached, and once more after a
            renewal following an HTTP 401 response.
        """
        generation = self._generation
        token = await self._get_token()
        if token is not None:
            request.headers["Authorization"] = f"Bearer {token}"
        response = yield request
        if response.status_code != httpx.codes.UNAUTHORIZED:
            return
        token = await self._renew(generation)
        if token is None:
            return
        request.headers["Authorization"] = f"Bearer {token}"
        yield request

    async def _get_token(self) -> str | None:
        """Return a usable bearer token, fetching one when nothing is cached.

        Returns:
            Bearer token, or None when the source cannot produce one.
        """
        token = self._source.get_cached_token()
        if token is not None:
            return token
        return await self._renew(self._generation)

    async def _renew(self, generation: int) -> str | None:
        """Renew the token unless another caller already did.

        Args:
            generation: Generation the caller's rejected token came from.

        Returns:
            Bearer token, or None when the source cannot produce one.
        """
        async with self._lock:
            if generation != self._generation:
                token = self._source.get_cached_token()
                if token is not None:
                    return token
            token = await self._source.fetch_token()
            if token is None:
                return None
            self._generation += 1
            return token

    async def close(self) -> None:
        """Close the token source."""
        await self._source.close()


class CredentialStoreTokenSource:
    """Token source backed by the credentials stored for one server."""

    def __init__(
        self,
        base_url: str,
        store: CredentialStore,
        exchange: TokenExchange,
    ) -> None:
        """Initialize the source.

        Args:
            base_url: Server base URL credentials are stored under.
            store: Credential store holding the token and the way to renew it.
            exchange: Login endpoint calls used to renew the token.
        """
        self._url = normalize_server_url(base_url)
        self._store = store
        self._exchange = exchange
        self._control_plane: ControlPlaneSession | None = None

    def get_cached_token(self) -> str | None:
        """Return the stored bearer token.

        Returns:
            Bearer token, or None when nothing usable is stored.
        """
        credentials = self._store.get(self._url)
        if credentials is not None and credentials.server_api_key is not None:
            # A server API key is used directly as a bearer credential and we
            # don't need to exchange it for a server token.
            return credentials.server_api_key
        token = self._store.get_token(self._url)
        if token is not None:
            return token.access_token
        return None

    async def fetch_token(self) -> str | None:
        """Fetch a fresh bearer token and write it back to the store.

        Returns:
            Bearer token, or None when nothing stored can produce one.
        """
        credentials = self._store.get(self._url)
        if credentials is None:
            return None
        token = await self._fetch(credentials)
        if token is None:
            return None
        self._store.set_token(self._url, token)
        return token.access_token

    async def _fetch(self, credentials: ServerCredentials) -> ApiToken | None:
        """Run the first stored credential that can produce a token.

        Args:
            credentials: Stored credentials of the server.

        Returns:
            Freshly issued token, or None when no stored credential can
            produce one.
        """
        if credentials.device_id is not None and credentials.device_code is not None:
            response = await self._exchange.exchange_device_code(
                credentials.device_id, credentials.device_code
            )
            return ApiToken.from_response(response)
        # The server allows exchanging a control plane API key, so a stored one
        # is used directly. Otherwise, we log into the control plane first and
        # exchange the resulting token for a server token.
        credential = credentials.control_plane_api_key
        if credential is None:
            credential = await self._get_control_plane_credential(credentials)
        if credential is not None:
            response = await self._exchange.exchange_control_plane_credential(
                credential
            )
            return ApiToken.from_response(response)
        return None

    async def _get_control_plane_credential(
        self, credentials: ServerCredentials
    ) -> str | None:
        """Return a control plane token this server accepts.

        Args:
            credentials: Stored credentials of the server.

        Returns:
            Control plane bearer token, or None when the server is not backed
            by a control plane the store can log in against.
        """
        url = credentials.control_plane_api_url
        if url is None:
            return None
        # The control plane is discovered at login, so a client built before
        # that has to pick it up from the store rather than at construction.
        if self._control_plane is None:
            self._control_plane = ControlPlaneSession(url, self._store)
        return await self._control_plane.get_token()

    async def close(self) -> None:
        """Close the control plane session the source opened."""
        if self._control_plane is not None:
            await self._control_plane.close()
            self._control_plane = None
