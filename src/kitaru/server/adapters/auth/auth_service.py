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
"""Authentication service for direct Kitaru server access."""

from datetime import datetime

from kitaru.server.adapters.auth.control_plane import (
    ControlPlaneClient,
    ControlPlaneError,
    ServerAuthorization,
)
from kitaru.server.adapters.auth.jwt import JWTToken, TokenError
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext


class AuthenticationError(Exception):
    """Raised when request authentication fails."""


class ScopeError(Exception):
    """Raised when an authenticated context is outside this server identity."""


class AuthService:
    """Resolve bearer credentials into request contexts."""

    def __init__(
        self,
        settings: APISettings,
        control_plane: ControlPlaneClient,
    ) -> None:
        """Create an authentication service.

        Args:
            settings: Runtime settings for this server.
            control_plane: Control plane API client used to validate external
                credentials.
        """
        self._settings = settings
        self._control_plane = control_plane

    async def resolve(
        self,
        credential: str,
        csrf_token: str | None = None,
    ) -> AuthContext:
        """Authenticate a bearer credential for API route handling.

        Args:
            credential: Bearer token supplied by the caller.
            csrf_token: CSRF token supplied alongside the bearer token.

        Raises:
            AuthenticationError: The credential cannot be validated.

        Returns:
            Request context accepted by this server.
        """
        try:
            context = JWTToken.decode(credential, self._settings).to_auth_context()
        except TokenError:
            context = await self._resolve_control_plane_credential(credential)
        if context.csrf_token is not None and csrf_token != context.csrf_token:
            raise AuthenticationError("Missing or invalid CSRF token.")
        return context

    def issue_token(
        self, context: AuthContext, csrf_token: str | None = None
    ) -> tuple[str, datetime]:
        """Issue a local session for an auth context.

        Args:
            context: Resolved context to store in the session token.
            csrf_token: CSRF token associated with a browser cookie session.

        Raises:
            AuthenticationError: Local session signing is not configured.

        Returns:
            Encoded bearer token and its expiry time.
        """
        try:
            token = JWTToken.from_auth_context(
                context,
                csrf_token=csrf_token,
            )
            token = token.model_copy(update={"expires_at": None})
            expires_at = token.expires(self._settings)
            token = token.model_copy(update={"expires_at": expires_at})
            return token.encode(self._settings), expires_at
        except TokenError as exc:
            raise AuthenticationError(str(exc)) from exc

    async def _resolve_control_plane_credential(self, credential: str) -> AuthContext:
        server_id = self._settings.SERVER_ID
        try:
            authorization = await self._control_plane.authorize_server(
                credential=credential,
                server_id=server_id,
            )
        except ControlPlaneError as exc:
            raise AuthenticationError(str(exc)) from exc
        if authorization.server_id != server_id:
            raise ScopeError("Credential is for a different server.")
        return self._context_from_control_plane(authorization)

    def _context_from_control_plane(
        self, authorization: ServerAuthorization
    ) -> AuthContext:
        return AuthContext(
            user=authorization.user,
            expires_at=authorization.expires_at,
        )
