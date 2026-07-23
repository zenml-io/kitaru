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
"""JWT support for Kitaru server sessions."""

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, Self

import jwt
from pydantic import BaseModel

from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.utils import to_tz_aware

DEFAULT_JWT_ALGORITHM = "HS256"


class TokenError(Exception):
    """Raised when a local session token is missing, invalid, or expired."""


class JWTToken(BaseModel):
    """Kitaru server session token."""

    account_id: uuid.UUID
    csrf_token: str | None = None
    expires_at: datetime | None = None

    @classmethod
    def from_auth_context(
        cls, context: AuthContext, csrf_token: str | None = None
    ) -> Self:
        """Build a local session token from an authenticated context.

        Args:
            context: Authenticated request context.
            csrf_token: CSRF token associated with a browser cookie session.

        Returns:
            Token representation for the supplied context.
        """
        return cls(
            account_id=context.account.id,
            csrf_token=csrf_token,
        )

    @classmethod
    def decode(
        cls,
        token: str,
        settings: APISettings,
        algorithm: str = DEFAULT_JWT_ALGORITHM,
    ) -> Self:
        """Decode a local session token.

        Args:
            token: JWT issued by this server.
            settings: Runtime settings containing JWT issuer, audience, and
                signing key.
            algorithm: JWT signing algorithm to accept.

        Raises:
            TokenError: The token is malformed, expired, or fails validation.

        Returns:
            Decoded token representation.
        """
        try:
            claims: dict[str, Any] = jwt.decode(
                jwt=token,
                key=settings.JWT_SIGNING_KEY,
                algorithms=[algorithm],
                audience=settings.JWT_AUDIENCE,
                issuer=settings.JWT_ISSUER,
                options={
                    "require": [
                        "sub",
                        "exp",
                    ],
                },
            )
        except jwt.PyJWTError as exc:
            raise TokenError(f"Invalid session token: {exc}") from exc

        subject = claims.pop("sub", None)
        if not isinstance(subject, str) or not subject:
            raise TokenError("Invalid session token: the subject claim is missing.")

        try:
            prefix, _, raw_account_id = subject.partition(":")
            if prefix != "account" or not raw_account_id:
                raise ValueError("subject is not an account subject.")
            account_id = uuid.UUID(raw_account_id)
            csrf_token = claims.pop("csrf", None)
            expires_at = cls._timestamp_claim(claims.pop("exp"))
        except (KeyError, TypeError, ValueError) as exc:
            raise TokenError(f"Invalid session token claims: {exc}") from exc

        return cls(
            account_id=account_id,
            csrf_token=csrf_token,
            expires_at=expires_at,
        )

    def encode(
        self,
        settings: APISettings,
        algorithm: str = DEFAULT_JWT_ALGORITHM,
    ) -> str:
        """Create a signed local session token.

        Args:
            settings: Runtime settings containing JWT issuer, audience, and
                signing key.
            algorithm: JWT signing algorithm to use.

        Returns:
            Encoded bearer token.
        """
        claims: dict[str, object] = {
            "sub": f"account:{self.account_id}",
            "iss": settings.JWT_ISSUER,
            "aud": settings.JWT_AUDIENCE,
        }
        claims["iat"] = int(datetime.now(UTC).timestamp())
        if self.expires_at is not None:
            claims["exp"] = int(to_tz_aware(self.expires_at).timestamp())
        else:
            claims["exp"] = int(self.expires(settings).timestamp())
        if self.csrf_token is not None:
            claims["csrf"] = self.csrf_token

        return jwt.encode(
            payload=claims,
            key=settings.JWT_SIGNING_KEY,
            algorithm=algorithm,
        )

    def expires(self, settings: APISettings) -> datetime:
        """Return the expiration time that will be used when encoding.

        Args:
            settings: Runtime settings controlling local session lifetime.

        Returns:
            Expiration time for the encoded local session.
        """
        return datetime.now(UTC) + timedelta(seconds=settings.JWT_LIFETIME_SECONDS)

    @staticmethod
    def _timestamp_claim(value: object) -> datetime:
        if not isinstance(value, int | float | str | bytes | bytearray):
            raise ValueError("timestamp claim is not a valid timestamp.")
        return datetime.fromtimestamp(int(value), tz=UTC)
