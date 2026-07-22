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
from kitaru.server.application.models.auth import AuthContext, AuthUser

DEFAULT_JWT_ALGORITHM = "HS256"


class TokenError(Exception):
    """Raised when a local session token is missing, invalid, or expired."""


class JWTToken(BaseModel):
    """Kitaru server session token."""

    user: AuthUser | None = None
    csrf_token: str | None = None
    issued_at: datetime | None = None
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
            user=context.user,
            csrf_token=csrf_token,
            expires_at=context.expires_at,
        )

    def to_auth_context(self) -> AuthContext:
        """Build the authenticated context represented by this token.

        Returns:
            Authenticated request context stored in the token.
        """
        return AuthContext(
            user=self.user,
            expires_at=self.expires_at,
            csrf_token=self.csrf_token,
        )

    @classmethod
    def decode(
        cls,
        token: str,
        settings: APISettings,
        algorithm: str = DEFAULT_JWT_ALGORITHM,
        verify: bool = True,
    ) -> Self:
        """Decode a local session token.

        Args:
            token: JWT issued by this server.
            settings: Runtime settings containing JWT issuer, audience, and
                signing key.
            algorithm: JWT signing algorithm to accept.
            verify: Whether to verify the token signature.

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
                    "verify_signature": verify,
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
            user_id = cls._uuid_claim(claims, "user_id")
            if user_id is None:
                raise ValueError("user_id claim is missing.")
            user = AuthUser(
                id=user_id,
                username=cls._string_claim(claims, "username"),
                email=cls._string_claim(claims, "email"),
                is_service_account=bool(claims.pop("is_service_account", False)),
                is_superuser=bool(claims.pop("is_superuser", False)),
            )
            csrf_token = claims.pop("csrf", None)
            expires_at = cls._timestamp_claim(claims.pop("exp"))
            issued_at = None
            if "iat" in claims:
                issued_at = cls._timestamp_claim(claims.pop("iat"))
            if subject != f"user:{user.id}":
                raise ValueError("subject does not match token principal.")
        except (KeyError, TypeError, ValueError) as exc:
            raise TokenError(f"Invalid session token claims: {exc}") from exc

        return cls(
            user=user,
            csrf_token=csrf_token,
            issued_at=issued_at,
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
            "sub": self._subject(),
            "iss": settings.JWT_ISSUER,
            "aud": settings.JWT_AUDIENCE,
        }
        issued_at = self.issued_at or datetime.now(UTC)
        claims["iat"] = int(self._aware(issued_at).timestamp())
        claims["exp"] = int(
            self._aware(self.expires(settings, issued_at=issued_at)).timestamp()
        )
        if self.user is not None:
            claims["user_id"] = str(self.user.id)
            if self.user.username is not None:
                claims["username"] = self.user.username
            if self.user.email is not None:
                claims["email"] = self.user.email
            if self.user.is_service_account:
                claims["is_service_account"] = True
            if self.user.is_superuser:
                claims["is_superuser"] = True
        if self.csrf_token is not None:
            claims["csrf"] = self.csrf_token

        return jwt.encode(
            payload=claims,
            key=settings.JWT_SIGNING_KEY,
            algorithm=algorithm,
        )

    def expires(
        self, settings: APISettings, issued_at: datetime | None = None
    ) -> datetime:
        """Return the expiration time that will be used when encoding.

        Args:
            settings: Runtime settings controlling local session lifetime.
            issued_at: Optional issue time to use as the lifetime baseline.

        Returns:
            Expiration time for the encoded local session.
        """
        now = self._aware(issued_at) if issued_at else datetime.now(UTC)
        expires_at = now + timedelta(seconds=settings.JWT_LIFETIME_SECONDS)
        if self.expires_at is not None:
            max_expires_at = self._aware(self.expires_at)
            if max_expires_at < expires_at:
                expires_at = max_expires_at
        return expires_at

    @staticmethod
    def _aware(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    @staticmethod
    def _timestamp_claim(value: object) -> datetime:
        if not isinstance(value, int | float | str | bytes | bytearray):
            raise ValueError("timestamp claim is not a valid timestamp.")
        return datetime.fromtimestamp(int(value), tz=UTC)

    @staticmethod
    def _uuid_claim(claims: dict[str, Any], name: str) -> uuid.UUID | None:
        value = claims.pop(name, None)
        if value is None:
            return None
        return uuid.UUID(str(value))

    @staticmethod
    def _string_claim(claims: dict[str, Any], name: str) -> str | None:
        value = claims.pop(name, None)
        if value is None:
            return None
        if not isinstance(value, str):
            raise ValueError(f"{name} claim is not a string.")
        return value

    def _subject(self) -> str:
        if self.user is not None:
            return f"user:{self.user.id}"
        raise TokenError("Session token requires a user.")
