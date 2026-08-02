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
"""JWT support for Kitaru server sessions, workers, and tasks."""

import uuid
from datetime import UTC, datetime
from typing import Annotated, Any, Literal, Self

import jwt
from pydantic import BaseModel, Field

from kitaru.server.api.config import APISettings
from kitaru.server.utils import to_tz_aware

DEFAULT_JWT_ALGORITHM = "HS256"


class TokenError(Exception):
    """Raised when a local session token is missing, invalid, or expired."""


def _optional_uuid_claim(value: object) -> uuid.UUID | None:
    """Parse an optional claim as a UUID.

    Args:
        value: Claim value, None when the claim is absent.

    Raises:
        ValueError: The value is not a valid UUID string.

    Returns:
        Parsed UUID, or None when the claim is absent.
    """
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("claim is not a UUID string.")
    return uuid.UUID(value)


# The subject classes below define the wire format of issued tokens: their
# fields map straight to JWT claims. Any change to them must stay backward
# compatible with tokens that are already issued and still valid.
class AccountSubject(BaseModel):
    """Account token subject."""

    kind: Literal["account"] = "account"
    account_id: uuid.UUID
    csrf_token: str | None = None
    device_id: uuid.UUID | None = None

    @classmethod
    def from_claims(cls, raw_id: str, claims: dict[str, Any]) -> Self:
        """Parse the subject from the sub claim's id and the token claims.

        Args:
            raw_id: Id carried in the sub claim.
            claims: Decoded token claims.

        Returns:
            Parsed subject.
        """
        return cls(
            account_id=uuid.UUID(raw_id),
            csrf_token=claims.pop("csrf", None),
            device_id=_optional_uuid_claim(claims.pop("device_id", None)),
        )

    def to_claims(self) -> dict[str, object]:
        """Build the claims carrying this subject.

        Returns:
            Claims including the sub claim.
        """
        claims: dict[str, object] = {"sub": f"account:{self.account_id}"}
        if self.csrf_token is not None:
            claims["csrf"] = self.csrf_token
        if self.device_id is not None:
            claims["device_id"] = str(self.device_id)
        return claims


class WorkerSubject(BaseModel):
    """Worker token subject."""

    kind: Literal["worker"] = "worker"
    worker_id: uuid.UUID
    account_id: uuid.UUID

    @classmethod
    def from_claims(cls, raw_id: str, claims: dict[str, Any]) -> Self:
        """Parse the subject from the sub claim's id and the token claims.

        Args:
            raw_id: Id carried in the sub claim.
            claims: Decoded token claims.

        Returns:
            Parsed subject.
        """
        return cls(
            worker_id=uuid.UUID(raw_id),
            account_id=uuid.UUID(claims.pop("account_id")),
        )

    def to_claims(self) -> dict[str, object]:
        """Build the claims carrying this subject.

        Returns:
            Claims including the sub claim.
        """
        return {
            "sub": f"worker:{self.worker_id}",
            "account_id": str(self.account_id),
        }


class TaskSubject(BaseModel):
    """Task token subject."""

    kind: Literal["task"] = "task"
    task_id: uuid.UUID
    attempt: int
    worker_id: uuid.UUID
    account_id: uuid.UUID
    input_session_id: uuid.UUID | None = None

    @classmethod
    def from_claims(cls, raw_id: str, claims: dict[str, Any]) -> Self:
        """Parse the subject from the sub claim's id and the token claims.

        Args:
            raw_id: Id carried in the sub claim.
            claims: Decoded token claims.

        Raises:
            ValueError: The attempt claim is not an integer.

        Returns:
            Parsed subject.
        """
        attempt = claims.pop("attempt")
        if not isinstance(attempt, int):
            raise ValueError("attempt claim is not an integer.")
        return cls(
            task_id=uuid.UUID(raw_id),
            attempt=attempt,
            worker_id=uuid.UUID(claims.pop("worker_id")),
            account_id=uuid.UUID(claims.pop("account_id")),
            input_session_id=_optional_uuid_claim(claims.pop("input_session_id", None)),
        )

    def to_claims(self) -> dict[str, object]:
        """Build the claims carrying this subject.

        Returns:
            Claims including the sub claim.
        """
        claims: dict[str, object] = {
            "sub": f"task:{self.task_id}",
            "attempt": self.attempt,
            "worker_id": str(self.worker_id),
            "account_id": str(self.account_id),
        }
        if self.input_session_id is not None:
            claims["input_session_id"] = str(self.input_session_id)
        return claims


TokenSubject = Annotated[
    AccountSubject | WorkerSubject | TaskSubject, Field(discriminator="kind")
]

_SUBJECT_TYPES: dict[str, type[AccountSubject | WorkerSubject | TaskSubject]] = {
    "account": AccountSubject,
    "worker": WorkerSubject,
    "task": TaskSubject,
}


class JWTToken(BaseModel):
    """Kitaru server token for an account session, a worker, or a task attempt."""

    subject: TokenSubject
    expires_at: datetime

    @classmethod
    def decode(
        cls,
        token: str,
        settings: APISettings,
        algorithm: str = DEFAULT_JWT_ALGORITHM,
    ) -> Self:
        """Decode a token issued by this server.

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
            prefix, _, raw_id = subject.partition(":")
            if not raw_id:
                raise ValueError("subject carries no id.")
            subject_type = _SUBJECT_TYPES.get(prefix)
            if subject_type is None:
                raise ValueError(f"subject kind '{prefix}' is not recognized.")
            expires_at = cls._timestamp_claim(claims.pop("exp"))
            parsed = subject_type.from_claims(raw_id, claims)
        except (AttributeError, KeyError, TypeError, ValueError) as exc:
            raise TokenError(f"Invalid session token claims: {exc}") from exc

        return cls(subject=parsed, expires_at=expires_at)

    def encode(
        self,
        settings: APISettings,
        algorithm: str = DEFAULT_JWT_ALGORITHM,
    ) -> str:
        """Create a signed token.

        Args:
            settings: Runtime settings containing JWT issuer, audience, and
                signing key.
            algorithm: JWT signing algorithm to use.

        Returns:
            Encoded bearer token.
        """
        claims: dict[str, object] = {
            **self.subject.to_claims(),
            "iss": settings.JWT_ISSUER,
            "aud": settings.JWT_AUDIENCE,
        }
        claims["iat"] = int(datetime.now(UTC).timestamp())
        claims["exp"] = int(to_tz_aware(self.expires_at).timestamp())

        return jwt.encode(
            payload=claims,
            key=settings.JWT_SIGNING_KEY,
            algorithm=algorithm,
        )

    @staticmethod
    def _timestamp_claim(value: object) -> datetime:
        if not isinstance(value, int | float | str | bytes | bytearray):
            raise ValueError("timestamp claim is not a valid timestamp.")
        return datetime.fromtimestamp(int(value), tz=UTC)
