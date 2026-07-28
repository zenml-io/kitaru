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
"""Stored client credentials."""

import uuid
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Self

from pydantic import BaseModel, ConfigDict

from kitaru.api_models.v1.auth import TokenResponse

# A token is treated as expired this long before its stated expiry, so a
# request started just under the wire does not arrive just over it.
MIN_TOKEN_LEEWAY_SECONDS = 30
# Fraction of the token lifetime used as leeway for longer-lived tokens.
TOKEN_LEEWAY_DIVISOR = 20


class CredentialType(StrEnum):
    """Credential type."""

    SERVER = "server"
    CONTROL_PLANE = "control_plane"


class ApiToken(BaseModel):
    """Cached bearer token."""

    model_config = ConfigDict(extra="forbid")

    access_token: str
    expires_at: datetime | None = None
    leeway_seconds: int = 0

    @classmethod
    def from_response(cls, response: TokenResponse) -> Self:
        """Build a cached token from a login response.

        Args:
            response: Token response returned by the server.

        Returns:
            Token with its absolute expiry and leeway resolved.
        """
        return cls.issued(response.access_token, response.expires_in)

    @classmethod
    def issued(cls, access_token: str, expires_in: int) -> Self:
        """Build a cached token from a bearer token and its lifetime.

        Args:
            access_token: Bearer token.
            expires_in: Token lifetime in seconds.

        Returns:
            Token with its absolute expiry and leeway resolved.
        """
        return cls(
            access_token=access_token,
            expires_at=datetime.now(UTC) + timedelta(seconds=expires_in),
            leeway_seconds=_leeway_seconds(expires_in),
        )

    @property
    def expired(self) -> bool:
        """Report whether the token is expired or about to be.

        Returns:
            Whether the token is inside its leeway window. A token without an
            expiry never expires.
        """
        if self.expires_at is None:
            return False
        deadline = self.expires_at - timedelta(seconds=self.leeway_seconds)
        return deadline <= datetime.now(UTC)


class ServerCredentials(BaseModel):
    """Credentials stored for one server."""

    model_config = ConfigDict(extra="forbid")

    url: str
    type: CredentialType = CredentialType.SERVER
    api_key: str | None = None
    api_token: ApiToken | None = None
    device_id: uuid.UUID | None = None
    device_code: str | None = None
    control_plane_api_url: str | None = None

    @property
    def can_refresh(self) -> bool:
        """Report whether a new token can be obtained without a fresh login.

        Returns:
            Whether an API key or a verified device authorization is stored. A
            control plane entry answers this for itself, so a server pointed at
            one does not count its own control plane URL.
        """
        return self.api_key is not None or (
            self.device_id is not None and self.device_code is not None
        )


def _leeway_seconds(expires_in: int) -> int:
    """Return the leeway applied to a token of the given lifetime.

    Args:
        expires_in: Token lifetime in seconds.

    Returns:
        Seconds subtracted from the expiry when deciding whether to refresh.
    """
    return max(MIN_TOKEN_LEEWAY_SECONDS, expires_in // TOKEN_LEEWAY_DIVISOR)
