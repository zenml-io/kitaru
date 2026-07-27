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
"""API process configuration."""

import uuid
from enum import StrEnum
from typing import Self

from pydantic import model_validator

from kitaru.server.config import Settings

# Sentinel meaning the server has not been enrolled with a control plane.
UNSET_SERVER_ID = uuid.UUID(int=0)


class AuthScheme(StrEnum):
    """Authentication scheme."""

    NONE = "none"
    LOCAL = "local"
    CONTROL_PLANE = "control_plane"


class APISettings(Settings):
    """API server settings."""

    HOST: str = "0.0.0.0"
    PORT: int = 8000

    AUTH_SCHEME: AuthScheme = AuthScheme.NONE

    SERVER_ID: uuid.UUID = UNSET_SERVER_ID
    ENROLLMENT_KEY: str = ""

    CONTROL_PLANE_API_URL: str = ""
    CONTROL_PLANE_AUDIENCE: str | None = None
    CONTROL_PLANE_TIMEOUT_SECONDS: float = 10.0
    CONTROL_PLANE_CONNECTION_POOL_SIZE: int = 20
    CONTROL_PLANE_RETRY_CONNECT: int = 2
    CONTROL_PLANE_RETRY_READ: int = 2
    CONTROL_PLANE_RETRY_STATUS: int = 2
    CONTROL_PLANE_RETRY_OTHER: int = 1
    CONTROL_PLANE_RETRY_BACKOFF_SECONDS: float = 0.25
    CONTROL_PLANE_TOKEN_REFRESH_LEEWAY_SECONDS: int = 60

    JWT_SIGNING_KEY: str = ""
    JWT_ISSUER: str = "kitaru"
    JWT_AUDIENCE: str = "kitaru"
    JWT_LIFETIME_SECONDS: int = 3600
    AUTH_COOKIE_NAME: str = ""

    DEFAULT_ACCOUNT_NAME: str = "default"
    DEFAULT_ACCOUNT_PASSWORD: str | None = None

    SECRET_ENCRYPTION_KEY: str = ""

    @model_validator(mode="after")
    def validate_auth_settings(self) -> Self:
        """Validate authentication settings.

        Raises:
            ValueError: A required setting is not set.

        Returns:
            The validated settings object.
        """
        if (
            self.AUTH_SCHEME in (AuthScheme.LOCAL, AuthScheme.CONTROL_PLANE)
            and not self.JWT_SIGNING_KEY
        ):
            raise ValueError("Set KITARU_SERVER_JWT_SIGNING_KEY")
        if self.AUTH_SCHEME is AuthScheme.CONTROL_PLANE:
            if not self.CONTROL_PLANE_API_URL:
                raise ValueError("Set KITARU_SERVER_CONTROL_PLANE_API_URL")
            if self.SERVER_ID == UNSET_SERVER_ID:
                raise ValueError("Set KITARU_SERVER_SERVER_ID")
        if not self.SECRET_ENCRYPTION_KEY:
            raise ValueError("Set KITARU_SERVER_SECRET_ENCRYPTION_KEY")
        return self
