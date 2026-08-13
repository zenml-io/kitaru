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
"""Auth API models."""

import uuid
from enum import StrEnum

from pydantic import Field

from kitaru.api_models.v1.base import ResponseModel

API_KEY_PREFIX = "KITKEY_"
CONTROL_PLANE_API_KEY_PREFIX = "ZENPROKEY_"


class GrantType(StrEnum):
    """Login grant type."""

    PASSWORD = "password"
    API_KEY = "api-key"
    CONTROL_PLANE = "control-plane"
    DEVICE_CODE = "urn:ietf:params:oauth:grant-type:device_code"


class TokenErrorCode(StrEnum):
    """Device authorization grant error code."""

    AUTHORIZATION_PENDING = "authorization_pending"
    SLOW_DOWN = "slow_down"
    ACCESS_DENIED = "access_denied"
    EXPIRED_TOKEN = "expired_token"
    INVALID_GRANT = "invalid_grant"
    INVALID_REQUEST = "invalid_request"
    UNSUPPORTED_GRANT_TYPE = "unsupported_grant_type"


class TokenResponse(ResponseModel):
    """Token response."""

    access_token: str = Field(description="Bearer token.")
    token_type: str = Field(description="Token type.")
    expires_in: int = Field(description="Token lifetime in seconds.")
    csrf_token: str | None = Field(
        default=None, description="CSRF token for cookie authentication."
    )


class TokenErrorResponse(ResponseModel):
    """Token error response."""

    error: TokenErrorCode = Field(description="Error code.")
    detail: str = Field(description="Error message.")


class DeviceAuthorizationResponse(ResponseModel):
    """Device authorization response."""

    device_id: uuid.UUID = Field(description="Device id to verify.")
    device_code: str = Field(description="Code the device presents when polling.")
    user_code: str = Field(description="Code the user confirms in the browser.")
    verification_uri: str = Field(description="Page where the user enters the code.")
    verification_uri_complete: str = Field(
        description="Verification page with the code already filled in."
    )
    expires_in: int = Field(description="Code lifetime in seconds.")
    interval: int = Field(description="Seconds to wait between polls.")
