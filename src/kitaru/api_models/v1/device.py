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
"""Device API models."""

import uuid
from datetime import datetime
from enum import StrEnum

from pydantic import Field

from kitaru.api_models.v1.base import (
    RequestModel,
    TimestampedResponseModel,
)
from kitaru.api_models.v1.filter import FilterableListParams


class DeviceStatus(StrEnum):
    """Authorized device status."""

    PENDING = "pending"
    VERIFIED = "verified"
    ACTIVE = "active"


class DeviceVerifyRequest(RequestModel):
    """Device verify request."""

    user_code: str = Field(description="User code shown on the device.")
    trusted: bool = Field(
        default=False, description="Whether to grant the trusted device lifetime."
    )


class DeviceUpdateRequest(RequestModel):
    """Device update request."""

    locked: bool | None = Field(
        default=None, description="New locked state, left unchanged when omitted."
    )
    trusted: bool | None = Field(
        default=None, description="New trusted state, left unchanged when omitted."
    )


class DeviceListParams(FilterableListParams):
    """Device list params."""


class DeviceResponse(TimestampedResponseModel):
    """Device response."""

    id: uuid.UUID = Field(description="Device id.")
    status: DeviceStatus = Field(description="Device status.")
    locked: bool = Field(description="Whether the device can authenticate.")
    trusted: bool = Field(description="Whether the device has the trusted lifetime.")
    expires: datetime | None = Field(
        description="Expiry time, null for a device that never expires."
    )
    last_login: datetime | None = Field(
        description="Time of the last authentication with this device."
    )
    hostname: str | None = Field(description="Host the device reported.")
    os: str | None = Field(description="Operating system the device reported.")
    ip_address: str | None = Field(description="Address the authorization came from.")
    python_version: str | None = Field(
        description="Python version the device reported."
    )
    client_version: str | None = Field(
        description="Kitaru version the device reported."
    )
