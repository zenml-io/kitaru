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
"""Device DTO conversions."""

from urllib.parse import urlencode

from kitaru.api_models.v1.auth import DeviceAuthorizationResponse
from kitaru.api_models.v1.device import (
    DeviceListParams,
    DeviceResponse,
    DeviceStatus,
)
from kitaru.server.application.models.device import DeviceFilter, DevicePolicy
from kitaru.server.domain.device import Device
from kitaru.server.domain.device import DeviceStatus as DomainDeviceStatus


def device_to_response(device: Device) -> DeviceResponse:
    """Convert a device entity to its response DTO.

    Args:
        device: Stored device.

    Returns:
        Device response.
    """
    assert device.created is not None
    assert device.updated is not None
    return DeviceResponse(
        id=device.id,
        status=DeviceStatus(device.status.value),
        locked=device.locked,
        trusted=device.trusted,
        expires=device.expires,
        last_login=device.last_login,
        hostname=device.hostname,
        os=device.os,
        ip_address=device.ip_address,
        python_version=device.python_version,
        client_version=device.client_version,
        created=device.created,
        updated=device.updated,
    )


def device_to_authorization_response(
    device: Device,
    user_code: str,
    device_code: str,
    dashboard_url: str,
    policy: DevicePolicy,
) -> DeviceAuthorizationResponse:
    """Convert a new device authorization to its response DTO.

    Args:
        device: Stored device.
        user_code: Plaintext user code.
        device_code: Plaintext device code.
        dashboard_url: Base URL of the page serving the verification form.
        policy: Device authorization policy.

    Returns:
        Device authorization response carrying the plaintext codes.
    """
    verification_uri = f"{dashboard_url.rstrip('/')}/devices/verify"
    query = urlencode({"device_id": str(device.id), "user_code": user_code})
    return DeviceAuthorizationResponse(
        device_id=device.id,
        device_code=device_code,
        user_code=user_code,
        verification_uri=verification_uri,
        verification_uri_complete=f"{verification_uri}?{query}",
        expires_in=policy.auth_timeout_seconds,
        interval=policy.polling_interval_seconds,
    )


def device_list_params_to_filter(params: DeviceListParams) -> DeviceFilter:
    """Convert device list params to the application filter.

    Args:
        params: Device list params.

    Returns:
        Device filter.
    """
    return DeviceFilter(
        status=DomainDeviceStatus(params.status.value)
        if params.status is not None
        else None,
        cursor=params.cursor,
        size=params.size,
        sort=params.sort,
    )
