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
"""Device authorization grant polling."""

import asyncio
import platform
import socket
from collections.abc import Awaitable, Callable
from importlib.metadata import version
from typing import NamedTuple, TypeVar

from kitaru.api_models.v1.auth import TokenErrorCode
from kitaru.client.exceptions import TokenGrantError

# Seconds added to the poll interval each time the server asks for less load.
SLOW_DOWN_INCREMENT_SECONDS = 5

T = TypeVar("T")


class DeviceLoginError(Exception):
    """Raised when a device authorization cannot be completed."""


class DeviceFingerprint(NamedTuple):
    """Description of the machine asking for authorization."""

    hostname: str
    os: str
    python_version: str
    client_version: str


def describe_this_device() -> DeviceFingerprint:
    """Describe the machine asking for authorization.

    Returns:
        Fingerprint fields an authorization server stores with the device.
    """
    return DeviceFingerprint(
        hostname=socket.gethostname(),
        os=platform.system(),
        python_version=platform.python_version(),
        client_version=version("kitaru"),
    )


async def poll_for_token(
    exchange: Callable[[], Awaitable[T]], expires_in: int, interval: int
) -> T:
    """Poll a token endpoint until a device authorization is confirmed.

    Args:
        exchange: Runs one token request for the device.
        expires_in: Seconds the authorization stays confirmable.
        interval: Seconds to wait between polls.

    Raises:
        DeviceLoginError: The authorization expired or was refused.

    Returns:
        Token the endpoint issued once the authorization was confirmed.
    """
    remaining = expires_in
    while remaining > 0:
        await asyncio.sleep(interval)
        remaining -= interval
        try:
            return await exchange()
        except TokenGrantError as exc:
            if exc.error == TokenErrorCode.SLOW_DOWN:
                interval += SLOW_DOWN_INCREMENT_SECONDS
                continue
            if exc.error != TokenErrorCode.AUTHORIZATION_PENDING:
                raise DeviceLoginError(exc.detail) from exc
    raise DeviceLoginError("Device authorization expired before it was confirmed")
