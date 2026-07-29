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
"""Tests for control-plane HTTP failure classification."""

from types import SimpleNamespace
from typing import Any

import pytest

from kitaru.server.adapters.auth.control_plane import (
    ControlPlaneAuthorizationError,
    ControlPlaneClient,
    ControlPlaneHTTPError,
    ControlPlaneUnavailableError,
)


def _client_raising(error: Exception) -> ControlPlaneClient:
    client = object.__new__(ControlPlaneClient)
    client._settings = SimpleNamespace(  # type: ignore[assignment]
        CONTROL_PLANE_RETRY_READ=0,
        CONTROL_PLANE_RETRY_STATUS=0,
        CONTROL_PLANE_RETRY_OTHER=0,
        CONTROL_PLANE_RETRY_BACKOFF_SECONDS=0,
    )

    async def _send(*args: Any, **kwargs: Any) -> Any:
        raise error

    client._send = _send  # type: ignore[method-assign]
    return client


@pytest.mark.parametrize("status_code", [401, 403])
async def test_control_plane_rejection_is_an_authorization_error(
    status_code: int,
) -> None:
    """Treat explicit authentication and authorization denials as invalid."""
    client = _client_raising(ControlPlaneHTTPError(status_code))

    with pytest.raises(ControlPlaneAuthorizationError, match=str(status_code)):
        await client._request_json("GET", "/authorize")


@pytest.mark.parametrize("status_code", [400, 500, 503])
async def test_control_plane_failure_is_an_availability_error(
    status_code: int,
) -> None:
    """Do not misreport control-plane failures as invalid credentials."""
    client = _client_raising(ControlPlaneHTTPError(status_code))

    with pytest.raises(ControlPlaneUnavailableError):
        await client._request_json("GET", "/authorize")
