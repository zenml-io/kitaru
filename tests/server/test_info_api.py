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
"""Tests for the server info route."""

import httpx
import pytest
from pydantic import ValidationError

from conftest import control_plane_settings, local_settings
from kitaru.api_models.v1.info import AuthScheme
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings


async def _get_info(settings: APISettings) -> dict[str, object]:
    """Read the info route of an app built from the settings.

    Args:
        settings: API server settings.

    Returns:
        Decoded response body.
    """
    app = create_app(settings)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/api/v1/info")
    assert response.status_code == 200
    payload: dict[str, object] = response.json()
    return payload


async def test_local_scheme_reports_no_control_plane() -> None:
    """Report the local scheme without naming a control plane."""
    payload = await _get_info(local_settings())

    assert payload["auth_scheme"] == AuthScheme.LOCAL.value
    assert payload["control_plane_api_url"] is None


async def test_control_plane_api_url_drops_its_trailing_slash() -> None:
    """Report a control plane URL a client can join paths onto."""
    payload = await _get_info(
        control_plane_settings(CONTROL_PLANE_API_URL="https://cp.example.com/")
    )

    assert payload["auth_scheme"] == AuthScheme.CONTROL_PLANE.value
    assert payload["control_plane_api_url"] == "https://cp.example.com"


async def test_analytics_enabled_reflects_the_opt_in_setting() -> None:
    """Report whether the server sends analytics events."""
    payload = await _get_info(local_settings(ANALYTICS_OPT_IN=False))

    assert payload["analytics_enabled"] is False


async def test_max_blob_size_reports_the_effective_server_limit() -> None:
    """Report the configured blob upload limit to clients."""
    payload = await _get_info(local_settings(MAX_BLOB_SIZE_BYTES=12_345))

    assert payload["max_blob_size_bytes"] == 12_345


async def test_max_blob_size_reports_zero_limit() -> None:
    """Keep the info route available when nonempty blobs are disabled."""
    payload = await _get_info(local_settings(MAX_BLOB_SIZE_BYTES=0))

    assert payload["max_blob_size_bytes"] == 0


def test_negative_blob_limit_is_rejected_at_startup() -> None:
    """Reject invalid settings before the info route can serve requests."""
    with pytest.raises(ValidationError, match="MAX_BLOB_SIZE_BYTES"):
        local_settings(MAX_BLOB_SIZE_BYTES=-1)
