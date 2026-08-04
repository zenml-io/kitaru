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

from conftest import control_plane_settings, local_settings
from kitaru.api_models.v1.info import AuthScheme, ServerInfoResponse
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
        response = await client.get("/v1/info")
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
    assert payload["features"] == ["idempotency.v1"]


def test_server_info_defaults_features_for_older_payloads() -> None:
    """Parse an older server response that predates feature discovery."""
    response = ServerInfoResponse.model_validate(
        {
            "version": "1.0.0",
            "auth_scheme": "none",
        }
    )
    assert response.features == []
