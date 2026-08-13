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
"""Round-trip tests for the info SDK resource."""

from conftest import asgi_api_client, control_plane_settings, local_settings
from kitaru.api_models.v1.info import AuthScheme
from kitaru.server.api.app import create_app


async def test_info_reports_the_local_scheme_without_a_control_plane() -> None:
    """Report the local scheme and no control plane URL."""
    app = create_app(local_settings())
    async with asgi_api_client(app) as client:
        info = await client.info.get()

    assert info.auth_scheme is AuthScheme.LOCAL
    assert info.control_plane_api_url is None
    assert info.version


async def test_info_reports_the_control_plane_api_url() -> None:
    """Report the control plane a client has to log in against."""
    settings = control_plane_settings()
    app = create_app(settings)
    async with asgi_api_client(app) as client:
        info = await client.info.get()

    assert info.auth_scheme is AuthScheme.CONTROL_PLANE
    assert info.control_plane_api_url == "https://control-plane.example.com"
    assert info.id == settings.SERVER_ID


async def test_info_reports_the_configured_urls() -> None:
    """Report the server and dashboard URLs without their trailing slash."""
    app = create_app(
        local_settings(
            SERVER_URL="https://kitaru.example.com/",
            DASHBOARD_URL="https://dashboard.example.com/",
        )
    )
    async with asgi_api_client(app) as client:
        info = await client.info.get()

    assert info.server_url == "https://kitaru.example.com"
    assert info.dashboard_url == "https://dashboard.example.com"


async def test_info_omits_unconfigured_urls_and_id() -> None:
    """Leave the URLs and the server id null when they are not configured."""
    app = create_app(local_settings())
    async with asgi_api_client(app) as client:
        info = await client.info.get()

    assert info.id is None
    assert info.server_url is None
    assert info.dashboard_url is None


async def test_info_is_unauthenticated() -> None:
    """Answer without a bearer token, so a client can read it before login."""
    app = create_app(control_plane_settings())
    async with asgi_api_client(app) as client:
        response = await client.request("GET", "/v1/info", authenticate=False)

    assert response.status_code == 200
    assert "Authorization" not in response.request.headers
