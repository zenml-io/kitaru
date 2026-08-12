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
"""Tests for the bundled dashboard routes."""

from pathlib import Path

import httpx
import pytest

from conftest import local_settings
from kitaru.server.api import dashboard
from kitaru.server.api.app import create_app


@pytest.fixture(params=[True, False], ids=["with-static-404", "without-static-404"])
def ui_dist(tmp_path: Path, request: pytest.FixtureRequest) -> Path:
    """Create a representative built dashboard directory."""
    dist = tmp_path / "dist"
    assets = dist / "assets"
    assets.mkdir(parents=True)
    (dist / "index.html").write_text(
        "<!doctype html><html><body>Kitaru dashboard</body></html>",
        encoding="utf-8",
    )
    if request.param:
        (dist / "404.html").write_text(
            "<!doctype html><html><body>Static asset not found</body></html>",
            encoding="utf-8",
        )
    (assets / "app.js").write_text("window.KITARU = true;", encoding="utf-8")
    return dist


async def test_serves_dashboard_and_spa_routes(
    monkeypatch: pytest.MonkeyPatch, ui_dist: Path
) -> None:
    """Serve packaged assets and fall back to the SPA for client routes."""
    monkeypatch.setattr(dashboard, "_get_packaged_ui_dist_path", lambda: ui_dist)
    app = create_app(local_settings())
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        root = await client.get("/")
        device_verify = await client.get(
            "/devices/verify", params={"device_id": "test", "user_code": "test"}
        )
        asset = await client.get("/assets/app.js")
        missing_asset = await client.get("/assets/missing.js")
        missing_api = await client.get("/v1/not-a-route")
        missing_api_post = await client.post("/v1/not-a-route")
        health_method_not_allowed = await client.post("/health/live")
        info_redirect = await client.get("/v1/info/", follow_redirects=False)

    assert root.status_code == 200
    assert root.headers["content-type"].startswith("text/html")
    assert "Kitaru dashboard" in root.text
    assert device_verify.status_code == 200
    assert device_verify.text == root.text
    assert asset.status_code == 200
    assert asset.text == "window.KITARU = true;"
    assert missing_asset.status_code == 404
    assert missing_api.status_code == 404
    assert missing_api.json() == {"detail": "Not Found"}
    assert missing_api_post.status_code == 404
    assert missing_api_post.json() == {"detail": "Not Found"}
    assert health_method_not_allowed.status_code == 405
    assert health_method_not_allowed.headers["allow"] == "GET"
    assert info_redirect.status_code == 307
    assert info_redirect.headers["location"] == "http://test/v1/info"


async def test_keeps_api_only_server_when_dashboard_is_not_packaged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep the server usable when a development build has no dashboard."""
    monkeypatch.setattr(dashboard, "_get_packaged_ui_dist_path", lambda: None)
    app = create_app(local_settings())
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/")

    assert response.status_code == 404
    assert response.json() == {"detail": "Not Found"}


async def test_preserves_backend_routes_with_root_path(
    monkeypatch: pytest.MonkeyPatch, ui_dist: Path
) -> None:
    """Classify backend and dashboard paths relative to the configured root."""
    monkeypatch.setattr(dashboard, "_get_packaged_ui_dist_path", lambda: ui_dist)
    app = create_app(local_settings(ROOT_URL_PATH="/prefix"))
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        missing_api = await client.get("/prefix/v1/not-a-route")
        client_route = await client.get("/prefix/devices/verify")

    assert missing_api.status_code == 404
    assert missing_api.json() == {"detail": "Not Found"}
    assert client_route.status_code == 200
    assert "Kitaru dashboard" in client_route.text
