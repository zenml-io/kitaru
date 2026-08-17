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
"""Tests for bundled UI serving."""

import json
from pathlib import Path

import httpx
import pydantic
import pytest
from fastapi import FastAPI

from conftest import local_settings
from kitaru.server.api import ui
from kitaru.server.api.app import create_app


@pytest.fixture
def ui_dist_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Build a fake UI bundle and point the resolver at it."""
    dist_dir = tmp_path / "dist"
    assets_dir = dist_dir / "assets"
    assets_dir.mkdir(parents=True)
    (dist_dir / "index.html").write_text("<html>index</html>", encoding="utf-8")
    (assets_dir / "app-abc123.js").write_text("console.log('app')", encoding="utf-8")
    (dist_dir / "favicon.svg").write_text("<svg></svg>", encoding="utf-8")
    (tmp_path / "bundle_manifest.json").write_text(
        json.dumps({"tag": "kitaru-ui-v0.9.0"}), encoding="utf-8"
    )
    monkeypatch.setattr(ui, "_get_ui_dist_dir", lambda: dist_dir)
    return dist_dir


def _build_client(app: FastAPI) -> httpx.AsyncClient:
    """Build a client over the app's ASGI transport without following redirects.

    Args:
        app: FastAPI application.

    Returns:
        Client for issuing requests against the app.
    """
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        follow_redirects=False,
    )


async def test_root_serves_index_html(ui_dist_dir: Path) -> None:
    """Serve the SPA shell with a no-cache header at the root URL."""
    app = create_app(local_settings())
    async with _build_client(app) as client:
        response = await client.get("/")

    assert response.status_code == 200
    assert response.text == "<html>index</html>"
    assert response.headers["Cache-Control"] == "no-cache"


async def test_asset_is_served_with_immutable_cache_control(ui_dist_dir: Path) -> None:
    """Serve a hashed asset with a long-lived, immutable cache header."""
    app = create_app(local_settings())
    async with _build_client(app) as client:
        response = await client.get("/assets/app-abc123.js")

    assert response.status_code == 200
    assert response.headers["Cache-Control"] == "public, max-age=31536000, immutable"


async def test_missing_asset_returns_404(ui_dist_dir: Path) -> None:
    """Report 404 for an asset that does not exist in the bundle."""
    app = create_app(local_settings())
    async with _build_client(app) as client:
        response = await client.get("/assets/missing.js")

    assert response.status_code == 404


async def test_root_file_is_served_directly(ui_dist_dir: Path) -> None:
    """Serve a plain root file such as the favicon by its own path."""
    app = create_app(local_settings())
    async with _build_client(app) as client:
        response = await client.get("/favicon.svg")

    assert response.status_code == 200
    assert response.text == "<svg></svg>"


async def test_spa_route_falls_back_to_index_html(ui_dist_dir: Path) -> None:
    """Serve the SPA shell for a client-side route with no matching file."""
    app = create_app(local_settings())
    async with _build_client(app) as client:
        response = await client.get("/some/spa/route")

    assert response.status_code == 200
    assert response.text == "<html>index</html>"


async def test_unmatched_api_path_returns_json_404(ui_dist_dir: Path) -> None:
    """Report a JSON 404 for an unmatched API path instead of the SPA shell."""
    app = create_app(local_settings())
    async with _build_client(app) as client:
        response = await client.get("/api/v1/does-not-exist")

    assert response.status_code == 404
    assert response.headers["content-type"].startswith("application/json")


async def test_without_bundle_root_returns_404(monkeypatch: pytest.MonkeyPatch) -> None:
    """Report 404 at the root URL when no UI bundle is packaged."""
    monkeypatch.setattr(ui, "_get_ui_dist_dir", lambda: None)
    app = create_app(local_settings())
    async with _build_client(app) as client:
        response = await client.get("/")

    assert response.status_code == 404


async def test_external_ui_redirects_to_dashboard() -> None:
    """Redirect the root URL and any other path to the configured dashboard."""
    settings = local_settings(EXTERNAL_UI=True, DASHBOARD_URL="https://ui.example.com")
    app = create_app(settings)
    async with _build_client(app) as client:
        root_response = await client.get("/")
        route_response = await client.get("/some/spa/route")

    assert root_response.status_code == 307
    assert root_response.headers["location"] == "https://ui.example.com"
    assert route_response.status_code == 307
    assert route_response.headers["location"] == "https://ui.example.com"


async def test_external_ui_still_404s_unmatched_api_paths() -> None:
    """Report 404 for an unmatched API path even under external UI mode."""
    settings = local_settings(EXTERNAL_UI=True, DASHBOARD_URL="https://ui.example.com")
    app = create_app(settings)
    async with _build_client(app) as client:
        response = await client.get("/api/v1/does-not-exist")

    assert response.status_code == 404


def test_external_ui_without_dashboard_url_is_rejected() -> None:
    """Reject EXTERNAL_UI without a configured dashboard URL."""
    with pytest.raises(pydantic.ValidationError, match="KITARU_SERVER_DASHBOARD_URL"):
        local_settings(EXTERNAL_UI=True)


async def test_info_reports_bundled_ui_version(ui_dist_dir: Path) -> None:
    """Report the bundled UI version on the info endpoint."""
    app = create_app(local_settings())
    async with _build_client(app) as client:
        response = await client.get("/api/v1/info")

    assert response.json()["ui_version"] == "kitaru-ui-v0.9.0"


async def test_info_reports_no_ui_version_in_external_mode(
    ui_dist_dir: Path,
) -> None:
    """Report no UI version on the info endpoint under external UI mode."""
    settings = local_settings(EXTERNAL_UI=True, DASHBOARD_URL="https://ui.example.com")
    app = create_app(settings)
    async with _build_client(app) as client:
        response = await client.get("/api/v1/info")

    assert response.json()["ui_version"] is None
