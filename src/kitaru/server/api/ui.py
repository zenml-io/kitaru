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
"""Bundled UI serving."""

import importlib.resources
import json
import logging
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, RedirectResponse, Response
from starlette.staticfiles import StaticFiles
from starlette.types import Scope

from kitaru.server.api.config import APISettings

logger = logging.getLogger(__name__)

_RESERVED_PREFIXES = ("api", "health", "docs", "redoc", "openapi.json")


def _is_reserved_path(path: str) -> bool:
    """Check whether a path belongs to a reserved API prefix.

    Args:
        path: Request path without a leading slash.

    Returns:
        Whether the path is the prefix itself or nested under it.
    """
    return any(
        path == prefix or path.startswith(f"{prefix}/") for prefix in _RESERVED_PREFIXES
    )


def _get_ui_dist_dir() -> Path | None:
    """Resolve the bundled UI build directory.

    Returns:
        Build directory, or None when no UI bundle is packaged.
    """
    dist_dir = Path(str(importlib.resources.files("kitaru") / "_ui" / "dist"))
    if (dist_dir / "index.html").is_file():
        return dist_dir
    return None


def get_ui_version() -> str | None:
    """Read the version tag of the bundled UI.

    Returns:
        The manifest ``tag`` value, or None when no UI bundle is packaged.
    """
    dist_dir = _get_ui_dist_dir()
    if dist_dir is None:
        return None
    manifest_path = dist_dir.parent / "bundle_manifest.json"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    tag = manifest.get("tag")
    return tag if isinstance(tag, str) else None


class _ImmutableStaticFiles(StaticFiles):
    """Static file server that marks its responses as immutable."""

    async def get_response(self, path: str, scope: Scope) -> Response:
        """Serve a static file with a long-lived, immutable cache header.

        Args:
            path: Path relative to the served directory.
            scope: ASGI request scope.

        Returns:
            HTTP response for the requested file.
        """
        response = await super().get_response(path, scope)
        if response.status_code == 200:
            # Asset filenames are content-hashed, so a served asset never
            # changes without also changing its URL.
            response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
        return response


def _register_external_ui_routes(app: FastAPI, settings: APISettings) -> None:
    """Register a redirect to the configured external dashboard.

    Args:
        app: FastAPI application.
        settings: API server settings.
    """

    @app.get("/{path:path}", include_in_schema=False)
    async def redirect_to_dashboard(path: str) -> RedirectResponse:
        """Redirect any non-API path to the external dashboard.

        Args:
            path: Request path without a leading slash.

        Raises:
            HTTPException: The path belongs to a reserved API prefix.

        Returns:
            Redirect to the configured dashboard URL.
        """
        if _is_reserved_path(path):
            raise HTTPException(status_code=404)
        return RedirectResponse(settings.DASHBOARD_URL)


def _register_bundled_ui_routes(app: FastAPI, dist_dir: Path) -> None:
    """Register routes serving the bundled UI build.

    Args:
        app: FastAPI application.
        dist_dir: Bundled UI build directory.
    """
    app.mount(
        "/assets",
        _ImmutableStaticFiles(directory=dist_dir / "assets", check_dir=False),
    )
    root_files = {
        entry.name
        for entry in dist_dir.iterdir()
        if entry.is_file() and entry.name != "index.html"
    }

    @app.get("/{path:path}", include_in_schema=False)
    async def serve_ui(path: str) -> Response:
        """Serve a bundled UI file, falling back to the SPA shell.

        Args:
            path: Request path without a leading slash.

        Raises:
            HTTPException: The path belongs to a reserved API prefix.

        Returns:
            The requested file, or the SPA shell for any other route.
        """
        if _is_reserved_path(path):
            raise HTTPException(status_code=404)
        if path in root_files:
            return FileResponse(dist_dir / path)
        return FileResponse(
            dist_dir / "index.html", headers={"Cache-Control": "no-cache"}
        )


def register_ui(app: FastAPI, settings: APISettings) -> None:
    """Register the bundled UI or external dashboard redirect routes.

    Args:
        app: FastAPI application.
        settings: API server settings.
    """
    if settings.EXTERNAL_UI:
        _register_external_ui_routes(app, settings)
        return
    dist_dir = _get_ui_dist_dir()
    if dist_dir is None:
        logger.info("No UI bundle found, UI serving is disabled.")
        return
    _register_bundled_ui_routes(app, dist_dir)
