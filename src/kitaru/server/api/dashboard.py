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
"""Serve the dashboard bundled in the Kitaru package."""

from pathlib import Path

from fastapi import FastAPI
from starlette.exceptions import HTTPException
from starlette.responses import Response
from starlette.routing import Match, Mount, get_route_path
from starlette.staticfiles import StaticFiles
from starlette.types import Scope

_BACKEND_PATH_PREFIXES = frozenset({"docs", "health", "openapi.json", "redoc", "v1"})


class _DashboardMount(Mount):
    """Match dashboard paths without intercepting backend routing."""

    @staticmethod
    def _is_backend_path(path: str) -> bool:
        """Return whether a path belongs to the server API.

        Args:
            path: Requested application path.

        Returns:
            Whether the path uses a reserved backend prefix.
        """
        first_segment = path.lstrip("/").partition("/")[0]
        return first_segment in _BACKEND_PATH_PREFIXES

    def matches(self, scope: Scope) -> tuple[Match, Scope]:
        """Return no match for paths reserved by the backend.

        Args:
            scope: ASGI request scope.

        Returns:
            Route match and child scope.
        """
        if scope["type"] != "http":
            return Match.NONE, {}
        if self._is_backend_path(get_route_path(scope)):
            return Match.NONE, {}
        return super().matches(scope)


class _DashboardFiles(StaticFiles):
    """Serve static files with an index fallback for dashboard routes."""

    @staticmethod
    def _should_serve_index(path: str) -> bool:
        """Return whether a missing path is a dashboard client route.

        Args:
            path: Requested path relative to the static mount.

        Returns:
            Whether the dashboard entrypoint should handle the request.
        """
        return not Path(path).suffix

    async def get_response(self, path: str, scope: Scope) -> Response:
        """Return a static asset or the dashboard entrypoint.

        Args:
            path: Requested path relative to the static mount.
            scope: ASGI request scope.

        Returns:
            Static file response.

        Raises:
            HTTPException: The requested path is not a dashboard route or asset.
        """
        try:
            response = await super().get_response(path, scope)
        except HTTPException as error:
            if error.status_code != 404 or not self._should_serve_index(path):
                raise
        else:
            if response.status_code != 404:
                return response
            if not self._should_serve_index(path):
                return response
        return await super().get_response("index.html", scope)


def _get_packaged_ui_dist_path() -> Path | None:
    """Get the dashboard directory installed with Kitaru.

    Returns:
        Dashboard directory when the package contains one, otherwise ``None``.
    """
    path = Path(__file__).parents[2] / "_ui" / "dist"
    return path if (path / "index.html").is_file() else None


def register_dashboard(app: FastAPI) -> None:
    """Register packaged dashboard routes after all backend routes.

    Args:
        app: FastAPI application to add dashboard routes to.
    """
    path = _get_packaged_ui_dist_path()
    if path is None:
        return
    app.router.routes.append(
        _DashboardMount(
            "/",
            app=_DashboardFiles(directory=path, html=True),
            name="dashboard",
        )
    )
