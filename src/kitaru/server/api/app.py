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
"""FastAPI application factory."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from importlib.metadata import version

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from kitaru.server.adapters.auth.control_plane import ControlPlaneClient
from kitaru.server.api import health
from kitaru.server.api.config import APISettings, AuthScheme
from kitaru.server.database.service import DatabaseService
from kitaru.server.domain.base import (
    ConflictError,
    DomainError,
    NotFoundError,
    ValidationError,
)


def _register_domain_exception_handlers(app: FastAPI) -> None:
    """Register JSON error responses for domain exceptions raised by routes.

    Clients receive HTTP 404 for ``NotFoundError``, 409 for ``ConflictError``,
    422 for ``ValidationError``, and 500 for other ``DomainError`` subclasses.
    Each body is ``{"detail": "<message>"}``.

    Args:
        app: FastAPI application that will serve the v1 API.
    """

    @app.exception_handler(NotFoundError)
    async def not_found(request: Request, exc: NotFoundError) -> JSONResponse:
        _ = request
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    @app.exception_handler(ConflictError)
    async def conflict(request: Request, exc: ConflictError) -> JSONResponse:
        _ = request
        return JSONResponse(status_code=409, content={"detail": str(exc)})

    @app.exception_handler(ValidationError)
    async def validation(request: Request, exc: ValidationError) -> JSONResponse:
        _ = request
        return JSONResponse(status_code=422, content={"detail": str(exc)})

    @app.exception_handler(DomainError)
    async def domain_error(request: Request, exc: DomainError) -> JSONResponse:
        _ = request
        return JSONResponse(status_code=500, content={"detail": str(exc)})


def create_app(settings: APISettings) -> FastAPI:
    """Create the FastAPI application.

    Args:
        settings: API server settings.

    Returns:
        Application instance.
    """

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
        database = DatabaseService(settings)
        if not settings.SKIP_DB_MIGRATION:
            await database.create_db_and_tables()
        app.state.database = database
        control_plane = None
        if settings.AUTH_SCHEME is AuthScheme.CONTROL_PLANE:
            control_plane = ControlPlaneClient(settings)
        app.state.control_plane = control_plane
        try:
            yield
        finally:
            if control_plane is not None:
                await control_plane.close()
            await database.cleanup()

    app = FastAPI(
        title="Kitaru",
        version=version("kitaru"),
        lifespan=lifespan,
    )
    app.state.settings = settings
    _register_domain_exception_handlers(app)
    app.include_router(health.router, prefix="/health", tags=["health"])
    return app
