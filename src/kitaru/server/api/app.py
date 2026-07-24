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

import math
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from importlib.metadata import version
from typing import Any

from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from kitaru.server.adapters.auth.passwords import BcryptPasswordHasher
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.rest.routers import (
    accounts,
    agent_versions,
    agents,
    api_keys,
    auth,
    cohorts,
    experiment_runs,
    experiments,
    replays,
    secrets,
    sessions,
    tags,
    workers,
)
from kitaru.server.api import health
from kitaru.server.api.config import APISettings
from kitaru.server.application.services.account_service import AccountService
from kitaru.server.database.service import DatabaseService
from kitaru.server.domain.base import (
    ConflictError,
    DomainError,
    NotFoundError,
    ValidationError,
)


def _sanitize_non_finite(value: Any) -> Any:
    """Replace non-finite floats with their repr so the value serializes as JSON.

    Args:
        value: JSON-encodable value.

    Returns:
        Value with non-finite floats replaced.
    """
    if isinstance(value, float) and not math.isfinite(value):
        return repr(value)
    if isinstance(value, dict):
        return {key: _sanitize_non_finite(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_sanitize_non_finite(item) for item in value]
    return value


def _register_request_validation_exception_handler(app: FastAPI) -> None:
    """Register the JSON error response for request validation errors.

    Clients receive HTTP 422 with the standard FastAPI error body. Non-finite
    floats echoed from the request are replaced so the body serializes.

    Args:
        app: FastAPI application that will serve the v1 API.
    """

    @app.exception_handler(RequestValidationError)
    async def request_validation(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        _ = request
        errors = _sanitize_non_finite(jsonable_encoder(exc.errors()))
        return JSONResponse(status_code=422, content={"detail": errors})


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
        async for session in database.get_async_session():
            account_service = AccountService(
                repository=SQLAccountRepository(session),
                password_hasher=BcryptPasswordHasher(),
            )
            await account_service.ensure_account(
                settings.DEFAULT_ACCOUNT_NAME, settings.DEFAULT_ACCOUNT_PASSWORD
            )
            await session.commit()
        try:
            yield
        finally:
            await database.cleanup()

    app = FastAPI(
        title="Kitaru",
        version=version("kitaru"),
        lifespan=lifespan,
    )
    app.state.settings = settings
    _register_request_validation_exception_handler(app)
    _register_domain_exception_handlers(app)
    app.include_router(health.router, prefix="/health", tags=["health"])
    app.include_router(auth.router, prefix="/v1", tags=["auth"])
    app.include_router(accounts.router, prefix="/v1/accounts", tags=["accounts"])
    app.include_router(agents.router, prefix="/v1/agents", tags=["agents"])
    app.include_router(
        agent_versions.router, prefix="/v1/agent-versions", tags=["agent-versions"]
    )
    app.include_router(api_keys.router, prefix="/v1/api-keys", tags=["api-keys"])
    app.include_router(cohorts.router, prefix="/v1/cohorts", tags=["cohorts"])
    app.include_router(
        experiment_runs.router,
        prefix="/v1/experiment-runs",
        tags=["experiment-runs"],
    )
    app.include_router(
        experiments.router, prefix="/v1/experiments", tags=["experiments"]
    )
    app.include_router(replays.router, prefix="/v1/replays", tags=["replays"])
    app.include_router(secrets.router, prefix="/v1/secrets", tags=["secrets"])
    app.include_router(sessions.router, prefix="/v1/sessions", tags=["sessions"])
    app.include_router(tags.router, prefix="/v1/tags", tags=["tags"])
    app.include_router(workers.router, prefix="/v1/workers", tags=["workers"])
    return app
