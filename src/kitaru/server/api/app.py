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

from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
from importlib.metadata import version

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

from kitaru.analytics.client import AnalyticsClient
from kitaru.analytics.source import (
    CLIENT_HEADER,
    AnalyticsSource,
    current_source,
    parse_client_header,
)
from kitaru.api_models.v1.info import AuthScheme
from kitaru.server.adapters.auth.control_plane import ControlPlaneClient
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
    blobs,
    cohort_versions,
    cohorts,
    devices,
    evaluations,
    evaluators,
    experiment_runs,
    experiments,
    importers,
    imports,
    info,
    jobs,
    replays,
    secrets,
    session_runs,
    sessions,
    tags,
    tasks,
    workers,
)
from kitaru.server.adapters.rest.routers.auth import TokenGrantError
from kitaru.server.api import health
from kitaru.server.api.config import APISettings
from kitaru.server.application.services.account_service import AccountService
from kitaru.server.database.service import DatabaseService
from kitaru.server.domain.base import (
    ConflictError,
    DomainError,
    NotFoundError,
    PayloadTooLargeError,
    QueryTimeoutError,
    ValidationError,
)


def _register_domain_exception_handlers(app: FastAPI) -> None:
    """Register JSON error responses for domain exceptions raised by routes.

    Clients receive HTTP 404 for ``NotFoundError``, 409 for ``ConflictError``,
    413 for ``PayloadTooLargeError``, 422 for ``ValidationError``, 503 for
    ``QueryTimeoutError``, and 500 for other ``DomainError`` subclasses. Each
    body is ``{"detail": "<message>"}``.

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

    @app.exception_handler(PayloadTooLargeError)
    async def payload_too_large(
        request: Request, exc: PayloadTooLargeError
    ) -> JSONResponse:
        _ = request
        return JSONResponse(status_code=413, content={"detail": str(exc)})

    @app.exception_handler(ValidationError)
    async def validation(request: Request, exc: ValidationError) -> JSONResponse:
        _ = request
        return JSONResponse(status_code=422, content={"detail": str(exc)})

    @app.exception_handler(QueryTimeoutError)
    async def query_timeout(request: Request, exc: QueryTimeoutError) -> JSONResponse:
        _ = request
        return JSONResponse(status_code=503, content={"detail": str(exc)})

    @app.exception_handler(DomainError)
    async def domain_error(request: Request, exc: DomainError) -> JSONResponse:
        _ = request
        return JSONResponse(status_code=500, content={"detail": str(exc)})


def _register_token_grant_exception_handler(app: FastAPI) -> None:
    """Register the OAuth 2.0 error response for a failed token grant.

    Clients receive HTTP 400 with an ``error`` code alongside the usual
    ``detail`` message.

    Args:
        app: FastAPI application that will serve the v1 API.
    """

    @app.exception_handler(TokenGrantError)
    async def token_grant_error(request: Request, exc: TokenGrantError) -> JSONResponse:
        _ = request
        return JSONResponse(
            status_code=400, content=exc.to_response().model_dump(mode="json")
        )


async def _set_analytics_source(
    request: Request, call_next: Callable[[Request], Awaitable[Response]]
) -> Response:
    """Set the analytics source for the request from the client header.

    Args:
        request: Incoming request.
        call_next: Next request handler.

    Returns:
        HTTP response.
    """
    source = parse_client_header(request.headers.get(CLIENT_HEADER, ""))
    token = current_source.set(source or AnalyticsSource.API)
    try:
        return await call_next(request)
    finally:
        current_source.reset(token)


def create_app(settings: APISettings) -> FastAPI:
    """Create the FastAPI application.

    Args:
        settings: API server settings.

    Returns:
        Application instance.
    """
    analytics = AnalyticsClient(enabled=settings.ANALYTICS_OPT_IN)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
        database = DatabaseService(settings)
        if not settings.SKIP_DB_MIGRATION:
            await database.create_db_and_tables()
        app.state.database = database
        if settings.AUTH_SCHEME is AuthScheme.CONTROL_PLANE:
            app.state.control_plane_client = ControlPlaneClient(settings)
        # The control plane owns every account under its auth scheme, so there
        # is no local default account to fall back on.
        if settings.AUTH_SCHEME is not AuthScheme.CONTROL_PLANE:
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
            await analytics.aclose()
            if app.state.control_plane_client is not None:
                await app.state.control_plane_client.close()
            await database.cleanup()

    app = FastAPI(
        title="Kitaru",
        version=version("kitaru"),
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.state.analytics = analytics
    # Replaced with a live client at startup under the control plane scheme.
    app.state.control_plane_client = None
    _register_domain_exception_handlers(app)
    _register_token_grant_exception_handler(app)
    app.middleware("http")(_set_analytics_source)
    app.include_router(health.router, prefix="/health", tags=["health"])
    app.include_router(info.router, prefix="/v1/info", tags=["info"])
    app.include_router(auth.router, prefix="/v1", tags=["auth"])
    app.include_router(accounts.router, prefix="/v1/accounts", tags=["accounts"])
    app.include_router(agents.router, prefix="/v1/agents", tags=["agents"])
    app.include_router(
        agent_versions.router, prefix="/v1/agent-versions", tags=["agent-versions"]
    )
    app.include_router(api_keys.router, prefix="/v1/api-keys", tags=["api-keys"])
    app.include_router(blobs.router, prefix="/v1/blobs", tags=["blobs"])
    app.include_router(cohorts.router, prefix="/v1/cohorts", tags=["cohorts"])
    app.include_router(
        cohort_versions.router, prefix="/v1/cohort-versions", tags=["cohort-versions"]
    )
    app.include_router(devices.router, prefix="/v1/devices", tags=["devices"])
    app.include_router(
        evaluations.router, prefix="/v1/evaluations", tags=["evaluations"]
    )
    app.include_router(evaluators.router, prefix="/v1/evaluators", tags=["evaluators"])
    app.include_router(
        experiments.router, prefix="/v1/experiments", tags=["experiments"]
    )
    app.include_router(
        experiment_runs.router,
        prefix="/v1/experiment-runs",
        tags=["experiment-runs"],
    )
    app.include_router(importers.router, prefix="/v1/importers", tags=["importers"])
    app.include_router(imports.router, prefix="/v1/imports", tags=["imports"])
    app.include_router(jobs.router, prefix="/v1/jobs", tags=["jobs"])
    app.include_router(replays.router, prefix="/v1/replays", tags=["replays"])
    app.include_router(secrets.router, prefix="/v1/secrets", tags=["secrets"])
    app.include_router(
        session_runs.router, prefix="/v1/session-runs", tags=["session-runs"]
    )
    app.include_router(sessions.router, prefix="/v1/sessions", tags=["sessions"])
    app.include_router(tags.router, prefix="/v1/tags", tags=["tags"])
    app.include_router(tasks.router, prefix="/v1/tasks", tags=["tasks"])
    app.include_router(workers.router, prefix="/v1/workers", tags=["workers"])
    return app
