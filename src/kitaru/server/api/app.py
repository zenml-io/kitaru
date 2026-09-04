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
from typing import Any

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy.exc import DBAPIError
from sqlalchemy.exc import TimeoutError as SQLAlchemyTimeoutError

from kitaru.analytics.client import AnalyticsClient
from kitaru.analytics.source import (
    AnalyticsSource,
    EventContext,
    current_event_context,
)
from kitaru.api_models.v1.base import ValidationErrorBody
from kitaru.api_models.v1.info import AuthScheme
from kitaru.headers import CLIENT_HEADER, SKILL_HEADER
from kitaru.server.adapters.auth.control_plane import ControlPlaneClient
from kitaru.server.adapters.db.errors import (
    is_connection_unavailable,
    is_deadlock,
    is_invalid_value,
)
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.plugin_repository import (
    SQLPluginRepository,
)
from kitaru.server.adapters.db.repositories.server_settings_repository import (
    SQLServerSettingsRepository,
)
from kitaru.server.adapters.rest.responses import error_responses
from kitaru.server.adapters.rest.routers import (
    accounts,
    agent_versions,
    agents,
    analyzers,
    annotations,
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
    insights,
    investigations,
    jobs,
    replays,
    secrets,
    service_accounts,
    session_runs,
    sessions,
    tags,
    tasks,
    ui,
    users,
    workers,
)
from kitaru.server.adapters.rest.routers.auth import TokenGrantError
from kitaru.server.api import health
from kitaru.server.api.bootstrap import (
    ensure_default_account,
    ensure_server_id,
    register_default_plugins,
)
from kitaru.server.api.config import APISettings
from kitaru.server.api.otel import configure_otel, instrument_engine, shutdown_otel
from kitaru.server.api.task_sweeper import start_task_sweeper, stop_task_sweeper
from kitaru.server.api.ui import get_ui_version, register_ui
from kitaru.server.application.services.server_analytics import (
    ServerAnalytics,
    build_analytics_context,
)
from kitaru.server.client_identity import parse_client_identity
from kitaru.server.database.service import DatabaseService
from kitaru.server.domain.base import (
    ConflictError,
    DomainError,
    ForbiddenError,
    NotFoundError,
    PayloadTooLargeError,
    QueryTimeoutError,
    UpgradeRequiredError,
    ValidationError,
)
from kitaru.server.ephemeral_worker_settings import EphemeralWorkerBackend

_COMMON_ERROR_RESPONSES = error_responses(401, 403, 503) | {
    422: {"model": ValidationErrorBody, "description": "Validation Error"}
}


def _register_domain_exception_handlers(app: FastAPI) -> None:
    """Register JSON error responses for domain exceptions raised by routes.

    Clients receive HTTP 403 for ``ForbiddenError``, 404 for ``NotFoundError``,
    409 for ``ConflictError``, 413 for ``PayloadTooLargeError``, 422 for
    ``ValidationError``, 426 for ``UpgradeRequiredError``, 503 for
    ``QueryTimeoutError``, and 500 for other ``DomainError`` subclasses. Each
    body is ``{"detail": "<message>"}``.

    Args:
        app: FastAPI application that will serve the v1 API.
    """

    @app.exception_handler(ForbiddenError)
    async def forbidden(request: Request, exc: ForbiddenError) -> JSONResponse:
        _ = request
        return JSONResponse(status_code=403, content={"detail": str(exc)})

    @app.exception_handler(NotFoundError)
    async def not_found(request: Request, exc: NotFoundError) -> JSONResponse:
        _ = request
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    @app.exception_handler(ConflictError)
    async def conflict(request: Request, exc: ConflictError) -> JSONResponse:
        _ = request
        return JSONResponse(status_code=409, content={"detail": str(exc)})

    @app.exception_handler(UpgradeRequiredError)
    async def upgrade_required(
        request: Request, exc: UpgradeRequiredError
    ) -> JSONResponse:
        _ = request
        return JSONResponse(status_code=426, content={"detail": str(exc)})

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


def _register_database_exception_handler(app: FastAPI) -> None:
    """Register the JSON error response for a retryable or invalid database error.

    A deadlock-canceled transaction and a lost or unavailable connection both
    yield HTTP 503. A value the database cannot store yields HTTP 422. Each
    of these responses carries a ``detail`` message. Any other database
    error propagates unchanged.

    Args:
        app: FastAPI application that will serve the v1 API.
    """

    @app.exception_handler(DBAPIError)
    async def database_error(request: Request, exc: DBAPIError) -> JSONResponse:
        _ = request
        if is_deadlock(exc):
            return JSONResponse(
                status_code=503, content={"detail": "Deadlock detected"}
            )
        if is_connection_unavailable(exc):
            return JSONResponse(
                status_code=503, content={"detail": "Database connection unavailable"}
            )
        if is_invalid_value(exc):
            return JSONResponse(
                status_code=422,
                content={
                    "detail": "Request contained a value the database cannot store"
                },
            )
        raise exc


def _register_pool_timeout_exception_handler(app: FastAPI) -> None:
    """Register the JSON error response for an exhausted connection pool.

    Clients receive HTTP 503 with the usual ``detail`` message.

    Args:
        app: FastAPI application that will serve the v1 API.
    """

    @app.exception_handler(SQLAlchemyTimeoutError)
    async def pool_timeout(
        request: Request, exc: SQLAlchemyTimeoutError
    ) -> JSONResponse:
        _ = request
        _ = exc
        return JSONResponse(
            status_code=503, content={"detail": "Database connection pool exhausted"}
        )


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


async def _set_event_context(
    request: Request, call_next: Callable[[Request], Awaitable[Response]]
) -> Response:
    """Set the analytics event context for the request from the client headers.

    Args:
        request: Incoming request.
        call_next: Next request handler.

    Returns:
        HTTP response.
    """
    identity = parse_client_identity(request.headers.get(CLIENT_HEADER, ""))
    source = identity.source if identity else AnalyticsSource.API
    properties: dict[str, Any] = {}
    if identity is not None and identity.version is not None:
        properties["client_version"] = f"{source.value}/{identity.version}"
    if identity is not None and identity.analytics_id is not None:
        properties["client_id"] = str(identity.analytics_id)
    skill = request.headers.get(SKILL_HEADER)
    if skill:
        properties["skill"] = skill
    token = current_event_context.set(
        EventContext(source=source, properties=properties)
    )
    try:
        return await call_next(request)
    finally:
        current_event_context.reset(token)


def create_app(settings: APISettings) -> FastAPI:
    """Create the FastAPI application.

    Args:
        settings: API server settings.

    Returns:
        Application instance.
    """
    analytics = AnalyticsClient(
        enabled=settings.ANALYTICS_OPT_IN, debug=settings.ANALYTICS_DEBUG
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
        database = DatabaseService(settings)
        instrument_engine(database.engine)
        if not settings.SKIP_DB_MIGRATION:
            await database.create_db_and_tables()
        app.state.database = database
        if settings.AUTH_SCHEME is AuthScheme.CONTROL_PLANE:
            app.state.control_plane_client = ControlPlaneClient(settings)
        if settings.BLOB_STORAGE.s3 is not None:
            from kitaru.server.adapters.blobstore.s3 import S3BlobDataStore

            app.state.s3_blob_data_store = S3BlobDataStore(settings.BLOB_STORAGE.s3)
        if settings.EPHEMERAL_WORKER.backend is EphemeralWorkerBackend.MODAL:
            from kitaru.server.adapters.ephemeral_workers.modal import (
                ModalEphemeralWorkers,
            )

            app.state.ephemeral_workers = ModalEphemeralWorkers(
                settings.EPHEMERAL_WORKER
            )
        async for session in database.get_async_session():
            server_id = await ensure_server_id(
                SQLServerSettingsRepository(session), settings.SERVER_ID
            )
            await session.commit()
            app.state.server_id = server_id
            analytics.set_context(
                build_analytics_context(
                    server_id=server_id,
                    auth_scheme=settings.AUTH_SCHEME,
                    organization_id=settings.ORGANIZATION_ID,
                    organization_name=settings.ORGANIZATION_NAME,
                    workspace_name=settings.WORKSPACE_NAME,
                )
            )
        # The control plane owns every account under its auth scheme, so there
        # is no local default account to fall back on.
        if settings.AUTH_SCHEME is not AuthScheme.CONTROL_PLANE:
            async for session in database.get_async_session():
                await ensure_default_account(
                    SQLAccountRepository(session),
                    settings.DEFAULT_ACCOUNT_NAME,
                    settings.DEFAULT_ACCOUNT_PASSWORD,
                    ServerAnalytics(client=analytics, session=session),
                )
                await session.commit()
        async for session in database.get_async_session():
            await register_default_plugins(SQLPluginRepository(session))
            await session.commit()
        sweep_task = start_task_sweeper(database, settings, analytics)
        try:
            yield
        finally:
            await stop_task_sweeper(sweep_task)
            await analytics.aclose()
            if app.state.control_plane_client is not None:
                await app.state.control_plane_client.close()
            await database.cleanup()
            # Last, so telemetry produced during the steps above still
            # gets exported and flushed.
            shutdown_otel()

    cors_allow_origins = settings.get_cors_allow_origins()
    app = FastAPI(
        title="Kitaru",
        version=version("kitaru"),
        lifespan=lifespan,
        root_path=settings.ROOT_URL_PATH,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_allow_origins,
        allow_credentials="*" not in cors_allow_origins,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.state.settings = settings
    app.state.analytics = analytics
    # Replaced with a live client at startup under the control plane scheme.
    app.state.control_plane_client = None
    # Replaced with the persisted server id at startup.
    app.state.server_id = None
    # Replaced with a live store at startup when S3 blob storage is configured.
    app.state.s3_blob_data_store = None
    # Replaced with a live backend at startup when an ephemeral worker backend
    # is configured.
    app.state.ephemeral_workers = None
    _register_domain_exception_handlers(app)
    _register_database_exception_handler(app)
    _register_pool_timeout_exception_handler(app)
    _register_token_grant_exception_handler(app)
    app.middleware("http")(_set_event_context)
    # FastAPIInstrumentor registers its middleware last, which makes the
    # OTel span the outermost layer, so this call must come after every
    # other middleware registration.
    configure_otel(settings, app)
    app.include_router(health.router, prefix="/health", tags=["health"])
    app.include_router(info.router, prefix="/api/v1/info", tags=["info"])
    app.include_router(auth.router, prefix="/api/v1", tags=["auth"])
    app.include_router(
        accounts.router,
        prefix="/api/v1/accounts",
        tags=["accounts"],
        responses=_COMMON_ERROR_RESPONSES,
    )
    app.include_router(
        agents.router,
        prefix="/api/v1/agents",
        tags=["agents"],
        responses=_COMMON_ERROR_RESPONSES,
    )
    app.include_router(
        agent_versions.router,
        prefix="/api/v1/agent-versions",
        tags=["agent-versions"],
        responses=_COMMON_ERROR_RESPONSES,
    )
    app.include_router(
        analyzers.router,
        prefix="/api/v1/analyzers",
        tags=["analyzers"],
        responses=_COMMON_ERROR_RESPONSES,
    )
    app.include_router(
        annotations.router,
        prefix="/api/v1/annotations",
        tags=["annotations"],
        responses=_COMMON_ERROR_RESPONSES,
    )
    app.include_router(
        api_keys.router,
        prefix="/api/v1/api-keys",
        tags=["api-keys"],
        responses=_COMMON_ERROR_RESPONSES,
    )
    app.include_router(
        blobs.router,
        prefix="/api/v1/blobs",
        tags=["blobs"],
        responses=_COMMON_ERROR_RESPONSES,
    )
    app.include_router(
        cohorts.router,
        prefix="/api/v1/cohorts",
        tags=["cohorts"],
        responses=_COMMON_ERROR_RESPONSES,
    )
    app.include_router(
        cohort_versions.router,
        prefix="/api/v1/cohort-versions",
        tags=["cohort-versions"],
        responses=_COMMON_ERROR_RESPONSES,
    )
    app.include_router(
        devices.router,
        prefix="/api/v1/devices",
        tags=["devices"],
        responses=_COMMON_ERROR_RESPONSES,
    )
    app.include_router(
        evaluations.router,
        prefix="/api/v1/evaluations",
        tags=["evaluations"],
        responses=_COMMON_ERROR_RESPONSES,
    )
    app.include_router(
        evaluators.router,
        prefix="/api/v1/evaluators",
        tags=["evaluators"],
        responses=_COMMON_ERROR_RESPONSES,
    )
    app.include_router(
        experiments.router,
        prefix="/api/v1/experiments",
        tags=["experiments"],
        responses=_COMMON_ERROR_RESPONSES,
    )
    app.include_router(
        experiment_runs.router,
        prefix="/api/v1/experiment-runs",
        tags=["experiment-runs"],
        responses=_COMMON_ERROR_RESPONSES,
    )
    app.include_router(
        importers.router,
        prefix="/api/v1/importers",
        tags=["importers"],
        responses=_COMMON_ERROR_RESPONSES,
    )
    app.include_router(
        imports.router,
        prefix="/api/v1/imports",
        tags=["imports"],
        responses=_COMMON_ERROR_RESPONSES,
    )
    app.include_router(
        insights.router,
        prefix="/api/v1/insights",
        tags=["insights"],
        responses=_COMMON_ERROR_RESPONSES,
    )
    app.include_router(
        investigations.router,
        prefix="/api/v1/investigations",
        tags=["investigations"],
        responses=_COMMON_ERROR_RESPONSES,
    )
    app.include_router(
        jobs.router,
        prefix="/api/v1/jobs",
        tags=["jobs"],
        responses=_COMMON_ERROR_RESPONSES,
    )
    app.include_router(
        replays.router,
        prefix="/api/v1/replays",
        tags=["replays"],
        responses=_COMMON_ERROR_RESPONSES,
    )
    app.include_router(
        secrets.router,
        prefix="/api/v1/secrets",
        tags=["secrets"],
        responses=_COMMON_ERROR_RESPONSES,
    )
    app.include_router(
        service_accounts.router,
        prefix="/api/v1/service-accounts",
        tags=["service-accounts"],
        responses=_COMMON_ERROR_RESPONSES,
    )
    app.include_router(
        session_runs.router,
        prefix="/api/v1/session-runs",
        tags=["session-runs"],
        responses=_COMMON_ERROR_RESPONSES,
    )
    app.include_router(
        sessions.router,
        prefix="/api/v1/sessions",
        tags=["sessions"],
        responses=_COMMON_ERROR_RESPONSES,
    )
    app.include_router(
        tags.router,
        prefix="/api/v1/tags",
        tags=["tags"],
        responses=_COMMON_ERROR_RESPONSES,
    )
    app.include_router(
        tasks.router,
        prefix="/api/v1/tasks",
        tags=["tasks"],
        responses=_COMMON_ERROR_RESPONSES,
    )
    app.include_router(
        ui.router,
        prefix="/api/v1/ui",
        tags=["ui"],
        responses=_COMMON_ERROR_RESPONSES,
    )
    app.include_router(
        users.router,
        prefix="/api/v1/users",
        tags=["users"],
        responses=_COMMON_ERROR_RESPONSES,
    )
    app.include_router(
        workers.router,
        prefix="/api/v1/workers",
        tags=["workers"],
        responses=_COMMON_ERROR_RESPONSES,
    )
    app.state.ui_version = None if settings.EXTERNAL_UI else get_ui_version()
    # Register last so the UI catch-all only sees paths no API route matched.
    register_ui(app, settings)
    return app
