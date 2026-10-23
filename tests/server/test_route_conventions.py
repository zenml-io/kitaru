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
"""Conventions every route registered on the application must follow."""

from collections.abc import Callable
from typing import Any

import pytest
from fastapi.dependencies.models import Dependant
from fastapi.routing import APIRoute, iter_route_contexts

from kitaru.server.adapters.rest.dependencies import (
    authorize,
    authorize_task_only,
    authorize_with_task,
    authorize_with_worker,
    authorize_with_worker_or_task,
    authorize_worker_only,
)
from kitaru.server.adapters.rest.route import KitaruAPIRoute
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings

AUTHORIZE_DEPENDENCIES: set[Callable[..., Any]] = {
    authorize,
    authorize_task_only,
    authorize_with_task,
    authorize_with_worker,
    authorize_with_worker_or_task,
    authorize_worker_only,
}

# Only KitaruAPIRoute commits the request session, so a plain route that
# writes would answer success and then roll the write back.
PLAIN_ROUTES: dict[str, str] = {
    "/health": "Probe that only pings the database.",
    "/health/live": "Probe with no database access.",
    "/api/v1/info": "Static server metadata with no database session.",
}

PUBLIC_ROUTES: dict[str, str] = {
    "/health": "Readiness probe.",
    "/health/live": "Liveness probe.",
    "/api/v1/info": "Server metadata clients read before logging in.",
    "/api/v1/login": "Issues the credential.",
    "/api/v1/logout": "Only clears the auth cookie.",
    "/api/v1/device_authorization": "Starts the device login flow.",
    "/api/v1/users/{account_id}/activate": "Authenticated by the activation token.",
}

# Guards against a FastAPI change that hides nested routes from the walk below,
# which would make every check here pass without inspecting anything.
MIN_ROUTE_COUNT = 150


@pytest.fixture(scope="module")
def routes() -> list[tuple[str, APIRoute]]:
    """Return every API route on the application with its full path."""
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )
    # app.routes holds included-router wrappers; only the walk reaches APIRoutes.
    found = [
        (context.path or route.path, route)
        for context in iter_route_contexts(app.routes)
        if isinstance(route := context.original_route, APIRoute)
    ]
    assert len(found) >= MIN_ROUTE_COUNT
    return found


def _get_dependency_calls(dependant: Dependant) -> set[Callable[..., Any]]:
    calls: set[Callable[..., Any]] = set()
    for dependency in dependant.dependencies:
        if dependency.call is not None:
            calls.add(dependency.call)
        calls |= _get_dependency_calls(dependency)
    return calls


def test_routes_commit_through_kitaru_route(
    routes: list[tuple[str, APIRoute]],
) -> None:
    """Build every route with KitaruAPIRoute unless it is listed as plain."""
    plain = {path for path, route in routes if not isinstance(route, KitaruAPIRoute)}
    assert plain == set(PLAIN_ROUTES), (
        "Build the router with APIRouter(route_class=KitaruAPIRoute) from "
        "kitaru.server.adapters.rest.route, or add the path to PLAIN_ROUTES "
        "with the reason it needs no commit."
    )


def test_routes_require_authorization(routes: list[tuple[str, APIRoute]]) -> None:
    """Depend on an authorize variant unless the route is listed as public."""
    public = {
        path
        for path, route in routes
        if not _get_dependency_calls(route.dependant) & AUTHORIZE_DEPENDENCIES
    }
    assert public == set(PUBLIC_ROUTES), (
        "Add an authorize dependency from kitaru.server.adapters.rest.dependencies "
        "and pass actor=actor to the service, or add the path to PUBLIC_ROUTES "
        "with the reason it is reachable without credentials."
    )
