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
"""Tests for the database exception handler."""

import httpx
import pytest
from asyncpg.exceptions import (
    CannotConnectNowError,
    CharacterNotInRepertoireError,
    ConnectionDoesNotExistError,
    DeadlockDetectedError,
)
from fastapi import FastAPI
from sqlalchemy.exc import DBAPIError

from conftest import local_settings
from kitaru.server.api.app import _register_database_exception_handler, create_app


def _dbapi_error(cause: BaseException) -> DBAPIError:
    """Build a database error whose driver cause is a given error.

    Args:
        cause: Driver error to place in the cause chain.

    Returns:
        A database error wrapping the cause.
    """
    adapter_error = Exception("adapter error")
    adapter_error.__cause__ = cause
    return DBAPIError("UPDATE task", None, adapter_error)


def _handler_app() -> FastAPI:
    """Build a bare app with the database exception handler and probe routes.

    Returns:
        An app whose probe routes raise database errors.
    """
    app = FastAPI()
    _register_database_exception_handler(app)

    @app.get("/probe-deadlock")
    async def raise_deadlock() -> None:
        """Raise a database error caused by a deadlock."""
        raise _dbapi_error(DeadlockDetectedError("deadlock detected"))

    @app.get("/probe-connection")
    async def raise_connection() -> None:
        """Raise a database error caused by a dropped connection."""
        raise _dbapi_error(ConnectionDoesNotExistError("connection gone"))

    @app.get("/probe-recovery")
    async def raise_recovery() -> None:
        """Raise a database error caused by the database being in recovery."""
        raise _dbapi_error(CannotConnectNowError("in recovery"))

    @app.get("/probe-invalid-value")
    async def raise_invalid_value() -> None:
        """Raise a database error caused by a value the database cannot store."""
        raise _dbapi_error(CharacterNotInRepertoireError("invalid byte sequence"))

    @app.get("/probe-other")
    async def raise_other() -> None:
        """Raise a database error with no recognized cause."""
        raise _dbapi_error(Exception("unrelated"))

    return app


async def _get(path: str) -> httpx.Response:
    """Call a probe route on the handler app.

    Args:
        path: Route to call.

    Returns:
        The route's response.
    """
    transport = httpx.ASGITransport(app=_handler_app())
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        return await client.get(path)


async def test_deadlock_maps_to_503() -> None:
    """Turn a transaction killed by a deadlock into HTTP 503."""
    response = await _get("/probe-deadlock")
    assert response.status_code == 503
    assert response.json() == {"detail": "Deadlock detected"}


async def test_dropped_connection_maps_to_503() -> None:
    """Turn a dropped database connection into HTTP 503."""
    response = await _get("/probe-connection")
    assert response.status_code == 503
    assert response.json() == {"detail": "Database connection unavailable"}


async def test_database_in_recovery_maps_to_503() -> None:
    """Turn a database refusing connections during recovery into HTTP 503."""
    response = await _get("/probe-recovery")
    assert response.status_code == 503
    assert response.json() == {"detail": "Database connection unavailable"}


async def test_invalid_value_maps_to_422() -> None:
    """Turn a value the database cannot store into HTTP 422."""
    response = await _get("/probe-invalid-value")
    assert response.status_code == 422
    assert response.json() == {
        "detail": "Request contained a value the database cannot store"
    }


async def test_other_database_errors_propagate() -> None:
    """Leave a database error without a recognized cause unhandled."""
    transport = httpx.ASGITransport(app=_handler_app())
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        with pytest.raises(DBAPIError):
            await client.get("/probe-other")


def test_handler_registered_on_app() -> None:
    """Wire the database exception handler into the application."""
    app = create_app(local_settings())
    assert DBAPIError in app.exception_handlers
