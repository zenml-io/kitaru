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
"""Tests for the deadlock exception handler."""

import httpx
import pytest
from asyncpg.exceptions import DeadlockDetectedError
from fastapi import FastAPI
from sqlalchemy.exc import DBAPIError

from conftest import local_settings
from kitaru.server.api.app import create_app


def _deadlock_error() -> DBAPIError:
    """Build a database error chained like a driver-reported deadlock."""
    adapter_error = Exception("adapter error")
    adapter_error.__cause__ = DeadlockDetectedError("deadlock detected")
    return DBAPIError("UPDATE task", None, adapter_error)


def create_app_with_error_probes() -> FastAPI:
    """Create the app with extra routes raising database errors."""
    app = create_app(local_settings())

    @app.get("/probe-deadlock")
    async def raise_deadlock() -> None:
        """Raise a database error caused by a deadlock."""
        raise _deadlock_error()

    @app.get("/probe-dbapi-error")
    async def raise_dbapi_error() -> None:
        """Raise a database error without a deadlock cause."""
        raise DBAPIError("UPDATE task", None, Exception("adapter error"))

    return app


async def test_deadlock_maps_to_503() -> None:
    """Turn a transaction killed by a deadlock into HTTP 503."""
    transport = httpx.ASGITransport(app=create_app_with_error_probes())
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/probe-deadlock")
    assert response.status_code == 503
    assert response.json() == {"detail": "Deadlock detected"}


async def test_other_database_errors_propagate() -> None:
    """Leave a database error without a deadlock cause unhandled."""
    transport = httpx.ASGITransport(app=create_app_with_error_probes())
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        with pytest.raises(DBAPIError):
            await client.get("/probe-dbapi-error")
