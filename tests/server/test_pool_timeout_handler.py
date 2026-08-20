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
"""Tests for the pool timeout exception handler."""

from unittest import mock

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.exc import TimeoutError as SQLAlchemyTimeoutError

from conftest import local_settings
from kitaru.server.api import ui
from kitaru.server.api.app import create_app


def create_app_with_error_probes() -> FastAPI:
    """Create the app with extra routes raising database errors."""
    # Build the app as if no UI bundle were packaged: the bundled UI's
    # catch-all route would shadow the probe routes added below.
    with mock.patch.object(ui, "_get_ui_dist_dir", return_value=None):
        app = create_app(local_settings())

    @app.get("/probe-pool-timeout")
    async def raise_pool_timeout() -> None:
        """Raise a database error caused by an exhausted connection pool."""
        raise SQLAlchemyTimeoutError(
            "QueuePool limit of size 5 overflow 10 reached, connection timed out"
        )

    @app.get("/probe-sqlalchemy-error")
    async def raise_sqlalchemy_error() -> None:
        """Raise a database error unrelated to pool exhaustion."""
        raise SQLAlchemyError("generic database error")

    return app


async def test_pool_timeout_maps_to_503() -> None:
    """Turn an exhausted connection pool into HTTP 503."""
    transport = httpx.ASGITransport(app=create_app_with_error_probes())
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/probe-pool-timeout")
    assert response.status_code == 503
    assert response.json() == {"detail": "Database connection pool exhausted"}


async def test_other_sqlalchemy_errors_propagate() -> None:
    """Leave a database error without a pool timeout cause unhandled."""
    transport = httpx.ASGITransport(app=create_app_with_error_probes())
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        with pytest.raises(SQLAlchemyError):
            await client.get("/probe-sqlalchemy-error")
