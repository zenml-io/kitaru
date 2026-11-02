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
"""Tests for the request validation exception handler."""

from collections.abc import AsyncIterator
from typing import Annotated
from unittest import mock

import httpx
import pytest
from fastapi import FastAPI, Query

from conftest import local_settings
from kitaru.api_models.v1.base import JsonValue, RequestModel
from kitaru.api_models.v1.filter import FilterableListParams
from kitaru.server.api import ui
from kitaru.server.api.app import create_app


class _ProbeBody(RequestModel):
    """Body with a JSON value field that rejects non-finite floats."""

    value: JsonValue


def create_app_with_validation_probes() -> FastAPI:
    """Create the app with extra routes validating a filter query and a body."""
    # Build the app as if no UI bundle were packaged: the bundled UI's
    # catch-all route would shadow the probe routes added below.
    with mock.patch.object(ui, "_get_ui_dist_dir", return_value=None):
        app = create_app(local_settings())

    @app.get("/probe-filter")
    async def probe_filter(params: Annotated[FilterableListParams, Query()]) -> None:
        """Validate list params carrying a JSON-encoded filter."""
        _ = params

    @app.post("/probe-body")
    async def probe_body(body: _ProbeBody) -> None:
        """Validate a JSON body."""
        _ = body

    return app


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    """Yield a client for the app with validation probe routes."""
    transport = httpx.ASGITransport(app=create_app_with_validation_probes())
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


# 1e999 overflows to inf even when the parser rejects the Infinity literal.
@pytest.mark.parametrize(
    ("raw", "expected_input"),
    [("Infinity", "inf"), ("NaN", "nan"), ("1e999", "inf")],
)
async def test_non_finite_filter_maps_to_422(
    client: httpx.AsyncClient, raw: str, expected_input: str
) -> None:
    """Reject a filter that parses to a non-finite float with HTTP 422."""
    response = await client.get("/probe-filter", params={"filter": raw})
    assert response.status_code == 422
    inputs = [error["input"] for error in response.json()["detail"]]
    assert expected_input in inputs


async def test_non_finite_body_value_maps_to_422(client: httpx.AsyncClient) -> None:
    """Reject a JSON body whose value overflows to inf with HTTP 422."""
    response = await client.post(
        "/probe-body",
        content=b'{"value": {"nested": [1e999]}}',
        headers={"content-type": "application/json"},
    )
    assert response.status_code == 422
    [error] = response.json()["detail"]
    assert error["loc"] == ["body", "value"]
    assert error["input"] == {"nested": ["inf"]}


async def test_finite_errors_keep_default_shape(client: httpx.AsyncClient) -> None:
    """Report an ordinary validation error exactly as FastAPI does by default."""
    response = await client.get("/probe-filter", params={"size": "many"})
    assert response.status_code == 422
    [error] = response.json()["detail"]
    assert error["loc"] == ["query", "size"]
    assert error["type"] == "int_parsing"
    assert error["input"] == "many"
