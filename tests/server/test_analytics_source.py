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
"""Tests for the analytics source middleware."""

from collections.abc import AsyncGenerator

import httpx
import pytest
from fastapi import FastAPI

from conftest import asgi_api_client, local_settings
from kitaru.analytics.source import current_source
from kitaru.server.api.app import create_app


def create_app_with_source_probe() -> FastAPI:
    """Create the app with an extra route exposing the analytics source."""
    app = create_app(local_settings())

    @app.get("/probe-source")
    async def read_source() -> dict[str, str]:
        """Return the analytics source seen by the route handler."""
        return {"source": current_source.get().value}

    return app


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with the source probe route."""
    transport = httpx.ASGITransport(app=create_app_with_source_probe())
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def test_source_defaults_to_api(client: httpx.AsyncClient) -> None:
    """Attribute requests without a client header to the API source."""
    response = await client.get("/probe-source")
    assert response.json() == {"source": "kitaru-api"}


@pytest.mark.parametrize(
    ("header", "source"),
    [
        ("kitaru-ui/0.3.0", "kitaru-ui"),
        ("kitaru-typescript", "kitaru-typescript"),
    ],
)
async def test_source_parsed_from_client_header(
    client: httpx.AsyncClient, header: str, source: str
) -> None:
    """Attribute requests to the source named in the client header."""
    response = await client.get("/probe-source", headers={"X-Kitaru-Client": header})
    assert response.json() == {"source": source}


async def test_unknown_client_header_falls_back_to_api(
    client: httpx.AsyncClient,
) -> None:
    """Attribute requests with an unknown client header to the API source."""
    response = await client.get(
        "/probe-source", headers={"X-Kitaru-Client": "curl/8.0"}
    )
    assert response.json() == {"source": "kitaru-api"}


async def test_sdk_client_reports_python_source() -> None:
    """Attribute SDK requests to the Python source via the default headers."""
    api_client = asgi_api_client(create_app_with_source_probe())
    response = await api_client.request("GET", "/probe-source")
    assert response.json() == {"source": "kitaru-python"}
    await api_client.close()
