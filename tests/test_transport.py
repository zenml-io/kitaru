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
"""Tests for the retrying HTTP transport."""

import asyncio
from collections.abc import AsyncIterator

import httpx
import pytest

from kitaru.transport import build_async_client

_PROXY_VARIABLES = (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
    "no_proxy",
)


@pytest.fixture
async def proxy_server(
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[list[str]]:
    """Run a local HTTP proxy and point HTTP_PROXY at it.

    Args:
        monkeypatch: Pytest monkeypatch fixture.

    Yields:
        Request lines the proxy received.
    """
    request_lines: list[str] = []

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        request_lines.append((await reader.readline()).decode().strip())
        while (await reader.readline()) not in (b"\r\n", b""):
            pass
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    for variable in _PROXY_VARIABLES:
        monkeypatch.delenv(variable, raising=False)
    monkeypatch.setenv("HTTP_PROXY", f"http://127.0.0.1:{port}")
    async with server:
        yield request_lines


def _build_client() -> httpx.AsyncClient:
    return build_async_client(
        base_url="http://kitaru.invalid",
        headers={},
        timeout=5.0,
        retries=0,
        pool_size=1,
    )


async def test_client_honors_proxy_environment(proxy_server: list[str]) -> None:
    """Send requests through the proxy configured in the environment."""
    async with _build_client() as client:
        response = await client.get("/api/v1/info")

    assert response.status_code == 200
    assert proxy_server == ["GET http://kitaru.invalid/api/v1/info HTTP/1.1"]


async def test_client_honors_no_proxy_environment(
    proxy_server: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Bypass the proxy for hosts listed in NO_PROXY."""
    monkeypatch.setenv("NO_PROXY", "kitaru.invalid")

    async with _build_client() as client:
        with pytest.raises(httpx.ConnectError):
            await client.get("/api/v1/info")

    assert proxy_server == []
