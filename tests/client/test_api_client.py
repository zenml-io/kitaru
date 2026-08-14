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
"""Retry and idempotency tests for the API client."""

from collections.abc import AsyncIterator, Callable
from pathlib import Path

import httpx
import pytest

from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.credential_store import CredentialStore
from kitaru.client.exceptions import NotFoundError, ServerError
from kitaru.transport import IDEMPOTENCY_KEY_HEADER, RetryTransport


def mock_api_client(
    handler: Callable[[httpx.Request], httpx.Response], retries: int = 3
) -> KitaruAPIClient:
    """Build an SDK client routed to a scripted transport.

    Args:
        handler: Handler producing the response for each request.
        retries: Retry count for failed requests.

    Returns:
        Client wired to a mock transport.
    """
    client = KitaruAPIClient(base_url="http://test")
    client._http = httpx.AsyncClient(
        transport=RetryTransport(
            httpx.MockTransport(handler), retries=retries, backoff=0.0
        ),
        base_url="http://test",
        headers=client._http.headers,
    )
    return client


async def test_retries_transport_error() -> None:
    """Retry a transport error and return the eventual response."""
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            raise httpx.ConnectError("connection refused", request=request)
        return httpx.Response(200, json={})

    client = mock_api_client(handler)
    response = await client.request("GET", "/api/v1/accounts")
    assert response.status_code == 200
    assert len(requests) == 2


async def test_retries_retryable_status_with_same_idempotency_key() -> None:
    """Retry a retryable status and reuse the idempotency key."""
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            return httpx.Response(503)
        return httpx.Response(200, json={})

    client = mock_api_client(handler)
    response = await client.request("POST", "/api/v1/users", json={"name": "alice"})
    assert response.status_code == 200
    assert len(requests) == 2
    keys = {request.headers[IDEMPOTENCY_KEY_HEADER] for request in requests}
    assert len(keys) == 1


async def test_no_retry_on_client_error() -> None:
    """Surface a non-retryable status without retrying."""
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(404, json={"detail": "not found"})

    client = mock_api_client(handler)
    with pytest.raises(NotFoundError):
        await client.request("GET", "/api/v1/accounts")
    assert len(requests) == 1


async def test_raises_after_retries_exhausted() -> None:
    """Raise the typed error once every retry attempt failed."""
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(503, json={"detail": "unavailable"})

    client = mock_api_client(handler, retries=2)
    with pytest.raises(ServerError) as exc_info:
        await client.request("GET", "/api/v1/accounts")
    assert exc_info.value.status_code == 503
    assert len(requests) == 3


async def test_raises_transport_error_after_retries_exhausted() -> None:
    """Raise the transport error once every retry attempt failed."""
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        raise httpx.ConnectError("connection refused", request=request)

    client = mock_api_client(handler, retries=2)
    with pytest.raises(httpx.ConnectError):
        await client.request("GET", "/api/v1/accounts")
    assert len(requests) == 3


async def test_fresh_idempotency_key_per_request() -> None:
    """Send a new idempotency key for each logical request."""
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={})

    client = mock_api_client(handler)
    await client.request("POST", "/api/v1/users", json={"name": "alice"})
    await client.request("POST", "/api/v1/users", json={"name": "bob"})
    keys = {request.headers[IDEMPOTENCY_KEY_HEADER] for request in requests}
    assert len(keys) == 2


async def test_no_retry_for_streaming_request_body() -> None:
    """Send a request with a non-replayable body exactly once."""
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(503)

    async def body() -> AsyncIterator[bytes]:
        yield b"chunk"

    async with httpx.AsyncClient(
        transport=RetryTransport(httpx.MockTransport(handler), backoff=0.0),
        base_url="http://test",
    ) as http:
        response = await http.post("/api/v1/blobs", content=body())
    assert response.status_code == 503
    assert len(requests) == 1


def test_client_registers_every_resource() -> None:
    """Expose every endpoint group as an attribute on the client."""
    client = KitaruAPIClient("http://test")
    expected = {
        "accounts",
        "agents",
        "agent_versions",
        "api_keys",
        "auth",
        "blobs",
        "cohorts",
        "cohort_versions",
        "devices",
        "evaluations",
        "evaluators",
        "experiments",
        "experiment_runs",
        "importers",
        "imports",
        "info",
        "jobs",
        "replays",
        "secrets",
        "session_runs",
        "sessions",
        "tags",
        "tasks",
        "workers",
    }
    assert expected <= vars(client).keys()


def test_conflicting_auth_inputs_are_rejected(tmp_path: Path) -> None:
    """Raise ValueError when both an API key and a credential store are supplied."""
    store = CredentialStore(path=tmp_path / "credentials.json")
    with pytest.raises(ValueError):
        KitaruAPIClient("http://test", api_key="key", credential_store=store)


async def test_closing_a_view_keeps_the_shared_transport_open() -> None:
    """Close a token view without closing the transport it shares."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={})

    client = mock_api_client(handler)
    view = client.with_token("view-token")

    async with view:
        pass
    response = await client.request("GET", "/api/v1/accounts")

    assert response.status_code == 200
    await client.close()


async def test_view_authenticates_with_its_own_token() -> None:
    """Send the view's bearer token instead of the parent client's."""
    tokens: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        header = request.headers.get("Authorization")
        tokens.append(header.removeprefix("Bearer ") if header else None)
        return httpx.Response(200, json={})

    client = mock_api_client(handler)
    view = client.with_token("view-token")

    await client.request("GET", "/api/v1/accounts")
    await view.request("GET", "/api/v1/accounts")

    assert tokens == [None, "view-token"]


async def test_streaming_response_is_not_consumed() -> None:
    """Return a successful response with its stream unread."""

    async def chunks() -> AsyncIterator[bytes]:
        yield b"first"
        yield b"second"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=chunks())

    async with (
        httpx.AsyncClient(
            transport=RetryTransport(httpx.MockTransport(handler), backoff=0.0),
            base_url="http://test",
        ) as http,
        http.stream("GET", "/api/v1/events") as response,
    ):
        collected = [chunk async for chunk in response.aiter_bytes()]
    assert collected == [b"first", b"second"]
