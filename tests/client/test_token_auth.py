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
"""Tests for the bearer token auth flows."""

import asyncio
from collections import deque

import httpx

from kitaru.client.auth import RenewingTokenAuth, StaticTokenAuth


class FakeTokenSource:
    """Fake token source producing scripted tokens."""

    def __init__(
        self, cached: str | None = None, tokens: list[str] | None = None
    ) -> None:
        """Initialize the fake with a cached token and fetchable tokens.

        Args:
            cached: Token returned from the cache.
            tokens: Tokens returned by successive fetches.
        """
        self._cached = cached
        self._tokens = deque(tokens or [])
        self.fetch_calls = 0
        self.closed = False

    def get_cached_token(self) -> str | None:
        """Return the cached token."""
        return self._cached

    async def fetch_token(self) -> str | None:
        """Pop and cache the next scripted token, or None once exhausted."""
        self.fetch_calls += 1
        if not self._tokens:
            return None
        self._cached = self._tokens.popleft()
        return self._cached

    async def close(self) -> None:
        """Mark the source closed."""
        self.closed = True


def _client_accepting(
    tokens: set[str], auth: httpx.Auth
) -> tuple[httpx.AsyncClient, list[str | None]]:
    """Build a client whose server accepts only the given bearer tokens.

    Args:
        tokens: Bearer tokens the server accepts.
        auth: Auth flow attached to every request.

    Returns:
        Client wired to a mock transport, and the bearer token of every
        request the server saw.
    """
    seen: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        header = request.headers.get("Authorization")
        token = header.removeprefix("Bearer ") if header is not None else None
        seen.append(token)
        if token in tokens:
            return httpx.Response(200, json={})
        return httpx.Response(401)

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://test", auth=auth
    )
    return client, seen


async def test_static_token_is_attached_and_never_renewed() -> None:
    """Attach the fixed bearer token and surface a 401 without retrying."""
    auth = StaticTokenAuth("fixed-token")
    client, seen = _client_accepting({"other-token"}, auth)

    response = await client.get("/v1/accounts")

    assert response.status_code == 401
    assert seen == ["fixed-token"]


async def test_cached_token_is_used_without_fetching() -> None:
    """Send the cached token and never call the source when it is accepted."""
    source = FakeTokenSource(cached="cached-token")
    client, seen = _client_accepting({"cached-token"}, RenewingTokenAuth(source))

    response = await client.get("/v1/accounts")

    assert response.status_code == 200
    assert seen == ["cached-token"]
    assert source.fetch_calls == 0


async def test_missing_token_is_fetched_before_the_first_request() -> None:
    """Fetch a token from the source when nothing is cached yet."""
    source = FakeTokenSource(tokens=["fresh-token"])
    client, seen = _client_accepting({"fresh-token"}, RenewingTokenAuth(source))

    response = await client.get("/v1/accounts")

    assert response.status_code == 200
    assert seen == ["fresh-token"]
    assert source.fetch_calls == 1


async def test_rejected_token_is_renewed_and_retried_once() -> None:
    """Retry a request rejected with HTTP 401 once, with a freshly fetched token."""
    source = FakeTokenSource(cached="stale-token", tokens=["fresh-token"])
    client, seen = _client_accepting({"fresh-token"}, RenewingTokenAuth(source))

    response = await client.get("/v1/accounts")

    assert response.status_code == 200
    assert seen == ["stale-token", "fresh-token"]
    assert source.fetch_calls == 1


async def test_no_infinite_loop_when_renewed_token_is_also_rejected() -> None:
    """Surface the 401 after one bounded retry, never looping forever."""
    source = FakeTokenSource(cached="stale-token", tokens=["also-stale"])
    client, seen = _client_accepting({"fresh-token"}, RenewingTokenAuth(source))

    response = await client.get("/v1/accounts")

    assert response.status_code == 401
    assert seen == ["stale-token", "also-stale"]


async def test_concurrent_renewals_dedup_to_one_fetch() -> None:
    """Reuse the token another caller already fetched instead of fetching again."""
    source = FakeTokenSource(cached="stale-token", tokens=["fresh-token"])
    client, _ = _client_accepting({"fresh-token"}, RenewingTokenAuth(source))

    responses = await asyncio.gather(*(client.get("/v1/accounts") for _ in range(5)))

    assert {response.status_code for response in responses} == {200}
    assert source.fetch_calls == 1


async def test_request_without_a_producible_token_is_sent_unauthenticated() -> None:
    """Send the request without a bearer token when the source produces none."""
    source = FakeTokenSource()
    client, seen = _client_accepting({"fresh-token"}, RenewingTokenAuth(source))

    response = await client.get("/v1/accounts")

    assert response.status_code == 401
    assert seen == [None]


async def test_close_closes_the_source() -> None:
    """Close the token source when the auth flow is closed."""
    source = FakeTokenSource()
    auth = RenewingTokenAuth(source)

    await auth.close()

    assert source.closed
