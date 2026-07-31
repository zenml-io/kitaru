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
"""Tests for the cached bearer token provider."""

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

from kitaru.api_models.v1.auth import (
    API_KEY_PREFIX,
    CONTROL_PLANE_API_KEY_PREFIX,
    TokenResponse,
)
from kitaru.client.auth import TokenProvider
from kitaru.client.credential_store import CredentialStore
from kitaru.client.credentials import ApiToken, ApiType

SERVER_URL = "https://kitaru.example.com"
CONTROL_PLANE_URL = "https://control-plane.example.com"
SERVER_API_KEY = f"{API_KEY_PREFIX}secret"
CONTROL_PLANE_API_KEY = f"{CONTROL_PLANE_API_KEY_PREFIX}secret"


class FakeTokenExchange:
    """Fake token exchange counting the calls it received."""

    def __init__(self) -> None:
        """Initialize the fake with no recorded calls."""
        self.device_code_calls = 0
        self.control_plane_credentials: list[str] = []

    async def exchange_device_code(
        self, device_id: uuid.UUID, device_code: str
    ) -> TokenResponse:
        """Record the call and issue a fresh token.

        Args:
            device_id: Id of the device.
            device_code: Device code issued with the authorization.

        Returns:
            Freshly issued token.
        """
        self.device_code_calls += 1
        return TokenResponse(
            access_token=f"token-for-device-{self.device_code_calls}",
            token_type="bearer",
            expires_in=3600,
        )

    async def exchange_control_plane_credential(self, credential: str) -> TokenResponse:
        """Record the credential and issue a fresh token.

        Args:
            credential: Control plane session token or API key.

        Returns:
            Freshly issued token.
        """
        self.control_plane_credentials.append(credential)
        return TokenResponse(
            access_token=f"token-for-{credential}",
            token_type="bearer",
            expires_in=3600,
        )


def _store(tmp_path: Path) -> CredentialStore:
    """Build a credential store backed by a file under tmp_path.

    Args:
        tmp_path: Pytest temporary directory.

    Returns:
        Store pointed at a fresh credentials file.
    """
    return CredentialStore(path=tmp_path / "credentials.json")


def _expired_token() -> ApiToken:
    """Build a token that expired a minute ago.

    Returns:
        Expired token.
    """
    return ApiToken(
        access_token="stale-token",
        expires_at=datetime.now(UTC) - timedelta(minutes=1),
        leeway_seconds=0,
    )


async def test_cached_valid_token_is_used_without_exchange(tmp_path: Path) -> None:
    """Return a cached valid token without calling the exchange."""
    store = _store(tmp_path)
    device_id = uuid.uuid4()
    store.set_device(SERVER_URL, device_id, "device-code")
    store.set_token(
        SERVER_URL, ApiToken(access_token="cached-token", leeway_seconds=30)
    )
    exchange = FakeTokenExchange()
    provider = TokenProvider(SERVER_URL, store, exchange)

    token = await provider.get_token()

    assert token == "cached-token"
    assert exchange.device_code_calls == 0


async def test_server_api_key_is_sent_directly(tmp_path: Path) -> None:
    """Send a stored server API key as the bearer token without exchanging it."""
    store = _store(tmp_path)
    store.set_api_key(SERVER_URL, SERVER_API_KEY)
    exchange = FakeTokenExchange()
    provider = TokenProvider(SERVER_URL, store, exchange)

    token = await provider.get_token()

    assert token == SERVER_API_KEY
    assert exchange.device_code_calls == 0
    assert store.get_token(SERVER_URL) is None


async def test_control_plane_api_key_is_exchanged_for_a_session(
    tmp_path: Path,
) -> None:
    """Exchange a stored control plane API key instead of sending it directly."""
    store = _store(tmp_path)
    store.set_api_key(SERVER_URL, CONTROL_PLANE_API_KEY)
    exchange = FakeTokenExchange()
    provider = TokenProvider(SERVER_URL, store, exchange)

    token = await provider.get_token()
    await provider.close()

    assert exchange.control_plane_credentials == [CONTROL_PLANE_API_KEY]
    assert token == f"token-for-{CONTROL_PLANE_API_KEY}"
    cached = store.get_token(SERVER_URL)
    assert cached is not None
    assert cached.access_token == token


async def test_control_plane_api_key_is_exchanged_once(tmp_path: Path) -> None:
    """Reuse the exchanged token rather than exchanging the key on every call."""
    store = _store(tmp_path)
    store.set_api_key(SERVER_URL, CONTROL_PLANE_API_KEY)
    exchange = FakeTokenExchange()
    provider = TokenProvider(SERVER_URL, store, exchange)

    tokens = [await provider.get_token() for _ in range(3)]
    await provider.close()

    assert len(exchange.control_plane_credentials) == 1
    assert len(set(tokens)) == 1


async def test_expired_token_triggers_device_code_exchange(tmp_path: Path) -> None:
    """Exchange the device code for a fresh token and write it back to the store."""
    store = _store(tmp_path)
    store.set_device(SERVER_URL, uuid.uuid4(), "device-code")
    store.set_token(SERVER_URL, _expired_token())
    exchange = FakeTokenExchange()
    provider = TokenProvider(SERVER_URL, store, exchange)

    token = await provider.get_token()

    assert exchange.device_code_calls == 1
    assert token == "token-for-device-1"
    cached = store.get_token(SERVER_URL)
    assert cached is not None
    assert cached.access_token == token


async def test_renew_with_stale_generation_reuses_fetched_token(
    tmp_path: Path,
) -> None:
    """Return the token another caller already fetched instead of exchanging again."""
    store = _store(tmp_path)
    store.set_device(SERVER_URL, uuid.uuid4(), "device-code")
    exchange = FakeTokenExchange()
    provider = TokenProvider(SERVER_URL, store, exchange)

    tokens = await asyncio.gather(*(provider.renew(0) for _ in range(5)))

    assert exchange.device_code_calls == 1
    assert len(set(tokens)) == 1
    assert provider.generation == 1


async def test_get_token_returns_none_when_nothing_can_refresh(
    tmp_path: Path,
) -> None:
    """Return None when the stored entry has no way to produce a fresh token."""
    store = _store(tmp_path)
    store.set_token(SERVER_URL, _expired_token())
    exchange = FakeTokenExchange()
    provider = TokenProvider(SERVER_URL, store, exchange)

    token = await provider.get_token()

    assert token is None
    assert exchange.device_code_calls == 0


async def test_get_token_returns_none_for_unknown_server(tmp_path: Path) -> None:
    """Return None when the server has no stored credentials at all."""
    store = _store(tmp_path)
    exchange = FakeTokenExchange()
    provider = TokenProvider(SERVER_URL, store, exchange)

    assert await provider.get_token() is None


async def test_control_plane_credential_is_exchanged_for_a_session(
    tmp_path: Path,
) -> None:
    """Present a stored control plane token to the server for a session token."""
    store = _store(tmp_path)
    store.set_token(
        SERVER_URL, _expired_token(), control_plane_api_url=CONTROL_PLANE_URL
    )
    store.set_token(
        CONTROL_PLANE_URL,
        ApiToken(access_token="cp-token", leeway_seconds=0),
        type=ApiType.CONTROL_PLANE,
    )
    exchange = FakeTokenExchange()
    provider = TokenProvider(SERVER_URL, store, exchange)

    token = await provider.get_token()
    await provider.close()

    assert exchange.control_plane_credentials == ["cp-token"]
    assert token == "token-for-cp-token"
    assert exchange.device_code_calls == 0


async def test_device_authorization_wins_over_the_control_plane(
    tmp_path: Path,
) -> None:
    """Run the device code exchange first, so credential priority stays in one place."""
    store = _store(tmp_path)
    store.set_device(SERVER_URL, uuid.uuid4(), "device-code")
    store.set_token(
        SERVER_URL, _expired_token(), control_plane_api_url=CONTROL_PLANE_URL
    )
    store.set_token(
        CONTROL_PLANE_URL,
        ApiToken(access_token="cp-token", leeway_seconds=0),
        type=ApiType.CONTROL_PLANE,
    )
    exchange = FakeTokenExchange()
    provider = TokenProvider(SERVER_URL, store, exchange)

    token = await provider.get_token()
    await provider.close()

    assert token == "token-for-device-1"
    assert exchange.control_plane_credentials == []


async def test_control_plane_without_a_stored_token_cannot_refresh(
    tmp_path: Path,
) -> None:
    """Return None when the control plane entry can produce no token either."""
    store = _store(tmp_path)
    store.set_token(
        SERVER_URL, _expired_token(), control_plane_api_url=CONTROL_PLANE_URL
    )
    exchange = FakeTokenExchange()
    provider = TokenProvider(SERVER_URL, store, exchange)

    token = await provider.get_token()
    await provider.close()

    assert token is None
    assert exchange.control_plane_credentials == []
