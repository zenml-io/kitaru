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
"""Tests for the client's bearer token attachment and 401 retry."""

import uuid
from pathlib import Path

import httpx
import pytest
from device_fakes import FakeDeviceRepository, build_device_auth_app
from fastapi import FastAPI, HTTPException, status

from conftest import FakeAccountRepository, FakeApiKeyRepository, create_api_key
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.credential_store import CredentialStore
from kitaru.client.credentials import ApiToken
from kitaru.client.exceptions import AuthenticationError
from kitaru.server.adapters.rest.dependencies import authorize
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.domain.account import Account
from kitaru.server.domain.device import Device, DeviceStatus
from kitaru.server.domain.keys import generate_secret, hash_secret
from kitaru.transport import RetryTransport

BASE_URL = "http://test"


class RequestRecordingTransport(httpx.AsyncBaseTransport):
    """ASGI transport wrapper recording the path of every request it saw."""

    def __init__(self, transport: httpx.AsyncBaseTransport) -> None:
        """Wrap a transport and start with an empty recording.

        Args:
            transport: Transport to delegate every request to.
        """
        self._transport = transport
        self.paths: list[str] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        """Record the request path and delegate to the wrapped transport.

        Args:
            request: Outgoing HTTP request.

        Returns:
            Response from the wrapped transport.
        """
        self.paths.append(request.url.path)
        return await self._transport.handle_async_request(request)


def _client(
    app: FastAPI, store: CredentialStore
) -> tuple[KitaruAPIClient, RequestRecordingTransport]:
    """Build an SDK client routed to the app and backed by a credential store.

    Args:
        app: Application to route requests to.
        store: Credential store the client authenticates from.

    Returns:
        Client wired to an ASGI transport, and the transport recording paths.
    """
    client = KitaruAPIClient(base_url=BASE_URL, credential_store=store)
    recorder = RequestRecordingTransport(httpx.ASGITransport(app=app))
    client._http = httpx.AsyncClient(
        transport=RetryTransport(recorder),
        base_url=BASE_URL,
        headers=client._http.headers,
    )
    return client, recorder


async def _reject_every_request() -> AuthContext:
    """Reject every request regardless of the bearer token it carries.

    Raises:
        HTTPException: Always, with an invalid session token detail.
    """
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid session token."
    )


async def _authorize_device(
    repository: FakeDeviceRepository, account_id: uuid.UUID
) -> tuple[uuid.UUID, str]:
    """Store an active device the client can exchange its code with.

    Args:
        repository: Fake device repository.
        account_id: Id of the account that approved the device.

    Returns:
        Id of the device and its plaintext device code.
    """
    device_code = generate_secret()
    device = await repository.create(
        Device(
            account_id=account_id,
            user_code_hash=hash_secret("USED-CODE"),
            device_code_hash=hash_secret(device_code),
            status=DeviceStatus.ACTIVE,
        )
    )
    return device.id, device_code


async def test_stored_api_key_authenticates_directly(tmp_path: Path) -> None:
    """Send a stored API key as the bearer token without exchanging it."""
    account_repository = FakeAccountRepository()
    api_key_repository = FakeApiKeyRepository()
    account = await account_repository.create(Account(name="alice"))
    _, plaintext_key = await create_api_key(api_key_repository, owner_id=account.id)
    app = build_device_auth_app(
        account_repository, api_key_repository, FakeDeviceRepository()
    )
    store = CredentialStore(path=tmp_path / "credentials.json")
    store.set_api_key(BASE_URL, plaintext_key)
    client, recorder = _client(app, store)

    page = await client.devices.list()

    assert page.items == []
    assert recorder.paths == ["/v1/devices"]
    assert store.get_token(BASE_URL) is None


async def test_device_code_is_exchanged_for_a_token(tmp_path: Path) -> None:
    """Exchange a stored device code for a session token and authenticate with it."""
    account_repository = FakeAccountRepository()
    device_repository = FakeDeviceRepository()
    account = await account_repository.create(Account(name="alice"))
    device_id, device_code = await _authorize_device(device_repository, account.id)
    app = build_device_auth_app(
        account_repository, FakeApiKeyRepository(), device_repository
    )
    store = CredentialStore(path=tmp_path / "credentials.json")
    store.set_device(BASE_URL, device_id, device_code)
    client, recorder = _client(app, store)

    page = await client.devices.list()

    assert len(page.items) == 1
    assert recorder.paths.count("/v1/login") == 1
    assert store.get_token(BASE_URL) is not None


async def test_stale_token_is_retried_once_with_renewed_token(
    tmp_path: Path,
) -> None:
    """Retry a request rejected with HTTP 401 once, with a freshly renewed token."""
    account_repository = FakeAccountRepository()
    device_repository = FakeDeviceRepository()
    account = await account_repository.create(Account(name="alice"))
    device_id, device_code = await _authorize_device(device_repository, account.id)
    app = build_device_auth_app(
        account_repository, FakeApiKeyRepository(), device_repository
    )
    store = CredentialStore(path=tmp_path / "credentials.json")
    store.set_device(BASE_URL, device_id, device_code)
    # A cached token that looks fresh to the store but that the server has
    # never issued, so the first request is rejected and a renewal is needed.
    store.set_token(BASE_URL, ApiToken(access_token="bogus-token", leeway_seconds=30))
    client, recorder = _client(app, store)

    page = await client.devices.list()

    assert len(page.items) == 1
    assert recorder.paths.count("/v1/devices") == 2
    renewed = store.get_token(BASE_URL)
    assert renewed is not None
    assert renewed.access_token != "bogus-token"


async def test_no_infinite_loop_when_renewed_token_is_also_rejected(
    tmp_path: Path,
) -> None:
    """Raise AuthenticationError after one bounded retry, never looping forever."""
    account_repository = FakeAccountRepository()
    device_repository = FakeDeviceRepository()
    account = await account_repository.create(Account(name="alice"))
    device_id, device_code = await _authorize_device(device_repository, account.id)
    app = build_device_auth_app(
        account_repository, FakeApiKeyRepository(), device_repository
    )
    # The exchange still succeeds, but every authenticated request is rejected
    # regardless of the token it carries, so the renewed token fails too.
    app.dependency_overrides[authorize] = _reject_every_request
    store = CredentialStore(path=tmp_path / "credentials.json")
    store.set_device(BASE_URL, device_id, device_code)
    client, recorder = _client(app, store)

    with pytest.raises(AuthenticationError):
        await client.devices.list()

    assert recorder.paths.count("/v1/devices") == 2
