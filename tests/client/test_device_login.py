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
"""Tests for the device authorization login flow."""

import asyncio
from pathlib import Path

import pytest
from device_fakes import FakeDeviceRepository, build_device_auth_app

from conftest import (
    FakeAccountRepository,
    FakeApiKeyRepository,
    asgi_api_client,
    create_api_key,
)
from kitaru.api_models.v1.auth import DeviceAuthorizationResponse
from kitaru.api_models.v1.device import DeviceStatus, DeviceVerifyRequest
from kitaru.client.credential_store import CredentialStore
from kitaru.client.device_auth import device_login
from kitaru.client.device_grant import DeviceLoginError
from kitaru.client.exceptions import TokenGrantError
from kitaru.server.domain.account import Account

BASE_URL = "http://test"


async def test_device_login_writes_store_and_returns_token(tmp_path: Path) -> None:
    """Authorize a device end to end and store the device and its token."""
    account_repository = FakeAccountRepository()
    api_key_repository = FakeApiKeyRepository()
    account = await account_repository.create(Account(name="alice"))
    _, plaintext_key = await create_api_key(api_key_repository, owner_id=account.id)
    app = build_device_auth_app(
        account_repository,
        api_key_repository,
        FakeDeviceRepository(),
        DEVICE_AUTH_POLLING_INTERVAL_SECONDS=0,
    )

    cli_store = CredentialStore(path=tmp_path / "cli-credentials.json")
    cli_client = asgi_api_client(app, credential_store=cli_store)

    confirming_store = CredentialStore(path=tmp_path / "confirming-credentials.json")
    confirming_store.set_api_key(BASE_URL, plaintext_key)
    confirming_client = asgi_api_client(app, credential_store=confirming_store)

    prompted: list[DeviceAuthorizationResponse] = []
    ready = asyncio.Event()

    def prompt(authorization: DeviceAuthorizationResponse) -> None:
        prompted.append(authorization)
        ready.set()

    login = asyncio.create_task(
        device_login(cli_client, BASE_URL, cli_store, open_browser=False, prompt=prompt)
    )
    await asyncio.wait_for(ready.wait(), timeout=2)
    authorization = prompted[0]

    verified = await confirming_client.devices.verify(
        authorization.device_id,
        DeviceVerifyRequest(user_code=authorization.user_code, trusted=True),
    )
    assert verified.status == DeviceStatus.VERIFIED

    token = await asyncio.wait_for(login, timeout=2)

    assert token.access_token
    stored = cli_store.get(BASE_URL)
    assert stored is not None
    assert stored.device_id == authorization.device_id
    assert stored.device_code is not None
    cached = cli_store.get_token(BASE_URL)
    assert cached is not None
    assert cached.access_token == token.access_token


async def test_exchange_device_code_reports_authorization_pending(
    tmp_path: Path,
) -> None:
    """Surface the authorization_pending error code before confirmation."""
    app = build_device_auth_app(
        FakeAccountRepository(), FakeApiKeyRepository(), FakeDeviceRepository()
    )
    store = CredentialStore(path=tmp_path / "credentials.json")
    client = asgi_api_client(app, credential_store=store)

    authorization = await client.auth.device_authorization()

    with pytest.raises(TokenGrantError) as exc_info:
        await client.auth.exchange_device_code(
            authorization.device_id, authorization.device_code
        )
    assert exc_info.value.error == "authorization_pending"


async def test_device_login_raises_when_authorization_expires(
    tmp_path: Path,
) -> None:
    """Raise DeviceLoginError once the authorization expires unconfirmed."""
    app = build_device_auth_app(
        FakeAccountRepository(),
        FakeApiKeyRepository(),
        FakeDeviceRepository(),
        DEVICE_AUTH_TIMEOUT_SECONDS=0,
    )
    store = CredentialStore(path=tmp_path / "credentials.json")
    client = asgi_api_client(app, credential_store=store)

    with pytest.raises(DeviceLoginError):
        await device_login(
            client, BASE_URL, store, open_browser=False, prompt=lambda _: None
        )
