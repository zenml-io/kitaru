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
"""End-to-end tests for the control plane login flow.

The control plane session is stubbed here. Its own HTTP behavior is covered
by tests/client/test_control_plane.py.
"""

import uuid
from collections.abc import AsyncGenerator
from typing import ClassVar

import pytest

from conftest import (
    FakeAccountRepository,
    FakeApiKeyRepository,
    FakeControlPlaneClient,
    FakePasswordHasher,
    asgi_api_client,
    control_plane_settings,
    local_settings,
)
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.auth import TokenProvider
from kitaru.client.control_plane import ControlPlaneLoginError
from kitaru.client.control_plane_auth import control_plane_login
from kitaru.client.credential_store import CredentialStore
from kitaru.client.credentials import ApiToken, CredentialType
from kitaru.server.adapters.auth.auth_service import AuthService
from kitaru.server.adapters.auth.control_plane import (
    ControlPlaneAuthenticator,
    ControlPlaneUser,
)
from kitaru.server.adapters.rest.dependencies import get_auth_service
from kitaru.server.api.app import create_app

CONTROL_PLANE_URL = "https://control-plane.example.com"
SERVER_URL = "http://test"
CONTROL_PLANE_TOKEN = "cp-token"

pytestmark = pytest.mark.usefixtures("isolated_config_directory")


class FakeControlPlaneSession:
    """Control plane session issuing a fixed token without any HTTP."""

    calls: ClassVar[list[str]] = []

    def __init__(self, api_url: str, store: CredentialStore) -> None:
        """Initialize the session.

        Args:
            api_url: Control plane API base URL.
            store: Credential store the session writes to.
        """
        self._url = api_url
        self._store = store

    async def login_with_api_key(self, api_key: str) -> ApiToken:
        """Record the call and issue the fixed token.

        Args:
            api_key: Control plane API key.

        Returns:
            Fixed token.
        """
        self.calls.append("api_key")
        return ApiToken.issued(CONTROL_PLANE_TOKEN, 3600)

    async def device_login(
        self, open_browser: bool = True, prompt: object = None
    ) -> ApiToken:
        """Record the call and issue the fixed token.

        Args:
            open_browser: Whether to open the verification page.
            prompt: Called with the authorization.

        Returns:
            Fixed token.
        """
        self.calls.append("device")
        return ApiToken.issued(CONTROL_PLANE_TOKEN, 3600)

    async def get_token(self) -> str | None:
        """Record the call and return the fixed token.

        Returns:
            Fixed token.
        """
        self.calls.append("get_token")
        return CONTROL_PLANE_TOKEN

    async def close(self) -> None:
        """Close the session, which holds no connections."""


@pytest.fixture(autouse=True)
def session_calls(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Answer every control plane call with a fixed token.

    Returns:
        Names of the session calls the flow made, in order.
    """
    calls: list[str] = []
    monkeypatch.setattr(FakeControlPlaneSession, "calls", calls)
    for module in ("control_plane_auth", "auth"):
        monkeypatch.setattr(
            f"kitaru.client.{module}.ControlPlaneSession", FakeControlPlaneSession
        )
    return calls


@pytest.fixture
def authorizer() -> FakeControlPlaneClient:
    """Provide a control plane API client authorizing every caller as one user."""
    return FakeControlPlaneClient(
        user=ControlPlaneUser(
            id=uuid.uuid4(), username="alice", email="alice@example.com"
        )
    )


@pytest.fixture
async def api_client(
    authorizer: FakeControlPlaneClient, credential_store: CredentialStore
) -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide a client routed to a control plane backed app."""
    settings = control_plane_settings(CONTROL_PLANE_API_URL=CONTROL_PLANE_URL)
    account_repository = FakeAccountRepository()
    app = create_app(settings)
    service = AuthService(
        settings=settings,
        account_repository=account_repository,
        api_key_repository=FakeApiKeyRepository(),
        password_hasher=FakePasswordHasher(),
        control_plane=ControlPlaneAuthenticator(
            client=authorizer,
            account_repository=account_repository,
            server_id=settings.SERVER_ID,
        ),
    )
    app.dependency_overrides[get_auth_service] = lambda: service
    async with asgi_api_client(app, credential_store=credential_store) as client:
        yield client


async def test_api_key_login_stores_both_credentials(
    api_client: KitaruAPIClient,
    credential_store: CredentialStore,
    authorizer: FakeControlPlaneClient,
) -> None:
    """Cache the control plane entry and the Kitaru session it produced."""
    token = await control_plane_login(
        api_client, SERVER_URL, credential_store, api_key="ZENPROKEY_abc"
    )

    assert token.access_token
    assert authorizer.received_credentials == [CONTROL_PLANE_TOKEN]

    server = credential_store.get(SERVER_URL)
    assert server is not None
    assert server.type is CredentialType.SERVER
    assert server.control_plane_api_url == CONTROL_PLANE_URL
    assert server.api_token is not None


async def test_login_without_an_api_key_runs_the_device_flow(
    api_client: KitaruAPIClient,
    credential_store: CredentialStore,
    session_calls: list[str],
) -> None:
    """Fall back to the device authorization flow when no API key is given."""
    await control_plane_login(
        api_client, SERVER_URL, credential_store, open_browser=False
    )

    assert session_calls == ["device"]


async def test_stored_control_plane_entry_renews_the_session_token(
    api_client: KitaruAPIClient,
    credential_store: CredentialStore,
    authorizer: FakeControlPlaneClient,
) -> None:
    """Exchange a control plane token for a new session once the cached one is gone."""
    await control_plane_login(
        api_client, SERVER_URL, credential_store, api_key="ZENPROKEY_abc"
    )
    credential_store.clear_token(SERVER_URL)
    provider = TokenProvider(SERVER_URL, credential_store, api_client.auth)

    token = await provider.get_token()
    await provider.close()

    assert token is not None
    assert len(authorizer.received_credentials) == 2
    cached = credential_store.get_token(SERVER_URL)
    assert cached is not None
    assert cached.access_token == token


async def test_login_rejects_a_server_without_a_control_plane(
    credential_store: CredentialStore,
) -> None:
    """Refuse to log in against a server that owns its own identities."""
    app = create_app(local_settings())
    async with asgi_api_client(app, credential_store=credential_store) as client:
        with pytest.raises(ControlPlaneLoginError):
            await control_plane_login(
                client, SERVER_URL, credential_store, api_key="key"
            )
    assert credential_store.get(SERVER_URL) is None
