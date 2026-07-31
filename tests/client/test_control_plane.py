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
"""Tests for the control plane session."""

import uuid

import httpx
import pytest

from kitaru.client.client_id import ENV_CLIENT_ID
from kitaru.client.control_plane import (
    API_KEY_GRANT_TYPE,
    DEVICE_CODE_GRANT_TYPE,
    ControlPlaneSession,
)
from kitaru.client.credential_store import CredentialStore
from kitaru.client.credentials import ApiToken, ApiType
from kitaru.client.device_grant import DeviceLoginError
from kitaru.transport import RetryTransport

CONTROL_PLANE_URL = "https://control-plane.example.com"

pytestmark = pytest.mark.usefixtures("isolated_config_directory")


class FakeControlPlane:
    """Stub of the control plane auth endpoints."""

    def __init__(
        self,
        pending_polls: int = 0,
        login_error: dict[str, str] | None = None,
    ) -> None:
        """Initialize the stub.

        Args:
            pending_polls: Device code polls answered with
                ``authorization_pending`` before the confirmation lands.
            login_error: OAuth 2.0 error body every login is refused with.
        """
        self.pending_polls = pending_polls
        self.login_error = login_error
        self.requests: list[httpx.Request] = []
        self.logins = 0

    async def handle(self, request: httpx.Request) -> httpx.Response:
        """Answer one control plane request.

        Args:
            request: Incoming request.

        Returns:
            HTTP response.
        """
        self.requests.append(request)
        form = dict(httpx.QueryParams(request.content.decode()))
        if request.url.path == "/auth/device_authorization":
            return httpx.Response(
                200,
                json={
                    "device_code": "cp-device-code",
                    "user_code": "ABCD-EFGH",
                    "verification_uri": "/devices/verify",
                    "verification_uri_complete": None,
                    "expires_in": 300,
                    "interval": 0,
                },
            )
        if self.login_error is not None:
            return httpx.Response(400, json=self.login_error)
        if form.get("grant_type") == DEVICE_CODE_GRANT_TYPE and self.pending_polls:
            self.pending_polls -= 1
            return httpx.Response(
                400,
                json={
                    "error": "authorization_pending",
                    "error_description": "Not confirmed yet",
                },
            )
        self.logins += 1
        return httpx.Response(
            200,
            json={
                "access_token": f"cp-token-{self.logins}",
                "expires_in": 3600,
                "token_type": "bearer",
            },
        )


def _session(
    store: CredentialStore, control_plane: FakeControlPlane
) -> ControlPlaneSession:
    """Build a session routed to the stub instead of the network.

    Args:
        store: Credential store the session writes to.
        control_plane: Stub answering the requests.

    Returns:
        Session wired to a mock transport.
    """
    session = ControlPlaneSession(CONTROL_PLANE_URL, store)
    session._http = httpx.AsyncClient(
        base_url=CONTROL_PLANE_URL,
        transport=RetryTransport(httpx.MockTransport(control_plane.handle)),
    )
    return session


async def test_api_key_login_sends_the_key_as_password_and_bearer(
    credential_store: CredentialStore,
) -> None:
    """Present the API key both as the password form field and as a bearer token."""
    control_plane = FakeControlPlane()
    session = _session(credential_store, control_plane)

    token = await session.login_with_api_key("ZENPROKEY_abc")
    await session.close()

    assert token.access_token == "cp-token-1"
    request = control_plane.requests[-1]
    assert request.url.path == "/auth/login"
    form = dict(httpx.QueryParams(request.content.decode()))
    assert form["grant_type"] == API_KEY_GRANT_TYPE
    assert form["password"] == "ZENPROKEY_abc"
    assert request.headers["Authorization"] == "Bearer ZENPROKEY_abc"


async def test_api_key_login_caches_the_key_and_the_token(
    credential_store: CredentialStore,
) -> None:
    """Store the key under the control plane URL so the token can be renewed."""
    session = _session(credential_store, FakeControlPlane())

    await session.login_with_api_key("ZENPROKEY_abc")
    await session.close()

    credentials = credential_store.get_control_plane(CONTROL_PLANE_URL)
    assert credentials is not None
    assert credentials.type is ApiType.CONTROL_PLANE
    assert credentials.api_key == "ZENPROKEY_abc"
    cached = credential_store.get_token(CONTROL_PLANE_URL)
    assert cached is not None
    assert cached.access_token == "cp-token-1"


async def test_get_token_returns_the_cached_token_without_a_login(
    credential_store: CredentialStore,
) -> None:
    """Reuse a cached control plane token instead of logging in again."""
    credential_store.set_token(
        CONTROL_PLANE_URL,
        ApiToken(access_token="cached", leeway_seconds=0),
        type=ApiType.CONTROL_PLANE,
    )
    control_plane = FakeControlPlane()
    session = _session(credential_store, control_plane)

    assert await session.get_token() == "cached"
    await session.close()

    assert control_plane.logins == 0


async def test_get_token_renews_an_expired_token_from_the_stored_key(
    credential_store: CredentialStore,
) -> None:
    """Log in again with the stored API key once the cached token expires."""
    credential_store.set_api_key(
        CONTROL_PLANE_URL, "ZENPROKEY_abc", type=ApiType.CONTROL_PLANE
    )
    control_plane = FakeControlPlane()
    session = _session(credential_store, control_plane)

    assert await session.get_token() == "cp-token-1"
    await session.close()

    assert control_plane.logins == 1


async def test_get_token_returns_none_without_stored_credentials(
    credential_store: CredentialStore,
) -> None:
    """Return None when nothing stored can produce a control plane token."""
    session = _session(credential_store, FakeControlPlane())

    assert await session.get_token() is None
    await session.close()


async def test_device_login_polls_until_the_code_is_confirmed(
    credential_store: CredentialStore,
) -> None:
    """Keep polling while the control plane reports the authorization pending."""
    control_plane = FakeControlPlane(pending_polls=2)
    session = _session(credential_store, control_plane)

    token = await session.device_login(open_browser=False, prompt=lambda _: None)
    await session.close()

    assert token.access_token == "cp-token-1"
    polls = [
        request
        for request in control_plane.requests
        if request.url.path == "/auth/login"
    ]
    assert len(polls) == 3
    form = dict(httpx.QueryParams(polls[-1].content.decode()))
    assert form["grant_type"] == DEVICE_CODE_GRANT_TYPE
    assert form["device_code"] == "cp-device-code"
    assert uuid.UUID(form["client_id"])


async def test_device_login_sends_a_stable_client_id(
    credential_store: CredentialStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reuse the same client id across logins, so the device is not duplicated."""
    client_id = uuid.uuid4()
    monkeypatch.setenv(ENV_CLIENT_ID, str(client_id))
    control_plane = FakeControlPlane()
    session = _session(credential_store, control_plane)

    await session.device_login(open_browser=False, prompt=lambda _: None)
    await session.close()

    authorization = control_plane.requests[0]
    form = dict(httpx.QueryParams(authorization.content.decode()))
    assert form["client_id"] == str(client_id)
    assert authorization.headers["User-Agent"].startswith("Host/")


async def test_device_login_reports_a_refused_authorization(
    credential_store: CredentialStore,
) -> None:
    """Raise when the control plane refuses the device authorization."""
    session = _session(
        credential_store,
        FakeControlPlane(
            login_error={"error": "access_denied", "error_description": "Refused"}
        ),
    )

    with pytest.raises(DeviceLoginError, match="Refused"):
        await session.device_login(open_browser=False, prompt=lambda _: None)
    await session.close()


async def test_relative_verification_uri_is_opened_against_the_control_plane(
    credential_store: CredentialStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Open the absolute verification page for a relative verification URI."""
    opened: list[str] = []
    monkeypatch.setattr(
        "kitaru.client.control_plane.webbrowser.open", lambda uri: opened.append(uri)
    )
    session = _session(credential_store, FakeControlPlane())

    await session.device_login(prompt=lambda _: None)
    await session.close()

    assert opened == [f"{CONTROL_PLANE_URL}/devices/verify"]
