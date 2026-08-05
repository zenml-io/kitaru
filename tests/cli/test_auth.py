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
"""Authentication orchestration and credential persistence behavior."""

import io
from dataclasses import dataclass

import pytest

from kitaru.api_models.v1.auth import TokenResponse
from kitaru.api_models.v1.info import AuthScheme, ServerInfoResponse
from kitaru.cli import auth
from kitaru.cli.output import CLIError
from kitaru.client.credential_store import CredentialStore
from kitaru.client.credentials import ApiToken
from kitaru.client.exceptions import AuthenticationError


@dataclass
class FakeAuthResource:
    """Configurable fake for the auth SDK resource."""

    error: Exception | None = None
    exchanged: str | None = None

    async def exchange_api_key(self, api_key: str) -> TokenResponse:
        """Validate one API key or raise the configured error."""
        self.exchanged = api_key
        if self.error:
            raise self.error
        return TokenResponse(
            access_token="session-token", token_type="bearer", expires_in=3600
        )


class FakeClient:
    """Minimal client exposing info, auth, and close behavior."""

    def __init__(self, scheme: AuthScheme, error: Exception | None = None) -> None:
        """Initialize the fake client."""
        self.auth = FakeAuthResource(error)
        self.closed = False
        info = ServerInfoResponse(version="0.21.0", auth_scheme=scheme)

        class InfoResource:
            async def get(self) -> ServerInfoResponse:
                return info

        self.info = InfoResource()

    async def close(self) -> None:
        """Record client closure."""
        self.closed = True


async def test_api_key_is_validated_before_replacing_stored_credential(
    tmp_path, monkeypatch
) -> None:
    """A rejected key leaves the previously working credential untouched."""
    credential_store = CredentialStore(tmp_path / "credentials.json")
    server = "https://api.example.com"
    credential_store.set_api_key(server, "KITKEY_working")

    rejected = FakeClient(AuthScheme.LOCAL, AuthenticationError(401, "rejected"))
    monkeypatch.setattr(auth, "KitaruAPIClient", lambda **_: rejected)
    with pytest.raises(AuthenticationError):
        await auth.login(
            server=server,
            local=False,
            username=None,
            password_stdin=False,
            api_key_stdin=True,
            credential_store=credential_store,
            timeout=30,
            non_interactive=True,
            no_browser=True,
            stdin=io.StringIO("KITKEY_rejected\n"),
        )

    assert rejected.closed is True
    stored = credential_store.get(server)
    assert stored is not None
    assert stored.api_key == "KITKEY_working"


async def test_valid_api_key_is_stored_without_selecting_a_target(
    tmp_path, monkeypatch
) -> None:
    """Successful validation persists the key, not the short-lived token."""
    credential_store = CredentialStore(tmp_path / "credentials.json")
    client = FakeClient(AuthScheme.LOCAL)
    monkeypatch.setattr(auth, "KitaruAPIClient", lambda **_: client)

    result = await auth.login(
        server="https://api.example.com/",
        local=False,
        username=None,
        password_stdin=False,
        api_key_stdin=True,
        credential_store=credential_store,
        timeout=30,
        non_interactive=True,
        no_browser=True,
        stdin=io.StringIO("KITKEY_valid\n"),
    )

    stored = credential_store.get("https://api.example.com")
    assert stored is not None
    assert stored.api_key == "KITKEY_valid"
    assert stored.api_token is None
    assert result.item["credential_kind"] == "api_key"
    assert result.item["credential_stored"] is True
    assert "KITKEY_valid" not in str(result.item)


async def test_no_auth_login_does_not_store_credentials_or_a_target(
    tmp_path, monkeypatch
) -> None:
    """A server with no auth produces no credential or target mutation."""
    credential_store = CredentialStore(tmp_path / "credentials.json")
    client = FakeClient(AuthScheme.NONE)
    monkeypatch.setattr(auth, "KitaruAPIClient", lambda **_: client)

    result = await auth.login(
        server="https://public.example.com",
        local=False,
        username=None,
        password_stdin=False,
        api_key_stdin=False,
        credential_store=credential_store,
        timeout=30,
        non_interactive=True,
        no_browser=True,
        stdin=io.StringIO(),
    )

    assert result.item["authentication"] == "not_required"
    assert credential_store.list() == []
    assert result.item["credential_stored"] is False


async def test_control_plane_api_key_reuses_login_helper(tmp_path, monkeypatch) -> None:
    """Control-plane login delegates protocol work and persists its server token."""
    credential_store = CredentialStore(tmp_path / "credentials.json")
    client = FakeClient(AuthScheme.CONTROL_PLANE)
    monkeypatch.setattr(auth, "KitaruAPIClient", lambda **_: client)
    captured: dict[str, object] = {}

    async def fake_control_plane_login(
        api_client,
        base_url,
        store,
        api_key=None,
        open_browser=True,
        prompt=None,
    ) -> ApiToken:
        captured.update(
            api_client=api_client,
            base_url=base_url,
            api_key=api_key,
            open_browser=open_browser,
            prompt=prompt,
        )
        token = ApiToken.issued("server-session", 3600)
        store.set_token(base_url, token)
        return token

    monkeypatch.setattr(auth, "control_plane_login", fake_control_plane_login)
    result = await auth.login(
        server="https://managed.example.com",
        local=False,
        username=None,
        password_stdin=False,
        api_key_stdin=True,
        credential_store=credential_store,
        timeout=30,
        non_interactive=True,
        no_browser=True,
        stdin=io.StringIO("ZENPROKEY_valid\n"),
    )

    assert captured["api_key"] == "ZENPROKEY_valid"
    assert captured["open_browser"] is False
    assert result.item["credential_kind"] == "api_key"
    assert credential_store.get("https://managed.example.com") is not None


def test_logout_removes_only_the_selected_credential(tmp_path) -> None:
    """One-server logout leaves unrelated credentials untouched."""
    credential_store = CredentialStore(tmp_path / "credentials.json")
    credential_store.set_api_key("https://prod.example.com", "KITKEY_prod")
    credential_store.set_api_key("https://dev.example.com", "KITKEY_dev")

    result = auth.logout(
        server_url="https://prod.example.com",
        all_servers=False,
        credential_store=credential_store,
    )

    assert result.item["credential_removed"] is True
    assert credential_store.get("https://prod.example.com") is None
    assert credential_store.get("https://dev.example.com") is not None


async def test_non_interactive_device_login_fails_without_mutation(
    tmp_path, monkeypatch
) -> None:
    """Structured/non-interactive use never opens an implicit device flow."""
    credential_store = CredentialStore(tmp_path / "credentials.json")
    client = FakeClient(AuthScheme.LOCAL)
    monkeypatch.setattr(auth, "KitaruAPIClient", lambda **_: client)

    with pytest.raises(CLIError) as raised:
        await auth.login(
            server="https://api.example.com",
            local=False,
            username=None,
            password_stdin=False,
            api_key_stdin=False,
            credential_store=credential_store,
            timeout=30,
            non_interactive=True,
            no_browser=True,
            stdin=io.StringIO(),
        )

    assert raised.value.kind == "interaction_required"
    assert credential_store.list() == []
    assert client.closed is True
