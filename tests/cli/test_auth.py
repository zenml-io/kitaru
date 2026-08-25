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
from pathlib import Path

import pytest

from kitaru.api_models.v1.auth import TokenResponse
from kitaru.api_models.v1.info import AuthScheme, ServerInfoResponse
from kitaru.cli import auth
from kitaru.cli.output import CLIError
from kitaru.client.config import get_server_url
from kitaru.client.credential_store import CredentialStore
from kitaru.client.credentials import ApiToken
from kitaru.client.exceptions import AuthenticationError, NotFoundError


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

    def __init__(
        self,
        scheme: AuthScheme,
        error: Exception | None = None,
        info_error: Exception | None = None,
    ) -> None:
        """Initialize the fake client."""
        self.auth = FakeAuthResource(error)
        self.closed = False
        info = ServerInfoResponse(version="0.21.0", auth_scheme=scheme)

        class InfoResource:
            async def get(self) -> ServerInfoResponse:
                if info_error:
                    raise info_error
                return info

        self.info = InfoResource()

    async def close(self) -> None:
        """Record client closure."""
        self.closed = True


async def test_login_explains_when_kitaru_is_not_available(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A missing info endpoint identifies the unavailable Kitaru deployment."""
    credential_store = CredentialStore(tmp_path / "credentials.json")
    server = "https://preview.example.com"
    client = FakeClient(
        AuthScheme.NONE,
        info_error=NotFoundError(404, "Not found"),
    )
    monkeypatch.setattr(auth, "KitaruAPIClient", lambda **_: client)

    with pytest.raises(CLIError) as raised:
        await auth.login(
            server=server,
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

    assert raised.value.kind == "invalid_configuration"
    assert str(raised.value) == (
        f"Kitaru is not available at {server}. Check the URL or deployment."
    )
    assert raised.value.details == {
        "status_code": 404,
        "server_url": server,
    }
    assert credential_store.list() == []
    assert client.closed is True


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


async def test_valid_api_key_is_stored_and_selects_its_server(
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
    assert get_server_url() == "https://api.example.com"


async def test_no_auth_login_stores_only_the_selected_target(
    tmp_path, monkeypatch
) -> None:
    """A server with no auth becomes selected without storing a credential."""
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
    assert get_server_url() == "https://public.example.com"


@pytest.mark.parametrize(
    "upgrade",
    [False, True],
)
async def test_local_login_starts_runtime_before_selecting_target(
    tmp_path, monkeypatch, upgrade
) -> None:
    """Local login delegates deployment startup and selects it after success."""
    credential_store = CredentialStore(tmp_path / "credentials.json")
    captured: dict[str, object] = {}
    interactions: list[str] = []
    write_progress = interactions.append

    async def fake_start(**kwargs):
        captured.update(kwargs)
        return (
            {
                "server_url": "http://localhost:8000",
                "port": 8000,
                "deployment": "created",
                "auth_scheme": "none",
                "authentication": "not_required",
                "credential_kind": "none",
                "credential_stored": False,
            },
            [],
        )

    async def opened(server_url: str) -> bool:
        assert server_url == "http://localhost:8000"
        return False

    monkeypatch.setattr(auth.local_runtime, "start_local_runtime", fake_start)
    monkeypatch.setattr(auth.local_runtime, "open_local_dashboard", opened)
    monkeypatch.setattr(auth, "write_interaction", write_progress)
    client = FakeClient(AuthScheme.NONE)
    monkeypatch.setattr(auth, "KitaruAPIClient", lambda **_: client)
    result = await auth.login(
        server=None,
        local=True,
        upgrade=upgrade,
        username=None,
        password_stdin=False,
        api_key_stdin=False,
        credential_store=credential_store,
        timeout=30,
        non_interactive=False,
        no_browser=False,
        stdin=io.StringIO(),
        package_version="0.21.0",
    )

    progress = captured.pop("progress")
    assert progress is write_progress
    assert interactions == []
    assert captured == {
        "package_version": "0.21.0",
        "upgrade": upgrade,
        "timeout": 30,
        "port": None,
    }
    assert result.item["deployment"] == "created"
    assert "could not be opened" in result.warnings[0]
    assert get_server_url() == "http://localhost:8000"


async def test_custom_local_port_selects_and_opens_dynamic_url(
    tmp_path, monkeypatch
) -> None:
    """Local login uses the runtime URL derived from the requested port."""
    credential_store = CredentialStore(tmp_path / "credentials.json")
    clients: list[str] = []
    opened_urls: list[str] = []

    async def fake_start(**kwargs):
        assert kwargs["port"] == 9010
        return (
            {
                "server_url": "http://localhost:9010",
                "port": 9010,
                "deployment": "created",
                "auth_scheme": "none",
                "authentication": "not_required",
                "credential_kind": "none",
                "credential_stored": False,
            },
            [],
        )

    async def opened(server_url: str) -> bool:
        opened_urls.append(server_url)
        return True

    monkeypatch.setattr(auth.local_runtime, "start_local_runtime", fake_start)
    monkeypatch.setattr(auth.local_runtime, "open_local_dashboard", opened)
    client = FakeClient(AuthScheme.NONE)

    def build_client(**kwargs):
        clients.append(kwargs["base_url"])
        return client

    monkeypatch.setattr(auth, "KitaruAPIClient", build_client)

    result = await auth.login(
        server=None,
        local=True,
        port=9010,
        username=None,
        password_stdin=False,
        api_key_stdin=False,
        credential_store=credential_store,
        timeout=30,
        non_interactive=False,
        no_browser=False,
        stdin=io.StringIO(),
        package_version="0.21.0",
    )

    assert clients == ["http://localhost:9010"]
    assert opened_urls == ["http://localhost:9010"]
    assert result.links == {"dashboard": "http://localhost:9010"}
    assert get_server_url() == "http://localhost:9010"


async def test_port_requires_local_login(tmp_path) -> None:
    """A host port cannot be combined with a managed server login."""
    with pytest.raises(CLIError) as raised:
        await auth.login(
            server="https://managed.example.com",
            local=False,
            port=9010,
            username=None,
            password_stdin=False,
            api_key_stdin=False,
            credential_store=CredentialStore(tmp_path / "credentials.json"),
            timeout=30,
            non_interactive=True,
            no_browser=True,
            stdin=io.StringIO(),
        )

    assert raised.value.kind == "invalid_arguments"
    assert "--port requires --local" in raised.value.message


async def test_failed_local_start_does_not_replace_selected_target(
    tmp_path, monkeypatch
) -> None:
    """A deployment failure leaves the previously selected server unchanged."""
    credential_store = CredentialStore(tmp_path / "credentials.json")
    auth.set_server_url("https://existing.example.com")
    captured: dict[str, object] = {}

    async def fail_start(**kwargs):
        captured.update(kwargs)
        raise CLIError("timeout", "unhealthy")

    monkeypatch.setattr(auth.local_runtime, "start_local_runtime", fail_start)
    with pytest.raises(CLIError, match="unhealthy"):
        await auth.login(
            server=None,
            local=True,
            username=None,
            password_stdin=False,
            api_key_stdin=False,
            credential_store=credential_store,
            timeout=30,
            non_interactive=True,
            no_browser=True,
            stdin=io.StringIO(),
            package_version="0.21.0",
        )

    assert get_server_url() == "https://existing.example.com"
    assert captured["progress"] is None


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
        refresh=False,
    ) -> tuple[ApiToken, str]:
        captured.update(
            api_client=api_client,
            base_url=base_url,
            api_key=api_key,
            open_browser=open_browser,
            prompt=prompt,
            refresh=refresh,
        )
        token = ApiToken.issued("server-session", 3600)
        store.set_token(base_url, token)
        return token, "api_key" if api_key is not None else "device"

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


async def test_logout_removes_only_the_selected_credential(tmp_path) -> None:
    """One-server logout leaves unrelated credentials untouched."""
    credential_store = CredentialStore(tmp_path / "credentials.json")
    credential_store.set_api_key("https://prod.example.com", "KITKEY_prod")
    credential_store.set_api_key("https://dev.example.com", "KITKEY_dev")

    result = await auth.logout(
        server_url="https://prod.example.com",
        all_servers=False,
        credential_store=credential_store,
    )

    assert result.item["credential_removed"] is True
    assert credential_store.get("https://prod.example.com") is None
    assert credential_store.get("https://dev.example.com") is not None


async def test_local_logout_stops_runtime_and_clears_selection(
    tmp_path, monkeypatch
) -> None:
    """Logging out from the owned local target stops it and clears selection."""
    credential_store = CredentialStore(tmp_path / "credentials.json")
    server_url = "http://localhost:8000"
    auth.set_server_url(server_url)
    monkeypatch.setattr(
        auth.local_runtime,
        "is_local_runtime_url",
        lambda candidate: candidate == server_url,
    )
    captured: dict[str, object] = {}

    async def fake_stop(*, delete_volumes: bool):
        captured["delete_volumes"] = delete_volumes
        return {
            "server_url": server_url,
            "deployment": "deleted",
            "data_deleted": True,
        }

    monkeypatch.setattr(auth.local_runtime, "stop_local_runtime", fake_stop)
    result = await auth.logout(
        server_url=server_url,
        all_servers=False,
        delete_volumes=True,
        credential_store=credential_store,
    )

    assert captured["delete_volumes"] is True
    assert result.item["deployment"] == "deleted"
    assert result.warnings
    assert get_server_url() is None


async def test_volume_logout_rejects_unowned_localhost_target(
    tmp_path, monkeypatch
) -> None:
    """Volume deletion cannot target an unrelated localhost server."""
    monkeypatch.setattr(auth.local_runtime, "is_local_runtime_url", lambda _: False)

    with pytest.raises(CLIError, match="CLI-owned local deployment"):
        await auth.logout(
            server_url="http://localhost:9010",
            all_servers=False,
            delete_volumes=True,
            credential_store=CredentialStore(tmp_path / "credentials.json"),
        )


async def test_logout_all_rejects_volume_deletion(tmp_path) -> None:
    """Credential-wide logout cannot delete local Docker data."""
    with pytest.raises(CLIError, match="cannot be combined"):
        await auth.logout(
            server_url=None,
            all_servers=True,
            delete_volumes=True,
            credential_store=CredentialStore(tmp_path / "credentials.json"),
        )


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
