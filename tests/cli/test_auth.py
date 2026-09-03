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
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import pytest

from kitaru.api_models.v1.auth import TokenResponse
from kitaru.api_models.v1.info import AuthScheme, ServerInfoResponse
from kitaru.cli import auth
from kitaru.cli.output import CLIError
from kitaru.client.config import get_server_url, set_server_url
from kitaru.client.control_plane import (
    MANAGED_CLOUD_API_URL,
    ControlPlaneSession,
    ControlPlaneToken,
    ControlPlaneWorkspace,
)
from kitaru.client.credential_store import CredentialStore
from kitaru.client.credentials import ApiToken
from kitaru.client.exceptions import AuthenticationError, NotFoundError


@dataclass
class FakeAuthResource:
    """Configurable fake for the auth SDK resource."""

    error: Exception | None = None
    exchanged: str | None = None
    control_plane_exchanged: str | None = None

    async def exchange_api_key(self, api_key: str) -> TokenResponse:
        """Validate one API key or raise the configured error."""
        self.exchanged = api_key
        if self.error:
            raise self.error
        return TokenResponse(
            access_token="session-token", token_type="bearer", expires_in=3600
        )

    async def exchange_control_plane_credential(self, credential: str) -> TokenResponse:
        """Exchange one control plane bearer token."""
        self.control_plane_exchanged = credential
        if self.error:
            raise self.error
        return TokenResponse(
            access_token="server-session", token_type="bearer", expires_in=3600
        )


class FakeClient:
    """Minimal client exposing info, auth, and close behavior."""

    def __init__(
        self,
        scheme: AuthScheme,
        error: Exception | None = None,
        info_error: Exception | None = None,
        server_id: uuid.UUID | None = None,
        control_plane_api_url: str | None = None,
    ) -> None:
        """Initialize the fake client."""
        self.auth = FakeAuthResource(error)
        self.closed = False
        info = ServerInfoResponse(
            id=server_id,
            version="0.21.0",
            auth_scheme=scheme,
            control_plane_api_url=control_plane_api_url,
        )

        class InfoResource:
            async def get(self) -> ServerInfoResponse:
                if info_error:
                    raise info_error
                return info

        self.info = InfoResource()

    async def close(self) -> None:
        """Record client closure."""
        self.closed = True


WORKSPACE_ID = uuid.UUID("11111111-1111-1111-1111-111111111111")


def managed_workspace(
    *, status: str = "available", server_url: str | None = "https://managed.example.com"
) -> ControlPlaneWorkspace:
    """Build the minimal managed workspace response used by login tests."""
    return ControlPlaneWorkspace.model_validate(
        {
            "id": str(WORKSPACE_ID),
            "name": "my-kitaru",
            "workspace_type": "kitaru",
            "status": status,
            "kitaru_service": {"status": {"server_url": server_url}},
        }
    )


async def test_bare_login_connects_to_the_browser_selected_managed_workspace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Resolve and select the Kitaru workspace chosen in the device flow."""
    credential_store = CredentialStore(tmp_path / "credentials.json")
    client = FakeClient(
        AuthScheme.CONTROL_PLANE,
        server_id=WORKSPACE_ID,
        control_plane_api_url=MANAGED_CLOUD_API_URL,
    )
    calls: list[object] = []

    class FakeManagedCloudSession:
        def __init__(
            self, api_url: str, store: CredentialStore, timeout: float
        ) -> None:
            calls.append((api_url, store, timeout))

        async def device_login_with_metadata(
            self, *, open_browser: bool, prompt: object
        ) -> ControlPlaneToken:
            calls.append(("device", open_browser, prompt))
            return ControlPlaneToken(
                access_token="control-plane-token",
                expires_in=3600,
                device_metadata={"tenant_id": str(WORKSPACE_ID)},
            )

        async def get_workspace(
            self, workspace_id: uuid.UUID, access_token: str
        ) -> ControlPlaneWorkspace:
            calls.append(("workspace", workspace_id, access_token))
            return managed_workspace()

        async def close(self) -> None:
            calls.append("closed")

    monkeypatch.setattr(auth, "ControlPlaneSession", FakeManagedCloudSession)
    monkeypatch.setattr(auth, "KitaruAPIClient", lambda **_: client)

    result = await auth.login(
        server=None,
        local=False,
        username=None,
        password_stdin=False,
        api_key_stdin=False,
        credential_store=credential_store,
        timeout=30,
        non_interactive=False,
        no_browser=True,
        stdin=io.StringIO(),
    )

    assert calls[0] == (MANAGED_CLOUD_API_URL, credential_store, 30)
    assert calls[1] == ("device", False, auth._show_device_prompt)
    assert calls[2] == ("workspace", WORKSPACE_ID, "control-plane-token")
    assert calls[3] == "closed"
    assert client.auth.control_plane_exchanged == "control-plane-token"
    assert get_server_url() == "https://managed.example.com"
    assert result.item == {
        "server_url": "https://managed.example.com",
        "workspace_id": str(WORKSPACE_ID),
        "workspace_name": "my-kitaru",
        "auth_scheme": "control_plane",
        "authentication": "authenticated",
        "credential_kind": "device",
        "credential_stored": True,
    }
    stored = credential_store.get("https://managed.example.com")
    assert stored is not None
    assert stored.control_plane_api_url == MANAGED_CLOUD_API_URL


async def test_bare_login_rejects_non_interactive_use_before_connecting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Do not start browser authorization for a non-interactive invocation."""
    monkeypatch.setattr(
        auth,
        "ControlPlaneSession",
        lambda *_args, **_kwargs: pytest.fail("control plane should not be contacted"),
    )

    with pytest.raises(CLIError) as raised:
        await auth.login(
            server=None,
            local=False,
            username=None,
            password_stdin=False,
            api_key_stdin=False,
            credential_store=CredentialStore(tmp_path / "credentials.json"),
            timeout=30,
            non_interactive=True,
            no_browser=True,
            stdin=io.StringIO(),
        )

    assert raised.value.kind == "interaction_required"
    assert "interactive terminal" in str(raised.value.hint)


async def test_bare_login_rejects_a_mismatched_workspace_server(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep the current server when discovery resolves an inconsistent server."""

    class FakeManagedCloudSession:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

        async def device_login_with_metadata(self, **_kwargs) -> ControlPlaneToken:
            return ControlPlaneToken(
                access_token="control-plane-token",
                expires_in=3600,
                device_metadata={"tenant_id": str(WORKSPACE_ID)},
            )

        async def close(self) -> None:
            pass

    async def return_workspace(*_args, **_kwargs) -> ControlPlaneWorkspace:
        return managed_workspace()

    async def reject_exchange(*_args, **_kwargs) -> None:
        pytest.fail("credentials should not be exchanged")

    monkeypatch.setattr(auth, "ControlPlaneSession", FakeManagedCloudSession)
    monkeypatch.setattr(auth, "_wait_for_managed_workspace", return_workspace)
    monkeypatch.setattr(
        auth,
        "KitaruAPIClient",
        lambda **_: FakeClient(AuthScheme.CONTROL_PLANE, server_id=uuid.uuid4()),
    )
    monkeypatch.setattr(auth, "exchange_control_plane_credential", reject_exchange)
    set_server_url("https://existing.example.com")

    with pytest.raises(CLIError) as raised:
        await auth.login(
            server=None,
            local=False,
            username=None,
            password_stdin=False,
            api_key_stdin=False,
            credential_store=CredentialStore(tmp_path / "credentials.json"),
            timeout=30,
            non_interactive=False,
            no_browser=True,
            stdin=io.StringIO(),
        )

    assert raised.value.kind == "invalid_configuration"
    assert "inconsistent Kitaru connection details" in raised.value.message
    assert get_server_url() == "https://existing.example.com"


async def test_bare_login_requires_the_browser_to_select_a_workspace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Explain a successful device grant that carries no workspace selection."""

    class FakeManagedCloudSession:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

        async def device_login_with_metadata(self, **_kwargs) -> ControlPlaneToken:
            return ControlPlaneToken(
                access_token="control-plane-token",
                expires_in=3600,
                device_metadata={},
            )

        async def close(self) -> None:
            pass

    monkeypatch.setattr(auth, "ControlPlaneSession", FakeManagedCloudSession)
    set_server_url("https://existing.example.com")

    with pytest.raises(CLIError) as raised:
        await auth.login(
            server=None,
            local=False,
            username=None,
            password_stdin=False,
            api_key_stdin=False,
            credential_store=CredentialStore(tmp_path / "credentials.json"),
            timeout=30,
            non_interactive=False,
            no_browser=True,
            stdin=io.StringIO(),
        )

    assert raised.value.kind == "invalid_configuration"
    assert "without a selected Kitaru workspace" in raised.value.message
    assert get_server_url() == "https://existing.example.com"


async def test_bare_login_waits_for_a_pending_workspace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Poll a newly created workspace until its Kitaru service is available."""
    responses = [
        managed_workspace(status="pending"),
        managed_workspace(server_url=None),
        managed_workspace(),
    ]
    delays: list[float] = []

    class FakeManagedCloudSession:
        async def get_workspace(
            self, workspace_id: uuid.UUID, access_token: str
        ) -> ControlPlaneWorkspace:
            assert workspace_id == WORKSPACE_ID
            assert access_token == "control-plane-token"
            return responses.pop(0)

    async def record_sleep(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr(auth.asyncio, "sleep", record_sleep)
    monkeypatch.setattr(auth, "write_interaction", lambda _message: None)
    workspace = await auth._wait_for_managed_workspace(
        cast(ControlPlaneSession, FakeManagedCloudSession()),
        WORKSPACE_ID,
        "control-plane-token",
    )

    assert workspace.status == "available"
    assert delays == [auth._WORKSPACE_POLL_INTERVAL_SECONDS] * 2


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


async def test_volume_logout_cleans_default_orphans_without_runtime_state(
    tmp_path, monkeypatch
) -> None:
    """A targetless volume logout cleans labeled resources without state."""
    monkeypatch.setattr(auth.local_runtime, "is_local_runtime_url", lambda _: False)
    monkeypatch.setattr(auth.local_runtime, "has_local_runtime_state", lambda: False)
    captured: dict[str, object] = {}

    async def fake_stop(*, delete_volumes: bool):
        captured["delete_volumes"] = delete_volumes
        return {
            "server_url": "http://localhost:8000",
            "deployment": "deleted",
            "data_deleted": True,
        }

    monkeypatch.setattr(auth.local_runtime, "stop_local_runtime", fake_stop)

    result = await auth.logout(
        server_url="http://localhost:9010",
        all_servers=False,
        delete_volumes=True,
        allow_orphan_cleanup=True,
        credential_store=CredentialStore(tmp_path / "credentials.json"),
    )

    assert captured["delete_volumes"] is True
    assert result.item["deployment"] == "deleted"


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
