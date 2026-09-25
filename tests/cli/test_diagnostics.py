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
"""Status and doctor aggregate behavior."""

import json
import os
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

from kitaru.api_models.v1.info import AuthScheme, ServerInfoResponse
from kitaru.cli import app as app_module
from kitaru.cli import diagnostics
from kitaru.cli.config import ResolvedCredential, ResolvedTarget
from kitaru.cli.output import CLIError
from kitaru.cli.skill_discovery import INSTALL_COMMAND
from kitaru.client.credential_store import CredentialStore
from kitaru.client.exceptions import APIError, NotFoundError


class FakeWorkers:
    """Worker resource returning a stable liveness snapshot."""

    async def iter(self):
        """Yield two live workers and one stale worker."""
        for live in (True, False, True):
            yield SimpleNamespace(live=live)

    async def list(self) -> list[object]:
        """Return an empty worker collection for authentication checks."""
        return []


class FakeClient:
    """Minimal unauthenticated server used by status."""

    def __init__(self, version: str = "0.21.0") -> None:
        """Initialize resources and closure state."""
        info = ServerInfoResponse(
            version=version,
            auth_scheme=AuthScheme.NONE,
            dashboard_url="https://dashboard.example.com",
        )

        class InfoResource:
            async def get(self) -> ServerInfoResponse:
                return info

        self.info: Any = InfoResource()
        self.workers = FakeWorkers()
        self.closed = False

    async def close(self) -> None:
        """Record client closure."""
        self.closed = True


async def test_status_reports_provenance_and_live_worker_count(
    tmp_path, monkeypatch
) -> None:
    """Status composes local provenance with unauthenticated server data."""
    client = FakeClient()
    monkeypatch.setattr(diagnostics, "build_api_client", lambda *args: client)
    credential_store = CredentialStore(tmp_path / "credentials.json")

    result = await diagnostics.status(
        target=ResolvedTarget("https://api.example.com", "environment"),
        credential_store=credential_store,
        timeout=30,
    )

    assert result.exit_code == 0
    assert result.item["server_source"] == "environment"
    assert result.item["authentication"] == "not_required"
    assert result.item["live_worker_count"] == 2
    assert result.item["credential_status"]["source"] == "none"
    assert client.closed is True


@pytest.mark.parametrize(
    ("client_version", "server_version", "status", "warning"),
    [
        ("1.2.0", "1.9.0", "compatible", None),
        ("1.2.0", "2.0.0", "major_version_mismatch", "major versions differ"),
        ("invalid", "1.0.0", "unknown", "could not be determined"),
        ("1.0.0", "invalid", "unknown", "could not be determined"),
    ],
)
async def test_status_and_info_add_non_blocking_compatibility_warnings(
    tmp_path,
    monkeypatch,
    client_version: str,
    server_version: str,
    status: str,
    warning: str | None,
) -> None:
    """Compatibility diagnostics warn without changing successful exits."""
    monkeypatch.setattr(diagnostics, "package_version", lambda: client_version)
    target = ResolvedTarget("https://api.example.com", "explicit")
    credential_store = CredentialStore(tmp_path / "credentials.json")

    status_client = FakeClient(server_version)
    monkeypatch.setattr(diagnostics, "build_api_client", lambda *args: status_client)
    status_result = await diagnostics.status(
        target=target,
        credential_store=credential_store,
        timeout=30,
    )

    info_client = FakeClient(server_version)
    monkeypatch.setattr(diagnostics, "build_api_client", lambda *args: info_client)
    info_result = await diagnostics.info(
        target=target,
        credential_store=credential_store,
        timeout=30,
    )

    for result in (status_result, info_result):
        assert result.exit_code == 0
        assert result.item["compatibility"]["status"] == status
        if warning is None:
            assert result.warnings == []
        else:
            assert len(result.warnings) == 1
            assert warning in result.warnings[0]
            assert "not blocked" in result.warnings[0]


async def _raise_bare_404() -> ServerInfoResponse:
    raise NotFoundError(404, "")


MISSING_SERVER_MESSAGE = (
    "The server at https://gone.example.com did not answer as a Kitaru server "
    "(HTTP 404). It may have been deleted or the URL may be wrong."
)


@pytest.mark.parametrize("command", [diagnostics.status, diagnostics.info])
async def test_info_endpoint_404_is_reported_as_a_missing_server(
    tmp_path, monkeypatch, command
) -> None:
    """A 404 from the info endpoint means the URL is not a Kitaru server."""
    client = FakeClient()
    client.info = SimpleNamespace(get=_raise_bare_404)
    monkeypatch.setattr(diagnostics, "build_api_client", lambda *args: client)

    with pytest.raises(CLIError) as excinfo:
        await command(
            target=ResolvedTarget("https://gone.example.com", "stored"),
            credential_store=CredentialStore(tmp_path / "credentials.json"),
            timeout=30,
        )

    error = excinfo.value
    assert error.kind == "invalid_configuration"
    assert error.message == MISSING_SERVER_MESSAGE
    assert error.details == {
        "server_url": "https://gone.example.com",
        "status_code": 404,
    }
    assert error.hint is not None
    assert "kitaru doctor" in error.hint
    assert "kitaru login" in error.hint
    assert client.closed is True


async def test_doctor_explains_a_404_from_the_info_endpoint(
    tmp_path, monkeypatch
) -> None:
    """Doctor's server_info check carries the same explanation as status."""

    async def probe_404(*args) -> int:
        return 404

    client = FakeClient()
    client.info = SimpleNamespace(get=_raise_bare_404)
    monkeypatch.setattr(diagnostics, "_probe", probe_404)
    monkeypatch.setattr(diagnostics, "build_api_client", lambda *args: client)

    result = await diagnostics.doctor(
        credential_store=CredentialStore(tmp_path / "credentials.json"),
        explicit_server="https://gone.example.com",
        timeout=0.1,
    )

    assert result.item["healthy"] is False
    server_info = next(c for c in result.item["checks"] if c["name"] == "server_info")
    assert server_info["status"] == "fail"
    assert server_info["detail"] == MISSING_SERVER_MESSAGE


async def test_info_reports_runtime_and_server_details(tmp_path, monkeypatch) -> None:
    """Info combines runtime metadata with the resolved server response."""
    client = FakeClient()
    monkeypatch.setattr(diagnostics, "build_api_client", lambda *args: client)

    result = await diagnostics.info(
        target=ResolvedTarget("https://api.example.com", "explicit"),
        credential_store=CredentialStore(tmp_path / "credentials.json"),
        timeout=30,
    )

    assert result.exit_code == 0
    assert result.item["server_url"] == "https://api.example.com"
    assert result.item["server_source"] == "explicit"
    assert result.item["server"]["version"] == "0.21.0"
    assert result.item["python_version"]
    assert client.closed is True


async def test_doctor_reports_every_check_when_no_server_is_configured(
    tmp_path, monkeypatch
) -> None:
    """Doctor keeps running after resolution failure and preserves check order."""
    monkeypatch.delenv("KITARU_API_URL", raising=False)
    result = await diagnostics.doctor(
        credential_store=CredentialStore(tmp_path / "credentials.json"),
        explicit_server=None,
        timeout=0.1,
    )

    assert result.exit_code == 2
    assert result.item["healthy"] is False
    checks = result.item["checks"]
    assert [check["name"] for check in checks] == [
        "config",
        "credentials",
        "server_resolution",
        "liveness",
        "readiness",
        "server_info",
        "compatibility",
        "authentication",
        "worker_extra",
        "kitaru_skills",
        "uv",
    ]
    assert checks[2]["status"] == "fail"
    assert all(check["status"] == "skip" for check in checks[3:8])
    assert checks[6]["required"] is False


@pytest.mark.parametrize(
    ("server_version", "expected_status"),
    [
        ("1.8.0", "pass"),
        ("2.0.0", "warn"),
        ("invalid", "warn"),
    ],
)
async def test_doctor_compatibility_check_is_non_failing(
    tmp_path, monkeypatch, server_version: str, expected_status: str
) -> None:
    """Doctor reports compatibility immediately after server info without gating."""

    async def successful_probe(*args) -> int:
        return 200

    monkeypatch.setattr(diagnostics, "package_version", lambda: "1.2.0")
    monkeypatch.setattr(diagnostics, "_probe", successful_probe)
    monkeypatch.setattr(
        diagnostics,
        "build_api_client",
        lambda *args: FakeClient(server_version),
    )

    result = await diagnostics.doctor(
        credential_store=CredentialStore(tmp_path / "credentials.json"),
        explicit_server="https://api.example.com",
        timeout=0.1,
    )

    assert result.exit_code == 0
    checks = result.item["checks"]
    assert [check["name"] for check in checks[5:8]] == [
        "server_info",
        "compatibility",
        "authentication",
    ]
    compatibility = checks[6]
    assert compatibility["status"] == expected_status
    assert compatibility["required"] is False
    if expected_status == "warn":
        assert "not blocked" in compatibility["detail"]


async def test_doctor_skips_compatibility_when_server_info_is_unavailable(
    tmp_path, monkeypatch
) -> None:
    """A server-info failure skips compatibility and retains the server exit."""

    async def successful_probe(*args) -> int:
        return 200

    class FailingInfo:
        async def get(self) -> ServerInfoResponse:
            raise httpx.ConnectError("server info unavailable")

    client = FakeClient()
    client.info = FailingInfo()
    monkeypatch.setattr(diagnostics, "_probe", successful_probe)
    monkeypatch.setattr(diagnostics, "build_api_client", lambda *args: client)

    result = await diagnostics.doctor(
        credential_store=CredentialStore(tmp_path / "credentials.json"),
        explicit_server="https://api.example.com",
        timeout=0.1,
    )

    assert result.exit_code == 6
    compatibility = next(
        check for check in result.item["checks"] if check["name"] == "compatibility"
    )
    assert compatibility == {
        "name": "compatibility",
        "status": "skip",
        "required": False,
        "detail": "Server info unavailable.",
    }


def test_doctor_continues_without_reusing_malformed_credentials(
    tmp_path, monkeypatch, capsys
) -> None:
    """Malformed credentials do not block independent explicit-server checks."""
    fake_token = "FAKE-TOKEN-MUST-NOT-LEAK"
    server_url = "https://api.example.com"
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    if os.name == "posix":
        config_dir.chmod(0o700)
    credentials_path = config_dir / "credentials.json"
    credentials_path.write_text(
        json.dumps(
            {
                server_url: {
                    "url": server_url,
                    "device_id": fake_token,
                }
            }
        ),
        encoding="utf-8",
    )
    if os.name == "posix":
        credentials_path.chmod(0o600)

    async def successful_probe(*args) -> int:
        return 200

    resolved_credentials: list[ResolvedCredential] = []

    def build_client(
        _server_url: str, credential: ResolvedCredential, *_args: Any
    ) -> FakeClient:
        resolved_credentials.append(credential)
        return FakeClient()

    monkeypatch.setenv("KITARU_CONFIG_DIR", str(config_dir))
    monkeypatch.delenv("KITARU_API_KEY", raising=False)
    monkeypatch.setattr(diagnostics, "_probe", successful_probe)
    monkeypatch.setattr(diagnostics, "build_api_client", build_client)

    assert app_module.main(["doctor", "--server", server_url, "--output", "json"]) == 2

    captured = capsys.readouterr()
    assert fake_token not in captured.out
    assert fake_token not in captured.err
    assert [credential.source for credential in resolved_credentials] == ["none"]
    payload = json.loads(captured.out)
    checks = payload["item"]["checks"]
    assert [check["name"] for check in checks] == [
        "config",
        "credentials",
        "server_resolution",
        "liveness",
        "readiness",
        "server_info",
        "compatibility",
        "authentication",
        "worker_extra",
        "kitaru_skills",
        "uv",
    ]
    credential_check = checks[1]
    assert credential_check["detail"] == (
        f"Credential document at {credentials_path} is invalid."
    )


async def test_doctor_worker_extra_hint_names_cli_and_worker(
    tmp_path, monkeypatch
) -> None:
    """Doctor recommends the same install extra required by worker start."""
    monkeypatch.delenv("KITARU_API_URL", raising=False)
    monkeypatch.setattr(diagnostics.importlib.util, "find_spec", lambda name: None)

    result = await diagnostics.doctor(
        credential_store=CredentialStore(tmp_path / "credentials.json"),
        explicit_server=None,
        timeout=0.1,
    )

    worker_extra = next(
        check for check in result.item["checks"] if check["name"] == "worker_extra"
    )
    assert worker_extra["status"] == "warn"
    assert worker_extra["detail"] == "Install kitaru[cli,worker] to run workers."


async def test_doctor_reports_missing_kitaru_skills_without_failing(
    tmp_path, monkeypatch
) -> None:
    """Missing agent skills are useful tooling guidance, not a health failure."""
    monkeypatch.delenv("KITARU_API_URL", raising=False)
    monkeypatch.setattr(
        diagnostics,
        "get_kitaru_skill_status",
        lambda: {
            "installed": False,
            "skill_count": 0,
            "skills": [],
            "installations": [],
            "locations_checked": ["/tmp/project/.agents/skills"],
        },
    )

    result = await diagnostics.doctor(
        credential_store=CredentialStore(tmp_path / "credentials.json"),
        explicit_server=None,
        timeout=0.1,
    )

    check = next(
        item for item in result.item["checks"] if item["name"] == "kitaru_skills"
    )
    assert check["status"] == "warn"
    assert check["required"] is False
    assert INSTALL_COMMAND in check["detail"]
    assert check["data"]["installed"] is False
    assert result.exit_code == 2


async def test_doctor_reports_detected_kitaru_skills(tmp_path, monkeypatch) -> None:
    """Doctor exposes discovered skill names and locations to machines."""
    monkeypatch.delenv("KITARU_API_URL", raising=False)
    status = {
        "installed": True,
        "skill_count": 2,
        "skills": ["kitaru-investigation", "kitaru-replay-lab"],
        "installations": [
            {
                "name": "kitaru-investigation",
                "scope": "user",
                "host": "codex",
                "path": "/home/user/.codex/skills/kitaru-investigation",
            }
        ],
        "locations_checked": ["/home/user/.codex/skills"],
    }
    monkeypatch.setattr(diagnostics, "get_kitaru_skill_status", lambda: status)

    result = await diagnostics.doctor(
        credential_store=CredentialStore(tmp_path / "credentials.json"),
        explicit_server=None,
        timeout=0.1,
    )

    check = next(
        item for item in result.item["checks"] if item["name"] == "kitaru_skills"
    )
    assert check["status"] == "pass"
    assert check["detail"] == (
        "2 Kitaru agent skills detected: kitaru-investigation, kitaru-replay-lab."
    )
    assert check["data"] == status


class AuthRequiredClient(FakeClient):
    """Server requiring local auth whose worker reads can be made to fail."""

    def __init__(self, worker_error: Exception | None = None) -> None:
        """Initialize a local-auth server with an optional worker-read error."""
        super().__init__()
        info = ServerInfoResponse(version="0.21.0", auth_scheme=AuthScheme.LOCAL)

        class InfoResource:
            async def get(self) -> ServerInfoResponse:
                return info

        class Workers(FakeWorkers):
            async def iter(self):
                if worker_error is not None:
                    raise worker_error
                yield SimpleNamespace(live=True)

            async def list(self) -> list[object]:
                if worker_error is not None:
                    raise worker_error
                return []

        self.info = InfoResource()
        self.workers = Workers()


async def test_status_reports_a_rejected_credential_as_a_warning(
    tmp_path, monkeypatch
) -> None:
    """A 401 on the worker read marks the credential rejected, not the command."""
    client = AuthRequiredClient(APIError(401, "expired"))
    monkeypatch.setattr(diagnostics, "build_api_client", lambda *args: client)
    credential_store = CredentialStore(tmp_path / "credentials.json")
    credential_store.set_api_key("https://api.example.com", "KITKEY_rejected")

    result = await diagnostics.status(
        target=ResolvedTarget("https://api.example.com", "stored"),
        credential_store=credential_store,
        timeout=30,
    )

    assert result.exit_code == 0
    assert result.item["authentication"] == "rejected"
    assert result.item["credential_status"]["kind"] == "api_key"
    assert "KITKEY_rejected" not in json.dumps(result.item)
    assert "credential was rejected" in result.warnings[0]
    assert client.closed is True


async def test_status_propagates_non_auth_server_errors(tmp_path, monkeypatch) -> None:
    """A server failure during the worker read is not mislabeled as bad auth."""
    client = AuthRequiredClient(APIError(500, "boom"))
    monkeypatch.setattr(diagnostics, "build_api_client", lambda *args: client)
    credential_store = CredentialStore(tmp_path / "credentials.json")
    credential_store.set_api_key("https://api.example.com", "KITKEY_valid")

    with pytest.raises(APIError) as raised:
        await diagnostics.status(
            target=ResolvedTarget("https://api.example.com", "stored"),
            credential_store=credential_store,
            timeout=30,
        )

    assert raised.value.status_code == 500
    assert client.closed is True


async def test_status_warns_when_auth_is_required_but_no_credential_exists(
    tmp_path, monkeypatch
) -> None:
    """Status tells the user to log in instead of reporting zero workers."""
    client = AuthRequiredClient()
    monkeypatch.setattr(diagnostics, "build_api_client", lambda *args: client)

    result = await diagnostics.status(
        target=ResolvedTarget("https://api.example.com", "explicit"),
        credential_store=CredentialStore(tmp_path / "credentials.json"),
        timeout=30,
    )

    assert result.item["authentication"] == "missing"
    assert result.item["live_worker_count"] is None
    assert "no credential is available" in result.warnings[0]


async def _probe_ok(*args) -> int:
    return 200


@pytest.mark.parametrize(
    ("stored_key", "worker_error", "status", "exit_code"),
    [
        (False, None, "fail", 3),
        (True, None, "pass", 0),
        (True, APIError(403, "forbidden"), "fail", 3),
        (True, APIError(500, "boom"), "fail", 6),
        (True, httpx.ConnectError("unreachable"), "fail", 6),
    ],
)
async def test_doctor_authentication_check_maps_failures_to_exit_codes(
    tmp_path,
    monkeypatch,
    stored_key: bool,
    worker_error: Exception | None,
    status: str,
    exit_code: int,
) -> None:
    """Auth rejections exit 3 while server faults during the check exit 6."""
    monkeypatch.setattr(diagnostics, "_probe", _probe_ok)
    monkeypatch.setattr(
        diagnostics, "build_api_client", lambda *args: AuthRequiredClient(worker_error)
    )
    credential_store = CredentialStore(tmp_path / "credentials.json")
    if stored_key:
        credential_store.set_api_key("https://api.example.com", "KITKEY_test")

    result = await diagnostics.doctor(
        credential_store=credential_store,
        explicit_server="https://api.example.com",
        timeout=0.1,
    )

    authentication = next(
        check for check in result.item["checks"] if check["name"] == "authentication"
    )
    assert authentication["status"] == status
    assert result.exit_code == exit_code
    assert result.item["healthy"] is (exit_code == 0)


async def test_doctor_reports_an_unreachable_server_as_a_server_failure(
    tmp_path, monkeypatch
) -> None:
    """Connection errors from health probes fail liveness and readiness with exit 6."""

    async def refuse(*args) -> int:
        raise httpx.ConnectError("connection refused")

    monkeypatch.setattr(diagnostics, "_probe", refuse)
    monkeypatch.setattr(diagnostics, "build_api_client", lambda *args: FakeClient())

    result = await diagnostics.doctor(
        credential_store=CredentialStore(tmp_path / "credentials.json"),
        explicit_server="https://api.example.com",
        timeout=0.1,
    )

    assert result.exit_code == 6
    probes = [
        c for c in result.item["checks"] if c["name"] in {"liveness", "readiness"}
    ]
    assert [c["status"] for c in probes] == ["fail", "fail"]
    assert all("connection refused" in c["detail"] for c in probes)


@pytest.mark.skipif(os.name != "posix", reason="POSIX file modes only")
async def test_doctor_fails_world_readable_credentials(tmp_path, monkeypatch) -> None:
    """A credential file other users can read is a configuration failure."""
    config_dir = tmp_path / "config"
    config_dir.mkdir(mode=0o700)
    config_dir.chmod(0o700)
    credentials_path = config_dir / "credentials.json"
    credentials_path.write_text("{}", encoding="utf-8")
    credentials_path.chmod(0o644)
    monkeypatch.setenv("KITARU_CONFIG_DIR", str(config_dir))

    result = await diagnostics.doctor(
        credential_store=CredentialStore(credentials_path),
        explicit_server=None,
        timeout=0.1,
    )

    credentials = next(c for c in result.item["checks"] if c["name"] == "credentials")
    assert credentials["status"] == "fail"
    assert "expected 0o600" in credentials["detail"]
    assert result.exit_code == 2
