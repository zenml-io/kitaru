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
from kitaru.cli.skill_discovery import INSTALL_COMMAND
from kitaru.client.credential_store import CredentialStore


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
