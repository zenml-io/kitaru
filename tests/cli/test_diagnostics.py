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

from types import SimpleNamespace

from kitaru.api_models.v1.info import AuthScheme, ServerInfoResponse
from kitaru.cli import diagnostics
from kitaru.cli.config import ConfigStore, ResolvedTarget
from kitaru.client.credential_store import CredentialStore


class FakeWorkers:
    """Worker resource returning a stable liveness snapshot."""

    async def iter(self):
        """Yield two live workers and one stale worker."""
        for live in (True, False, True):
            yield SimpleNamespace(live=live)


class FakeClient:
    """Minimal unauthenticated server used by status."""

    def __init__(self) -> None:
        """Initialize resources and closure state."""
        info = ServerInfoResponse(
            version="0.21.0",
            auth_scheme=AuthScheme.NONE,
            dashboard_url="https://dashboard.example.com",
        )

        class InfoResource:
            async def get(self) -> ServerInfoResponse:
                return info

        self.info = InfoResource()
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
        target=ResolvedTarget("https://api.example.com", "active_context", "Prod"),
        credential_store=credential_store,
        timeout=30,
    )

    assert result.exit_code == 0
    assert result.item["server_source"] == "active_context"
    assert result.item["context"] == "Prod"
    assert result.item["authentication"] == "not_required"
    assert result.item["live_worker_count"] == 2
    assert result.item["credential_status"]["source"] == "none"
    assert client.closed is True


async def test_info_reports_runtime_and_server_details(tmp_path, monkeypatch) -> None:
    """Info combines runtime metadata with the resolved server response."""
    client = FakeClient()
    monkeypatch.setattr(diagnostics, "build_api_client", lambda *args: client)

    result = await diagnostics.info(
        target=ResolvedTarget("https://api.example.com", "explicit", "Prod"),
        credential_store=CredentialStore(tmp_path / "credentials.json"),
        timeout=30,
    )

    assert result.exit_code == 0
    assert result.item["server_url"] == "https://api.example.com"
    assert result.item["server_source"] == "explicit"
    assert result.item["context"] == "Prod"
    assert result.item["server"]["version"] == "0.21.0"
    assert result.item["python_version"]
    assert client.closed is True


async def test_doctor_reports_every_check_when_no_server_is_configured(
    tmp_path, monkeypatch
) -> None:
    """Doctor keeps running after resolution failure and preserves check order."""
    monkeypatch.delenv("KITARU_API_URL", raising=False)
    result = await diagnostics.doctor(
        config_store=ConfigStore(tmp_path / "config.json"),
        credential_store=CredentialStore(tmp_path / "credentials.json"),
        explicit_server=None,
        context_name=None,
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
        "authentication",
        "worker_extra",
        "uv",
    ]
    assert checks[2]["status"] == "fail"
    assert all(check["status"] == "skip" for check in checks[3:7])
