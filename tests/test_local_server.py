"""Unit tests for src/kitaru/_local_server.py lifecycle helpers."""

from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

import kitaru._local_server as ls_mod
from kitaru._local_server import (
    LocalServerConnectionResult,
    _deploy_and_connect,
    _resolve_bundled_ui_dir,
    _track_local_server_started,
    start_or_connect_local_server,
)
from kitaru.analytics import AnalyticsEvent
from kitaru.errors import KitaruBackendError

# ---------------------------------------------------------------------------
# Test-local helpers
# ---------------------------------------------------------------------------


class _FakeDeploymentConfig:
    """Captures constructor kwargs so tests can assert config fields."""

    def __init__(self, **kwargs: Any) -> None:
        self.provider = kwargs["provider"]
        self.ip_address = kwargs["ip_address"]
        self.port = kwargs["port"]


_FAKE_PROVIDER = SimpleNamespace(DAEMON="daemon")


def _make_local_server(
    url: str | None, *, port: int | None = None, is_running: bool = True
) -> SimpleNamespace:
    """Build a minimal object compatible with existing-server introspection."""
    parsed_port = port
    if parsed_port is None and url:
        from urllib.parse import urlparse

        parsed_port = urlparse(url).port
    return SimpleNamespace(
        status=SimpleNamespace(url=url),
        config=SimpleNamespace(url=url, port=parsed_port),
        is_running=is_running,
    )


def _make_runtime_tuple(
    deployer: MagicMock,
    local_server: Any = None,
) -> tuple[type, type, Any, Any]:
    """Build the 4-tuple returned by _load_local_server_runtime."""
    deployer_cls = MagicMock(return_value=deployer)
    return (deployer_cls, _FakeDeploymentConfig, _FAKE_PROVIDER, lambda: local_server)


# ---------------------------------------------------------------------------
# _resolve_bundled_ui_dir
# ---------------------------------------------------------------------------


class TestResolveBundledUiDir:
    def test_returns_path_when_dist_and_index_exist(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake_pkg = tmp_path / "pkg"
        fake_pkg.mkdir()
        dist = fake_pkg / "_ui" / "dist"
        dist.mkdir(parents=True)
        (dist / "index.html").write_text("<html/>")
        monkeypatch.setattr(ls_mod, "__file__", str(fake_pkg / "_local_server.py"))

        result = _resolve_bundled_ui_dir()

        assert result == dist

    def test_returns_none_when_index_html_missing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake_pkg = tmp_path / "pkg"
        fake_pkg.mkdir()
        dist = fake_pkg / "_ui" / "dist"
        dist.mkdir(parents=True)
        monkeypatch.setattr(ls_mod, "__file__", str(fake_pkg / "_local_server.py"))

        assert _resolve_bundled_ui_dir() is None

    def test_returns_none_when_dist_dir_missing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake_pkg = tmp_path / "pkg"
        fake_pkg.mkdir()
        monkeypatch.setattr(ls_mod, "__file__", str(fake_pkg / "_local_server.py"))

        assert _resolve_bundled_ui_dir() is None


# ---------------------------------------------------------------------------
# _track_local_server_started
# ---------------------------------------------------------------------------


class TestTrackLocalServerStarted:
    @patch("kitaru.analytics.track")
    def test_emits_correct_event_and_metadata(self, mock_track: MagicMock) -> None:
        result = LocalServerConnectionResult(
            url="http://127.0.0.1:8383", action="started"
        )

        _track_local_server_started(result)

        mock_track.assert_called_once_with(
            AnalyticsEvent.LOCAL_SERVER_STARTED, {"action": "started"}
        )


# ---------------------------------------------------------------------------
# _deploy_and_connect
# ---------------------------------------------------------------------------


class TestDeployAndConnect:
    def test_sets_env_vars_during_deploy_and_cleans_up(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("ZENML_SERVER_DASHBOARD_FILES_PATH", raising=False)
        monkeypatch.delenv("ZENML_DEFAULT_ANALYTICS_SOURCE", raising=False)

        captured_env: dict[str, str | None] = {}
        fake_ui_dir = Path("/fake/ui/dist")

        def capture_env_during_deploy(_config: Any, **_kw: Any) -> SimpleNamespace:
            captured_env["dashboard"] = os.environ.get(
                "ZENML_SERVER_DASHBOARD_FILES_PATH"
            )
            captured_env["analytics"] = os.environ.get("ZENML_DEFAULT_ANALYTICS_SOURCE")
            return SimpleNamespace(status=SimpleNamespace(url="http://127.0.0.1:9090"))

        deployer = MagicMock()
        deployer.deploy_server.side_effect = capture_env_during_deploy

        with patch.object(ls_mod, "_resolve_bundled_ui_dir", return_value=fake_ui_dir):
            result = _deploy_and_connect(
                deployer=deployer,
                deployment_config_cls=_FakeDeploymentConfig,
                provider_type=_FAKE_PROVIDER,
                port=9090,
                timeout=30,
                action="started",
            )

        assert captured_env["dashboard"] == str(fake_ui_dir)
        assert captured_env["analytics"] == "kitaru-api"

        assert os.environ.get("ZENML_SERVER_DASHBOARD_FILES_PATH") is None
        assert os.environ.get("ZENML_DEFAULT_ANALYTICS_SOURCE") == "kitaru-python"

        assert result == LocalServerConnectionResult(
            url="http://127.0.0.1:9090", action="started"
        )
        deployer.connect_to_server.assert_called_once()

    def test_cleans_up_env_on_deploy_failure(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("ZENML_SERVER_DASHBOARD_FILES_PATH", raising=False)
        monkeypatch.delenv("ZENML_DEFAULT_ANALYTICS_SOURCE", raising=False)

        deployer = MagicMock()
        deployer.deploy_server.side_effect = RuntimeError("connection refused")

        with (
            patch.object(
                ls_mod, "_resolve_bundled_ui_dir", return_value=Path("/fake/ui/dist")
            ),
            pytest.raises(KitaruBackendError, match="failed to start"),
        ):
            _deploy_and_connect(
                deployer=deployer,
                deployment_config_cls=_FakeDeploymentConfig,
                provider_type=_FAKE_PROVIDER,
                port=8383,
                timeout=30,
                action="started",
            )

        assert os.environ.get("ZENML_SERVER_DASHBOARD_FILES_PATH") is None
        assert os.environ.get("ZENML_DEFAULT_ANALYTICS_SOURCE") == "kitaru-python"

    def test_cleans_up_env_on_connect_failure(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("ZENML_SERVER_DASHBOARD_FILES_PATH", raising=False)
        monkeypatch.delenv("ZENML_DEFAULT_ANALYTICS_SOURCE", raising=False)

        deployer = MagicMock()
        deployer.deploy_server.return_value = SimpleNamespace(
            status=SimpleNamespace(url="http://127.0.0.1:8383")
        )
        deployer.connect_to_server.side_effect = RuntimeError("refused")

        with (
            patch.object(
                ls_mod, "_resolve_bundled_ui_dir", return_value=Path("/fake/ui/dist")
            ),
            pytest.raises(KitaruBackendError, match="failed to start"),
        ):
            _deploy_and_connect(
                deployer=deployer,
                deployment_config_cls=_FakeDeploymentConfig,
                provider_type=_FAKE_PROVIDER,
                port=8383,
                timeout=30,
                action="started",
            )

        assert os.environ.get("ZENML_SERVER_DASHBOARD_FILES_PATH") is None
        assert os.environ.get("ZENML_DEFAULT_ANALYTICS_SOURCE") == "kitaru-python"

    def test_restart_action_produces_restart_error_message(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("ZENML_SERVER_DASHBOARD_FILES_PATH", raising=False)
        monkeypatch.delenv("ZENML_DEFAULT_ANALYTICS_SOURCE", raising=False)

        deployer = MagicMock()
        deployer.deploy_server.side_effect = RuntimeError("boom")

        with (
            patch.object(ls_mod, "_resolve_bundled_ui_dir", return_value=None),
            pytest.raises(KitaruBackendError, match="failed to restart"),
        ):
            _deploy_and_connect(
                deployer=deployer,
                deployment_config_cls=_FakeDeploymentConfig,
                provider_type=_FAKE_PROVIDER,
                port=8383,
                timeout=30,
                action="restarted",
            )

    def test_no_bundled_ui_skips_dashboard_env_var(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("ZENML_SERVER_DASHBOARD_FILES_PATH", raising=False)
        monkeypatch.delenv("ZENML_DEFAULT_ANALYTICS_SOURCE", raising=False)

        captured_env: dict[str, str | None] = {}

        def capture_env(_config: Any, **_kw: Any) -> SimpleNamespace:
            captured_env["dashboard"] = os.environ.get(
                "ZENML_SERVER_DASHBOARD_FILES_PATH"
            )
            return SimpleNamespace(status=SimpleNamespace(url="http://127.0.0.1:8383"))

        deployer = MagicMock()
        deployer.deploy_server.side_effect = capture_env

        with patch.object(ls_mod, "_resolve_bundled_ui_dir", return_value=None):
            _deploy_and_connect(
                deployer=deployer,
                deployment_config_cls=_FakeDeploymentConfig,
                provider_type=_FAKE_PROVIDER,
                port=8383,
                timeout=30,
                action="started",
            )

        assert captured_env["dashboard"] is None

    def test_config_fields_passed_correctly(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("ZENML_SERVER_DASHBOARD_FILES_PATH", raising=False)
        monkeypatch.delenv("ZENML_DEFAULT_ANALYTICS_SOURCE", raising=False)

        captured_config: list[_FakeDeploymentConfig] = []

        def capture_config(config: Any, **_kw: Any) -> SimpleNamespace:
            captured_config.append(config)
            return SimpleNamespace(status=SimpleNamespace(url=None))

        deployer = MagicMock()
        deployer.deploy_server.side_effect = capture_config

        with patch.object(ls_mod, "_resolve_bundled_ui_dir", return_value=None):
            result = _deploy_and_connect(
                deployer=deployer,
                deployment_config_cls=_FakeDeploymentConfig,
                provider_type=_FAKE_PROVIDER,
                port=9999,
                timeout=60,
                action="started",
            )

        assert len(captured_config) == 1
        cfg = captured_config[0]
        assert cfg.provider == "daemon"
        assert cfg.ip_address == "127.0.0.1"
        assert cfg.port == 9999

        # Falls back to constructed URL when deployed server has no URL
        assert result.url == "http://127.0.0.1:9999"


# ---------------------------------------------------------------------------
# start_or_connect_local_server
# ---------------------------------------------------------------------------

_PATCH_PREFIX = "kitaru._local_server"


class TestStartOrConnectLocalServer:
    """Tests for the main lifecycle orchestration function."""

    def test_existing_running_server_returns_connected(self) -> None:
        server_url = "http://127.0.0.1:8383"
        deployer = MagicMock()
        local_server = _make_local_server(server_url, is_running=True)
        runtime = _make_runtime_tuple(deployer, local_server)

        with (
            patch(f"{_PATCH_PREFIX}._ensure_local_server_dependencies"),
            patch(f"{_PATCH_PREFIX}._load_local_server_runtime", return_value=runtime),
            patch(f"{_PATCH_PREFIX}._resolve_bundled_ui_dir", return_value=None),
            patch(f"{_PATCH_PREFIX}._track_local_server_started") as mock_track,
        ):
            result = start_or_connect_local_server(port=None, timeout=30)

        assert result.action == "connected"
        assert result.url == server_url
        deployer.connect_to_server.assert_called_once()
        deployer.remove_server.assert_not_called()
        mock_track.assert_called_once_with(result)

    def test_existing_running_server_logs_stale_ui_warning(self) -> None:
        deployer = MagicMock()
        local_server = _make_local_server("http://127.0.0.1:8383", is_running=True)
        runtime = _make_runtime_tuple(deployer, local_server)

        with (
            patch(f"{_PATCH_PREFIX}._ensure_local_server_dependencies"),
            patch(f"{_PATCH_PREFIX}._load_local_server_runtime", return_value=runtime),
            patch(
                f"{_PATCH_PREFIX}._resolve_bundled_ui_dir",
                return_value=Path("/fake/ui/dist"),
            ),
            patch(f"{_PATCH_PREFIX}._track_local_server_started"),
            patch(f"{_PATCH_PREFIX}.logger") as mock_logger,
        ):
            start_or_connect_local_server(port=None, timeout=30)

        mock_logger.debug.assert_called_once()
        msg = mock_logger.debug.call_args[0][0]
        assert "dashboard may differ" in msg
        assert "Restart the server" in msg

    def test_existing_running_server_no_stale_ui_log_without_bundled_ui(self) -> None:
        deployer = MagicMock()
        local_server = _make_local_server("http://127.0.0.1:8383", is_running=True)
        runtime = _make_runtime_tuple(deployer, local_server)

        with (
            patch(f"{_PATCH_PREFIX}._ensure_local_server_dependencies"),
            patch(f"{_PATCH_PREFIX}._load_local_server_runtime", return_value=runtime),
            patch(f"{_PATCH_PREFIX}._resolve_bundled_ui_dir", return_value=None),
            patch(f"{_PATCH_PREFIX}._track_local_server_started"),
            patch(f"{_PATCH_PREFIX}.logger") as mock_logger,
        ):
            start_or_connect_local_server(port=None, timeout=30)

        mock_logger.debug.assert_not_called()

    def test_existing_server_connect_failure_raises(self) -> None:
        deployer = MagicMock()
        deployer.connect_to_server.side_effect = RuntimeError("refused")
        local_server = _make_local_server("http://127.0.0.1:8383", is_running=True)
        runtime = _make_runtime_tuple(deployer, local_server)

        with (
            patch(f"{_PATCH_PREFIX}._ensure_local_server_dependencies"),
            patch(f"{_PATCH_PREFIX}._load_local_server_runtime", return_value=runtime),
            patch(f"{_PATCH_PREFIX}._resolve_bundled_ui_dir", return_value=None),
            pytest.raises(KitaruBackendError, match="Failed to connect"),
        ):
            start_or_connect_local_server(port=None, timeout=30)

    def test_mismatched_port_removes_and_restarts(self) -> None:
        deployer = MagicMock()
        local_server = _make_local_server("http://127.0.0.1:8383", is_running=True)
        runtime = _make_runtime_tuple(deployer, local_server)
        expected_result = LocalServerConnectionResult(
            url="http://127.0.0.1:9090", action="restarted"
        )

        with (
            patch(f"{_PATCH_PREFIX}._ensure_local_server_dependencies"),
            patch(f"{_PATCH_PREFIX}._load_local_server_runtime", return_value=runtime),
            patch(
                f"{_PATCH_PREFIX}._deploy_and_connect", return_value=expected_result
            ) as mock_deploy,
            patch(f"{_PATCH_PREFIX}._track_local_server_started") as mock_track,
        ):
            result = start_or_connect_local_server(port=9090, timeout=30)

        deployer.remove_server.assert_called_once_with(timeout=30)
        mock_deploy.assert_called_once()
        call_kwargs = mock_deploy.call_args[1]
        assert call_kwargs["port"] == 9090
        assert call_kwargs["action"] == "restarted"
        assert result == expected_result
        mock_track.assert_called_once_with(result)

    def test_stale_server_with_port_none_uses_started_action(self) -> None:
        """Stale server + port=None uses 'started' not 'restarted'."""
        deployer = MagicMock()
        local_server = _make_local_server("http://127.0.0.1:8383", is_running=False)
        runtime = _make_runtime_tuple(deployer, local_server)
        expected_result = LocalServerConnectionResult(
            url="http://127.0.0.1:8383", action="started"
        )

        with (
            patch(f"{_PATCH_PREFIX}._ensure_local_server_dependencies"),
            patch(f"{_PATCH_PREFIX}._load_local_server_runtime", return_value=runtime),
            patch(
                f"{_PATCH_PREFIX}._deploy_and_connect", return_value=expected_result
            ) as mock_deploy,
            patch(f"{_PATCH_PREFIX}._track_local_server_started"),
        ):
            result = start_or_connect_local_server(port=None, timeout=30)

        deployer.remove_server.assert_called_once()
        call_kwargs = mock_deploy.call_args[1]
        assert call_kwargs["action"] == "started"
        assert call_kwargs["port"] == 8383
        assert result.action == "started"

    def test_no_existing_server_deploys_on_default_port(self) -> None:
        deployer = MagicMock()
        runtime = _make_runtime_tuple(deployer, local_server=None)
        expected_result = LocalServerConnectionResult(
            url="http://127.0.0.1:8383", action="started"
        )

        with (
            patch(f"{_PATCH_PREFIX}._ensure_local_server_dependencies"),
            patch(f"{_PATCH_PREFIX}._load_local_server_runtime", return_value=runtime),
            patch(
                f"{_PATCH_PREFIX}._deploy_and_connect", return_value=expected_result
            ) as mock_deploy,
            patch(f"{_PATCH_PREFIX}._track_local_server_started") as mock_track,
        ):
            result = start_or_connect_local_server(port=None, timeout=30)

        deployer.remove_server.assert_not_called()
        call_kwargs = mock_deploy.call_args[1]
        assert call_kwargs["port"] == 8383
        assert call_kwargs["action"] == "started"
        assert result == expected_result
        mock_track.assert_called_once_with(result)

    def test_no_existing_server_with_explicit_port(self) -> None:
        deployer = MagicMock()
        runtime = _make_runtime_tuple(deployer, local_server=None)
        expected_result = LocalServerConnectionResult(
            url="http://127.0.0.1:7070", action="started"
        )

        with (
            patch(f"{_PATCH_PREFIX}._ensure_local_server_dependencies"),
            patch(f"{_PATCH_PREFIX}._load_local_server_runtime", return_value=runtime),
            patch(
                f"{_PATCH_PREFIX}._deploy_and_connect", return_value=expected_result
            ) as mock_deploy,
            patch(f"{_PATCH_PREFIX}._track_local_server_started"),
        ):
            result = start_or_connect_local_server(port=7070, timeout=30)

        call_kwargs = mock_deploy.call_args[1]
        assert call_kwargs["port"] == 7070
        # No existing server + explicit port → "started" (not "restarted")
        assert call_kwargs["action"] == "started"
        assert result.action == "started"

    def test_remove_server_failure_raises(self) -> None:
        deployer = MagicMock()
        deployer.remove_server.side_effect = RuntimeError("stuck")
        local_server = _make_local_server("http://127.0.0.1:8383", is_running=False)
        runtime = _make_runtime_tuple(deployer, local_server)

        with (
            patch(f"{_PATCH_PREFIX}._ensure_local_server_dependencies"),
            patch(f"{_PATCH_PREFIX}._load_local_server_runtime", return_value=runtime),
            pytest.raises(KitaruBackendError, match="Failed to stop existing"),
        ):
            start_or_connect_local_server(port=None, timeout=30)
