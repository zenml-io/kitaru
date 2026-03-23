"""Tests for Kitaru UI dashboard patching in _local_server.py."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from kitaru._local_server import (
    _SENTINEL_FILE_NAME,
    _apply_dashboard_patch,
    _dashboard_needs_update,
    _ensure_kitaru_dashboard,
    _load_bundled_manifest,
    _load_installed_sentinel,
)
from kitaru.errors import KitaruBackendError

_VALID_MANIFEST = {
    "schema_version": 1,
    "ui_version": "v0.2.0",
    "bundle_sha256": "abc123",
    "source": "https://example.com/kitaru-ui.tar.gz",
}


def _write_manifest(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f)


def _create_dashboard(
    dashboard_dir: Path,
    *,
    sentinel: dict | None = None,
    with_index: bool = True,
) -> None:
    """Create a fake ZenML dashboard directory."""
    dashboard_dir.mkdir(parents=True, exist_ok=True)
    if with_index:
        (dashboard_dir / "index.html").write_text("<html>zenml</html>")
    if sentinel is not None:
        sentinel_dir = dashboard_dir / ".kitaru-ui"
        sentinel_dir.mkdir(exist_ok=True)
        with open(sentinel_dir / "bundle_manifest.json", "w") as f:
            json.dump(sentinel, f)


# ---------------------------------------------------------------------------
# _load_bundled_manifest / _load_installed_sentinel
# ---------------------------------------------------------------------------


class TestLoadBundledManifest:
    def test_loads_manifest_from_package_dir(self, tmp_path: Path) -> None:
        manifest_path = tmp_path / "_ui" / _SENTINEL_FILE_NAME
        _write_manifest(manifest_path, _VALID_MANIFEST)

        with patch("kitaru._local_server.__file__", str(tmp_path / "_local_server.py")):
            result = _load_bundled_manifest()

        assert result is not None
        assert result["ui_version"] == "v0.2.0"
        assert result["bundle_sha256"] == "abc123"

    def test_returns_none_when_file_missing(self, tmp_path: Path) -> None:
        with patch("kitaru._local_server.__file__", str(tmp_path / "_local_server.py")):
            assert _load_bundled_manifest() is None


class TestLoadInstalledSentinel:
    def test_returns_none_when_no_sentinel(self, tmp_path: Path) -> None:
        dashboard = tmp_path / "dashboard"
        dashboard.mkdir()
        assert _load_installed_sentinel(dashboard) is None

    def test_returns_none_when_malformed(self, tmp_path: Path) -> None:
        dashboard = tmp_path / "dashboard"
        sentinel_dir = dashboard / ".kitaru-ui"
        sentinel_dir.mkdir(parents=True)
        (sentinel_dir / "bundle_manifest.json").write_text("not json")
        assert _load_installed_sentinel(dashboard) is None

    def test_returns_none_when_wrong_schema_version(self, tmp_path: Path) -> None:
        dashboard = tmp_path / "dashboard"
        bad_manifest = {**_VALID_MANIFEST, "schema_version": 99}
        _create_dashboard(dashboard, sentinel=bad_manifest)
        assert _load_installed_sentinel(dashboard) is None

    def test_returns_valid_sentinel(self, tmp_path: Path) -> None:
        dashboard = tmp_path / "dashboard"
        _create_dashboard(dashboard, sentinel=_VALID_MANIFEST)
        result = _load_installed_sentinel(dashboard)
        assert result is not None
        assert result["ui_version"] == "v0.2.0"


# ---------------------------------------------------------------------------
# _dashboard_needs_update (now takes bundled manifest as parameter)
# ---------------------------------------------------------------------------


class TestDashboardNeedsUpdate:
    def test_needs_update_when_no_index_html(self, tmp_path: Path) -> None:
        dashboard = tmp_path / "dashboard"
        _create_dashboard(dashboard, with_index=False)
        assert _dashboard_needs_update(dashboard, _VALID_MANIFEST) is True

    def test_needs_update_when_no_sentinel(self, tmp_path: Path) -> None:
        dashboard = tmp_path / "dashboard"
        _create_dashboard(dashboard, with_index=True)
        assert _dashboard_needs_update(dashboard, _VALID_MANIFEST) is True

    def test_needs_update_when_version_mismatch(self, tmp_path: Path) -> None:
        dashboard = tmp_path / "dashboard"
        old_sentinel = {**_VALID_MANIFEST, "ui_version": "v0.1.0"}
        _create_dashboard(dashboard, sentinel=old_sentinel)
        assert _dashboard_needs_update(dashboard, _VALID_MANIFEST) is True

    def test_needs_update_when_checksum_mismatch(self, tmp_path: Path) -> None:
        dashboard = tmp_path / "dashboard"
        old_sentinel = {**_VALID_MANIFEST, "bundle_sha256": "old_hash"}
        _create_dashboard(dashboard, sentinel=old_sentinel)
        assert _dashboard_needs_update(dashboard, _VALID_MANIFEST) is True

    def test_no_update_needed_when_current(self, tmp_path: Path) -> None:
        dashboard = tmp_path / "dashboard"
        _create_dashboard(dashboard, sentinel=_VALID_MANIFEST)
        assert _dashboard_needs_update(dashboard, _VALID_MANIFEST) is False


# ---------------------------------------------------------------------------
# _apply_dashboard_patch (now takes bundled manifest as parameter)
# ---------------------------------------------------------------------------


class TestApplyDashboardPatch:
    def test_replaces_dashboard_with_kitaru_ui(self, tmp_path: Path) -> None:
        bundled_dir = tmp_path / "bundled" / "dist"
        bundled_dir.mkdir(parents=True)
        (bundled_dir / "index.html").write_text("<html>kitaru</html>")
        (bundled_dir / "assets").mkdir()
        (bundled_dir / "assets" / "app.js").write_text("// kitaru")

        dashboard = tmp_path / "zen_server" / "dashboard"
        _create_dashboard(dashboard)

        with patch(
            "kitaru._local_server._resolve_bundled_ui_dir",
            return_value=bundled_dir,
        ):
            _apply_dashboard_patch(dashboard, _VALID_MANIFEST)

        assert (dashboard / "index.html").read_text() == "<html>kitaru</html>"
        assert (dashboard / "assets" / "app.js").read_text() == "// kitaru"
        sentinel = _load_installed_sentinel(dashboard)
        assert sentinel is not None
        assert sentinel["ui_version"] == "v0.2.0"

    def test_works_when_dashboard_dir_missing(self, tmp_path: Path) -> None:
        bundled_dir = tmp_path / "bundled" / "dist"
        bundled_dir.mkdir(parents=True)
        (bundled_dir / "index.html").write_text("<html>kitaru</html>")

        dashboard = tmp_path / "zen_server" / "dashboard"
        assert not dashboard.exists()

        with patch(
            "kitaru._local_server._resolve_bundled_ui_dir",
            return_value=bundled_dir,
        ):
            dashboard.parent.mkdir(parents=True, exist_ok=True)
            _apply_dashboard_patch(dashboard, _VALID_MANIFEST)

        assert (dashboard / "index.html").read_text() == "<html>kitaru</html>"

    def test_raises_when_no_bundled_ui(self, tmp_path: Path) -> None:
        dashboard = tmp_path / "dashboard"
        with (
            patch("kitaru._local_server._resolve_bundled_ui_dir", return_value=None),
            pytest.raises(KitaruBackendError, match="assets are missing"),
        ):
            _apply_dashboard_patch(dashboard, _VALID_MANIFEST)


# ---------------------------------------------------------------------------
# _ensure_kitaru_dashboard
# ---------------------------------------------------------------------------


class TestEnsureKitaruDashboard:
    def test_returns_false_when_no_bundled_ui(self) -> None:
        with patch("kitaru._local_server._resolve_bundled_ui_dir", return_value=None):
            assert _ensure_kitaru_dashboard() is False

    def test_returns_false_when_already_current(self, tmp_path: Path) -> None:
        dashboard = tmp_path / "dashboard"
        _create_dashboard(dashboard, sentinel=_VALID_MANIFEST)

        with (
            patch(
                "kitaru._local_server._resolve_bundled_ui_dir",
                return_value=tmp_path / "dist",
            ),
            patch(
                "kitaru._local_server._resolve_zenml_dashboard_dir",
                return_value=dashboard,
            ),
            patch(
                "kitaru._local_server._load_bundled_manifest",
                return_value=_VALID_MANIFEST,
            ),
        ):
            assert _ensure_kitaru_dashboard() is False

    def test_returns_true_when_patched(self, tmp_path: Path) -> None:
        bundled_dir = tmp_path / "bundled" / "dist"
        bundled_dir.mkdir(parents=True)
        (bundled_dir / "index.html").write_text("<html>kitaru</html>")

        dashboard = tmp_path / "zen_server" / "dashboard"
        _create_dashboard(dashboard)

        with (
            patch(
                "kitaru._local_server._resolve_bundled_ui_dir",
                return_value=bundled_dir,
            ),
            patch(
                "kitaru._local_server._resolve_zenml_dashboard_dir",
                return_value=dashboard,
            ),
            patch(
                "kitaru._local_server._load_bundled_manifest",
                return_value=_VALID_MANIFEST,
            ),
        ):
            assert _ensure_kitaru_dashboard() is True

        assert (dashboard / "index.html").read_text() == "<html>kitaru</html>"


# ---------------------------------------------------------------------------
# Integration with start_or_connect_local_server
# ---------------------------------------------------------------------------


class TestStartOrConnectDashboardIntegration:
    """Verify that start_or_connect_local_server calls _ensure_kitaru_dashboard."""

    def test_ensure_dashboard_called_before_deploy(self) -> None:
        from kitaru._local_server import start_or_connect_local_server

        call_order: list[str] = []

        def mock_ensure() -> bool:
            call_order.append("ensure_dashboard")
            return False

        mock_deployer = MagicMock()
        mock_deployer.deploy_server.side_effect = lambda *a, **kw: (
            call_order.append("deploy"),
            MagicMock(),
        )[1]
        mock_deployer.connect_to_server.return_value = None

        with (
            patch(
                "kitaru._local_server._ensure_kitaru_dashboard",
                side_effect=mock_ensure,
            ),
            patch(
                "kitaru._local_server._ensure_local_server_dependencies",
            ),
            patch(
                "kitaru._local_server._load_local_server_runtime",
                return_value=(
                    lambda: mock_deployer,
                    MagicMock(return_value=MagicMock(provider="DAEMON")),
                    MagicMock(DAEMON="DAEMON"),
                    lambda: None,
                ),
            ),
        ):
            start_or_connect_local_server(port=None, timeout=30)

        assert call_order.index("ensure_dashboard") < call_order.index("deploy")

    def test_stale_running_server_restarts(self) -> None:
        from kitaru._local_server import start_or_connect_local_server

        mock_deployer = MagicMock()
        mock_deployer.deploy_server.return_value = MagicMock()
        mock_deployer.connect_to_server.return_value = None

        mock_local_server = MagicMock()
        mock_local_server.is_running = True
        mock_local_server.status.url = "http://127.0.0.1:8383"

        with (
            patch(
                "kitaru._local_server._ensure_kitaru_dashboard",
                return_value=True,
            ),
            patch(
                "kitaru._local_server._ensure_local_server_dependencies",
            ),
            patch(
                "kitaru._local_server._load_local_server_runtime",
                return_value=(
                    lambda: mock_deployer,
                    MagicMock(return_value=MagicMock(provider="DAEMON")),
                    MagicMock(DAEMON="DAEMON"),
                    lambda: mock_local_server,
                ),
            ),
        ):
            result = start_or_connect_local_server(port=None, timeout=30)

        assert result.action == "restarted"
        mock_deployer.remove_server.assert_called_once()
        mock_deployer.deploy_server.assert_called_once()

    def test_current_running_server_just_connects(self) -> None:
        from kitaru._local_server import start_or_connect_local_server

        mock_deployer = MagicMock()
        mock_deployer.connect_to_server.return_value = None

        mock_local_server = MagicMock()
        mock_local_server.is_running = True
        mock_local_server.status.url = "http://127.0.0.1:8383"

        with (
            patch(
                "kitaru._local_server._ensure_kitaru_dashboard",
                return_value=False,
            ),
            patch(
                "kitaru._local_server._ensure_local_server_dependencies",
            ),
            patch(
                "kitaru._local_server._load_local_server_runtime",
                return_value=(
                    lambda: mock_deployer,
                    MagicMock(return_value=MagicMock(provider="DAEMON")),
                    MagicMock(DAEMON="DAEMON"),
                    lambda: mock_local_server,
                ),
            ),
        ):
            result = start_or_connect_local_server(port=None, timeout=30)

        assert result.action == "connected"
        mock_deployer.remove_server.assert_not_called()
        mock_deployer.deploy_server.assert_not_called()
