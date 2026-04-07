"""Tests for bootstrap, packaging wiring, and CLI startup resilience."""

from __future__ import annotations

import importlib.metadata
import tomllib
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_PYPROJECT_PATH = Path(__file__).resolve().parents[1] / "pyproject.toml"


def test_resolve_installed_version_returns_unknown_when_metadata_missing() -> None:
    """Missing package metadata should fall back to `unknown`."""
    from kitaru import _version

    _version.resolve_installed_version.cache_clear()
    try:
        with patch(
            "importlib.metadata.version",
            side_effect=importlib.metadata.PackageNotFoundError,
        ):
            assert _version.resolve_installed_version() == "unknown"
    finally:
        _version.resolve_installed_version.cache_clear()


def test_console_scripts_point_to_package_entrypoints() -> None:
    """Packaging should expose package entrypoints without top-level shims."""
    pyproject = tomllib.loads(_PYPROJECT_PATH.read_text())

    scripts = pyproject["project"]["scripts"]
    assert scripts["kitaru"] == "kitaru.cli:cli"
    assert scripts["kitaru-mcp"] == "kitaru.mcp:main"

    wheel_target = pyproject["tool"]["hatch"]["build"]["targets"]["wheel"]
    assert "force-include" not in wheel_target


def test_ui_artifacts_configured_for_wheel_inclusion() -> None:
    """Hatch build config should include gitignored UI assets via artifacts."""
    pyproject = tomllib.loads(_PYPROJECT_PATH.read_text())

    artifacts = pyproject["tool"]["hatch"]["build"].get("artifacts", [])
    assert any("_ui/dist" in a for a in artifacts), (
        "pyproject.toml must include _ui/dist in [tool.hatch.build] artifacts"
    )
    assert any("bundle_manifest.json" in a for a in artifacts), (
        "pyproject.toml must include bundle_manifest.json in artifacts"
    )


def test_ui_package_scaffold_exists() -> None:
    """The _ui package __init__.py must be tracked (not gitignored)."""
    ui_init = (
        Path(__file__).resolve().parents[1] / "src" / "kitaru" / "_ui" / "__init__.py"
    )
    assert ui_init.is_file(), f"Missing tracked file: {ui_init}"


def test_cli_entrypoint_populates_version_before_dispatch() -> None:
    """The package CLI entrypoint should set the version before running the app."""
    import kitaru.cli as cli_module

    events: list[str] = []

    with (
        patch("sys.argv", ["kitaru", "status"]),
        patch.object(
            cli_module,
            "_apply_runtime_version",
            side_effect=lambda: events.append("version"),
        ),
        patch.object(
            cli_module,
            "app",
            side_effect=lambda *_args, **_kwargs: events.append("app"),
        ),
        patch.object(cli_module, "GlobalConfiguration"),
        patch("kitaru.analytics.track", return_value=True),
        patch("kitaru.analytics.set_source"),
    ):
        cli_module.cli()

    assert events == ["version", "app"]


# ---------------------------------------------------------------------------
# _should_bootstrap_store classification tests
# ---------------------------------------------------------------------------


class TestShouldBootstrapStore:
    """Argv-based classification for eager store initialization."""

    @pytest.mark.parametrize(
        "argv",
        [
            [],
            ["--help"],
            ["-h"],
            ["--version"],
            ["-V"],
            ["login"],
            ["login", "http://localhost:8080"],
            ["logout"],
            ["init"],
            ["init", "--path", "/tmp/project"],
            ["stack", "--help"],
            ["executions", "-h"],
            ["--help", "status"],
        ],
        ids=[
            "no-args",
            "help-long",
            "help-short",
            "version-long",
            "version-short",
            "login-bare",
            "login-with-url",
            "logout",
            "init-bare",
            "init-with-args",
            "subcommand-help",
            "subcommand-help-short",
            "help-before-command",
        ],
    )
    def test_deferred_cases(self, argv: list[str]) -> None:
        from kitaru.cli import _should_bootstrap_store

        assert _should_bootstrap_store(argv) is False

    @pytest.mark.parametrize(
        "argv",
        [
            ["status"],
            ["info"],
            ["stack", "list"],
            ["executions", "get", "kr-123"],
            ["secrets", "list"],
        ],
        ids=["status", "info", "stack-list", "executions-get", "secrets-list"],
    )
    def test_eager_cases(self, argv: list[str]) -> None:
        from kitaru.cli import _should_bootstrap_store

        assert _should_bootstrap_store(argv) is True


# ---------------------------------------------------------------------------
# CLI startup resilience integration tests
# ---------------------------------------------------------------------------


class TestCliStartupResilience:
    """Verify that safe commands skip store bootstrap in cli()."""

    @pytest.mark.parametrize(
        "argv",
        [
            ["kitaru", "--help"],
            ["kitaru", "--version"],
            ["kitaru", "login"],
            ["kitaru", "logout"],
            ["kitaru", "init"],
            ["kitaru"],
        ],
        ids=["help", "version", "login", "logout", "init", "no-args"],
    )
    def test_safe_commands_skip_store_bootstrap(self, argv: list[str]) -> None:
        """Commands that don't need a server must not touch zen_store."""
        import kitaru.cli as cli_module

        mock_gc = MagicMock()
        mock_gc_class = MagicMock(return_value=mock_gc)

        with (
            patch("sys.argv", argv),
            patch.object(cli_module, "GlobalConfiguration", mock_gc_class),
            patch.object(cli_module, "app"),
            patch.object(cli_module, "_apply_runtime_version"),
            patch("kitaru.analytics.track", return_value=True),
            patch("kitaru.analytics.set_source"),
        ):
            cli_module.cli()

        assert not mock_gc_class.called, "GlobalConfiguration() should not be called"

    def test_store_backed_command_bootstraps_store(self) -> None:
        """Commands like 'status' should still eagerly initialize the store."""
        import kitaru.cli as cli_module

        mock_gc = MagicMock()
        mock_gc_class = MagicMock(return_value=mock_gc)

        with (
            patch("sys.argv", ["kitaru", "status"]),
            patch.object(cli_module, "GlobalConfiguration", mock_gc_class),
            patch.object(cli_module, "app"),
            patch.object(cli_module, "_apply_runtime_version"),
            patch("kitaru.analytics.track", return_value=True),
            patch("kitaru.analytics.set_source"),
        ):
            cli_module.cli()

        mock_gc_class.assert_called_once()
        # zen_store attribute was accessed
        _ = mock_gc.zen_store

    def test_analytics_still_attempted_for_deferred_commands(self) -> None:
        """Even deferred commands should attempt analytics tracking."""
        import kitaru.cli as cli_module

        track_mock = MagicMock(return_value=True)

        with (
            patch("sys.argv", ["kitaru", "login"]),
            patch.object(cli_module, "app"),
            patch.object(cli_module, "_apply_runtime_version"),
            patch("kitaru.analytics.track", track_mock),
            patch("kitaru.analytics.set_source"),
        ):
            cli_module.cli()

        track_mock.assert_called_once()
        call_args = track_mock.call_args
        assert call_args[0][1] == {"command": "login"}
