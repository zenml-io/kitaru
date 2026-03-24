"""Tests for Stage 14 bootstrap and packaging wiring."""

from __future__ import annotations

import importlib.metadata
import sys
import tomllib
from pathlib import Path
from unittest.mock import patch

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
        patch.object(
            cli_module,
            "_apply_runtime_version",
            side_effect=lambda: events.append("version"),
        ),
        patch.object(
            cli_module,
            "app",
            side_effect=lambda *args, **kwargs: events.append("app"),
        ),
        patch(
            "kitaru.analytics.track", side_effect=lambda *a, **k: events.append("track")
        ),
    ):
        cli_module.cli()

    assert events == ["version", "app", "track"]


def test_cli_entrypoint_tracks_analytics_even_on_system_exit() -> None:
    """Analytics tracking fires in the finally block even when app() raises."""
    import kitaru.cli as cli_module

    events: list[str] = []

    with (
        patch.object(
            cli_module,
            "_apply_runtime_version",
            side_effect=lambda: events.append("version"),
        ),
        patch.object(
            cli_module,
            "app",
            side_effect=SystemExit(0),
        ),
        patch(
            "kitaru.analytics.track", side_effect=lambda *a, **k: events.append("track")
        ),
        pytest.raises(SystemExit),
    ):
        cli_module.cli()

    assert "track" in events


@pytest.mark.parametrize(
    ("argv", "expected"),
    [
        (["kitaru", "status", "--all"], "status"),
        (["kitaru", "--help"], "--help"),
        (["kitaru"], "help"),
    ],
)
def test_invoked_command_name_extracts_first_arg(
    argv: list[str], expected: str
) -> None:
    """_invoked_command_name should return the first CLI token or 'help'."""
    from kitaru.cli import _invoked_command_name

    with patch.object(sys, "argv", argv):
        assert _invoked_command_name() == expected
