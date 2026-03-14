"""Tests for Stage 14 bootstrap and packaging wiring."""

from __future__ import annotations

import importlib.metadata
import tomllib
from pathlib import Path
from unittest.mock import patch

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
    ):
        cli_module.cli()

    assert events == ["version", "app"]
