"""Tests for the lightweight console bootstrap module."""

from __future__ import annotations

import importlib.metadata
from unittest.mock import Mock, patch

import pytest

import _kitaru_bootstrap as bootstrap


def test_ensure_supported_python_rejects_old_versions(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Unsupported interpreters should fail with a clear message."""
    with pytest.raises(SystemExit) as exc_info:
        bootstrap.ensure_supported_python((3, 10, 14))

    assert exc_info.value.code == 1
    assert (
        "Kitaru requires Python 3.11 or newer. Detected Python 3.10.14."
        in capsys.readouterr().err
    )


def test_cli_main_checks_python_before_importing_cli() -> None:
    """CLI bootstrap should stop before importing `kitaru.cli` on old Python."""
    with (
        patch.object(bootstrap, "ensure_supported_python", side_effect=SystemExit(1)),
        patch("importlib.import_module") as import_module,
        pytest.raises(SystemExit),
    ):
        bootstrap.cli_main()

    import_module.assert_not_called()


def test_mcp_main_checks_python_before_importing_mcp() -> None:
    """MCP bootstrap should stop before importing `kitaru.mcp` on old Python."""
    with (
        patch.object(bootstrap, "ensure_supported_python", side_effect=SystemExit(1)),
        patch("importlib.import_module") as import_module,
        pytest.raises(SystemExit),
    ):
        bootstrap.mcp_main()

    import_module.assert_not_called()


def test_resolve_installed_version_returns_unknown_when_metadata_missing() -> None:
    """Missing package metadata should fall back to `unknown`."""
    bootstrap.resolve_installed_version.cache_clear()

    with patch(
        "importlib.metadata.version",
        side_effect=importlib.metadata.PackageNotFoundError,
    ):
        assert bootstrap.resolve_installed_version() == "unknown"


def test_cli_main_imports_and_runs_cli_entrypoint() -> None:
    """CLI bootstrap should import and call the CLI entrypoint on success."""
    runner = Mock(return_value=None)

    with (
        patch.object(bootstrap, "ensure_supported_python"),
        patch.object(bootstrap, "_load_cli_entrypoint", return_value=runner),
    ):
        bootstrap.cli_main()

    runner.assert_called_once_with()


def test_mcp_main_imports_and_runs_mcp_entrypoint() -> None:
    """MCP bootstrap should import and call the MCP entrypoint on success."""
    runner = Mock(return_value=None)

    with (
        patch.object(bootstrap, "ensure_supported_python"),
        patch.object(bootstrap, "_load_mcp_entrypoint", return_value=runner),
    ):
        bootstrap.mcp_main()

    runner.assert_called_once_with()
