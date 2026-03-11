"""Tests for the lightweight console bootstrap module."""

from __future__ import annotations

import importlib.metadata
import importlib.util
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

_BOOTSTRAP_PATH = Path(__file__).resolve().parents[1] / "src" / "_kitaru_bootstrap.py"
_BOOTSTRAP_SPEC = importlib.util.spec_from_file_location(
    "_kitaru_bootstrap_test",
    _BOOTSTRAP_PATH,
)
assert _BOOTSTRAP_SPEC is not None and _BOOTSTRAP_SPEC.loader is not None
bootstrap = importlib.util.module_from_spec(_BOOTSTRAP_SPEC)
_BOOTSTRAP_SPEC.loader.exec_module(bootstrap)


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
        patch.object(bootstrap, "_apply_env_translations"),
        patch.object(bootstrap, "_load_cli_entrypoint", return_value=runner),
    ):
        bootstrap.cli_main()

    runner.assert_called_once_with()


def test_mcp_main_imports_and_runs_mcp_entrypoint() -> None:
    """MCP bootstrap should import and call the MCP entrypoint on success."""
    runner = Mock(return_value=None)

    with (
        patch.object(bootstrap, "ensure_supported_python"),
        patch.object(bootstrap, "_apply_env_translations"),
        patch.object(bootstrap, "_load_mcp_entrypoint", return_value=runner),
    ):
        bootstrap.mcp_main()

    runner.assert_called_once_with()


def test_cli_main_applies_env_translations_before_loading_entrypoint() -> None:
    """CLI bootstrap should translate env vars before importing the CLI."""
    events: list[str] = []

    def _record_translation() -> None:
        events.append("translate")

    def _load_runner() -> Mock:
        events.append("load")
        return Mock()

    with (
        patch.object(bootstrap, "ensure_supported_python"),
        patch.object(
            bootstrap, "_apply_env_translations", side_effect=_record_translation
        ),
        patch.object(bootstrap, "_load_cli_entrypoint", side_effect=_load_runner),
    ):
        bootstrap.cli_main()

    assert events[:2] == ["translate", "load"]


def test_mcp_main_stops_before_loading_entrypoint_if_translation_fails() -> None:
    """Bootstrap should not import MCP entrypoint if env translation fails."""
    with (
        patch.object(bootstrap, "ensure_supported_python"),
        patch.object(
            bootstrap,
            "_apply_env_translations",
            side_effect=RuntimeError("bad env"),
        ),
        patch.object(bootstrap, "_load_mcp_entrypoint") as load_entrypoint,
        pytest.raises(RuntimeError, match="bad env"),
    ):
        bootstrap.mcp_main()

    load_entrypoint.assert_not_called()
