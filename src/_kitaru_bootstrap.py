# ruff: noqa: UP006, UP035, UP045
"""Bootstrap helpers for Kitaru console entrypoints.

This module intentionally lives outside the `kitaru` package so console-script
entrypoints can validate the interpreter before importing the package itself.
Keep it compatible with older Python versions.
"""

from __future__ import annotations

import functools
import importlib
import importlib.metadata
import sys
from collections.abc import Callable
from typing import Optional, Tuple

MIN_SUPPORTED_PYTHON: Tuple[int, int] = (3, 11)
_UNKNOWN_VERSION = "unknown"


def format_unsupported_python_message(
    version_info: Tuple[int, int, int],
) -> str:
    """Format the unsupported-Python error message."""
    detected = ".".join(str(part) for part in version_info)
    return f"Kitaru requires Python 3.11 or newer. Detected Python {detected}."


def ensure_supported_python(
    version_info: Optional[Tuple[int, int, int]] = None,
) -> None:
    """Exit early with a clear message if Python is unsupported."""
    if version_info is None:
        version_info = (
            sys.version_info.major,
            sys.version_info.minor,
            sys.version_info.micro,
        )

    if version_info[:2] < MIN_SUPPORTED_PYTHON:
        sys.stderr.write(format_unsupported_python_message(version_info) + "\n")
        raise SystemExit(1)


@functools.lru_cache(maxsize=1)
def resolve_installed_version() -> str:
    """Resolve the installed Kitaru version lazily.

    Returns:
        Installed package version, or ``"unknown"`` if package metadata is not
        available in the current environment.
    """
    try:
        return importlib.metadata.version("kitaru")
    except importlib.metadata.PackageNotFoundError:
        return _UNKNOWN_VERSION


def _load_cli_entrypoint() -> Callable[[], object]:
    """Import and return the CLI entrypoint."""
    module = importlib.import_module("kitaru.cli")
    return module.cli


def _load_mcp_entrypoint() -> Callable[[], object]:
    """Import and return the MCP entrypoint."""
    module = importlib.import_module("kitaru.mcp")
    return module.main


def _apply_env_translations() -> None:
    """Apply bootstrap-safe KITARU_* -> ZENML_* environment translations."""
    from _kitaru_env import apply_env_translations

    apply_env_translations()


def cli_main() -> object:
    """Bootstrap the `kitaru` console script."""
    ensure_supported_python()
    _apply_env_translations()
    return _load_cli_entrypoint()()


def mcp_main() -> object:
    """Bootstrap the `kitaru-mcp` console script."""
    ensure_supported_python()
    _apply_env_translations()
    return _load_mcp_entrypoint()()
