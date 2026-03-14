"""Lightweight package version helpers."""

from __future__ import annotations

import functools
import importlib.metadata

_UNKNOWN_VERSION = "unknown"


@functools.lru_cache(maxsize=1)
def resolve_installed_version() -> str:
    """Resolve the installed Kitaru version lazily."""
    try:
        return importlib.metadata.version("kitaru")
    except importlib.metadata.PackageNotFoundError:
        return _UNKNOWN_VERSION
