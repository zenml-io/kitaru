"""Logfire importer plugin for Kitaru."""

from typing import Any

from kitaru_logfire_importer.importer import parse

__all__ = ["LogfireAdapter", "parse"]


def __getattr__(name: str) -> Any:
    """Resolve the adapter on first access."""
    if name == "LogfireAdapter":
        # Import the adapter lazily so parse() does not load the provider SDK.
        from .adapter import LogfireAdapter

        return LogfireAdapter
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
