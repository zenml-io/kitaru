"""Arize Phoenix importer plugin for Kitaru."""

from typing import Any

from kitaru_phoenix_importer.importer import parse

__all__ = ["PhoenixAdapter", "parse"]


def __getattr__(name: str) -> Any:
    """Resolve the adapter on first access."""
    if name == "PhoenixAdapter":
        # Import the adapter lazily so parse() does not load the provider SDK.
        from .adapter import PhoenixAdapter

        return PhoenixAdapter
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
