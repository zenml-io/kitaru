"""LangSmith trace importer for Kitaru."""

from typing import Any

__all__ = ["LangSmithAdapter"]


def __getattr__(name: str) -> Any:
    """Resolve the adapter on first access."""
    if name == "LangSmithAdapter":
        # Import the adapter lazily so parse() does not load the provider SDK.
        from .adapter import LangSmithAdapter

        return LangSmithAdapter
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
