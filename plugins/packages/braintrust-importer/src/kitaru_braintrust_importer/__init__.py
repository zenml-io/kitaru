"""Braintrust trace importer for Kitaru."""

from typing import Any

__all__ = ["BraintrustAdapter"]


def __getattr__(name: str) -> Any:
    """Resolve the adapter on first access."""
    if name == "BraintrustAdapter":
        # Import the adapter lazily so parse() does not load the provider SDK.
        from .adapter import BraintrustAdapter

        return BraintrustAdapter
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
