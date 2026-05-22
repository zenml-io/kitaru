"""Small shared utilities for Replay Lab internals."""

from __future__ import annotations

from typing import Any


def number_or_none(value: Any) -> float | None:
    """Return a float for numeric-looking values, excluding booleans."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None
