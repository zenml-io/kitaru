"""Shared lightweight coercion helpers for client mappers."""

from __future__ import annotations

from typing import Any


def optional_string(value: Any) -> str | None:
    """Return a stripped string or ``None``."""
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


__all__ = ["optional_string"]
