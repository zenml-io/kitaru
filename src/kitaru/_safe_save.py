"""Shared artifact-save helper with blob fallback for tracking paths."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any


def _safe_save(
    name: str,
    value: Any,
    *,
    artifact_type: str,
    save_func: Callable[..., None],
) -> str:
    """Save an artifact and fall back to a blob repr if serialization fails."""
    try:
        save_func(name, value, type=artifact_type)
        return artifact_type
    except Exception:
        fallback_value = {
            "repr": repr(value),
            "python_type": value.__class__.__name__,
        }
        save_func(name, fallback_value, type="blob")
        return "blob"
