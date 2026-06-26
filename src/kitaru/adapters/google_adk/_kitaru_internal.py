"""Adapter-local access to Kitaru private runtime helpers."""

from __future__ import annotations

from typing import Any

from kitaru import runtime
from kitaru.errors import KitaruFeatureNotAvailableError


def _runtime_helper(name: str) -> Any:
    helper = getattr(runtime, name, None)
    if helper is None:
        raise KitaruFeatureNotAvailableError(
            "Installed Kitaru runtime does not expose private helper "
            f"`{name}` required by the Google ADK adapter."
        )
    return helper


def is_inside_flow() -> bool:
    return bool(_runtime_helper("_is_inside_flow")())


def is_inside_checkpoint() -> bool:
    return bool(_runtime_helper("_is_inside_checkpoint")())


def get_current_checkpoint_id() -> str | None:
    value = _runtime_helper("_get_current_checkpoint_id")()
    return str(value) if value is not None else None


def get_current_checkpoint_name() -> str | None:
    value = _runtime_helper("_get_current_checkpoint_name")()
    return str(value) if value is not None else None
