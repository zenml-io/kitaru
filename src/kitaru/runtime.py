"""Internal runtime support for the Kitaru SDK.

This module provides shared utilities used across the SDK implementation.
It is not part of the public API surface.

Note: This is scaffolding for Phase 1. Runtime context tracking
(current flow, current checkpoint, execution IDs) will be implemented
in a later phase.
"""

from __future__ import annotations

from typing import NoReturn


def _not_implemented(name: str) -> NoReturn:
    """Raise NotImplementedError with a consistent message."""
    raise NotImplementedError(
        f"kitaru.{name}() is not yet implemented. "
        f"The Kitaru SDK is under active development."
    )
