"""Lazy import guard for Dapr SDK dependencies."""

from __future__ import annotations

import importlib

_DAPR_INSTALL_ERROR = (
    "Dapr SDK dependencies are not installed. Install with: pip install kitaru[dapr]"
)


def require_dapr_sdk() -> None:
    """Ensure the Dapr SDK is available, raising a clear error if not."""
    try:
        importlib.import_module("dapr.clients")
    except ImportError:
        raise ImportError(_DAPR_INSTALL_ERROR) from None
