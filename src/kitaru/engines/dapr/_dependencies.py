"""Lazy import guard for Dapr SDK dependencies."""

from __future__ import annotations

import importlib

_DAPR_INSTALL_ERROR = (
    "Dapr SDK dependencies are not installed. Install with: pip install kitaru[dapr]"
)

_DAPR_WORKFLOW_INSTALL_ERROR = (
    "Dapr workflow SDK dependencies are not installed. "
    "Install with: pip install kitaru[dapr]"
)


def require_dapr_sdk() -> None:
    """Ensure the Dapr state client SDK is available."""
    try:
        importlib.import_module("dapr.clients")
    except ImportError:
        raise ImportError(_DAPR_INSTALL_ERROR) from None


def require_dapr_workflow_sdk() -> None:
    """Ensure the Dapr workflow extension SDK is available."""
    try:
        importlib.import_module("dapr.ext.workflow")
    except ImportError:
        raise ImportError(_DAPR_WORKFLOW_INSTALL_ERROR) from None
