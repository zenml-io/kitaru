"""Kitaru product analytics (thin layer over ZenML analytics).

Event name constants are defined here so every ``track()`` call site
references the same canonical string.  Add new events to
:class:`AnalyticsEvent` rather than scattering raw strings.
"""

from __future__ import annotations

import logging
from contextvars import ContextVar
from enum import StrEnum
from typing import Any

logger = logging.getLogger(__name__)

class AnalyticsEvent(StrEnum):
    """Kitaru analytics event names.

    Members are strings and can be passed directly to ``track()``
    and ZenML's ``track(event=...)``.
    """
    CLI_INVOKED = "Kitaru CLI invoked"
    SERVER_CONNECTED = "Kitaru server connected"
    MCP_SERVER_STARTED = "Kitaru MCP server started"
    MCP_TOOL_CALLED = "Kitaru MCP tool called"
    FLOW_DEPLOYED = "Kitaru flow deployed"
    FLOW_REPLAYED = "Kitaru flow replayed"


# ── Source / interface helpers ───────────────────────────────────────────────

# Map set_source() suffixes to their SourceContextTypes enum values.
# The enum values are the canonical strings used in the Source-Context header.
_SUFFIX_TO_SOURCE_VALUE: dict[str, str] = {
    "python": "kitaru-python",
    "cli": "kitaru-cli",
    "mcp": "kitaru-mcp",
    "api": "kitaru-api",
    "ui": "kitaru-ui",  # reserved for future Kitaru UI
}

# Tracks the active Kitaru interface surface for every analytics event.
# Default is "kitaru-python" — overridden at each process entrypoint (CLI, MCP, API).
interface_context: ContextVar[str] = ContextVar(
    "Kitaru-Interface", default="kitaru-python"
)


def set_source(suffix_or_source: str) -> None:
    """Set the ZenML ``Source-Context`` header to the Kitaru source type.

    Accepts either a short suffix (``"python"``, ``"cli"``, ``"mcp"``,
    ``"api"``, ``"ui"``) or the full canonical value
    (``"kitaru-python"``, ``"kitaru-api"``, etc.) — whichever is more
    convenient at the call site.

    Silently ignored if ZenML's analytics module is unavailable.
    """
    try:
        from zenml.analytics import source_context
        from zenml.enums import SourceContextTypes

        suffix = (
            suffix_or_source.removeprefix("kitaru-")
            if suffix_or_source.startswith("kitaru-")
            else suffix_or_source
        )
        value = _SUFFIX_TO_SOURCE_VALUE.get(suffix, f"kitaru-{suffix}")
        source_context.set(SourceContextTypes(value))
    except Exception:
        logger.debug(
            "Failed to set analytics source context for %r",
            suffix_or_source,
            exc_info=True,
        )


def set_interface(suffix_or_source: str) -> None:
    """Set both the ZenML source context and the Kitaru interface context.

    Convenience wrapper that calls :func:`set_source` and updates
    :data:`interface_context` in one shot.  Accepts the same short suffix
    (``"cli"``) or full value (``"kitaru-cli"``) that ``set_source`` does.
    """
    set_source(suffix_or_source)
    suffix = (
        suffix_or_source.removeprefix("kitaru-")
        if suffix_or_source.startswith("kitaru-")
        else suffix_or_source
    )
    value = _SUFFIX_TO_SOURCE_VALUE.get(suffix, f"kitaru-{suffix}")
    interface_context.set(value)


def track(event_name: str, metadata: dict[str, Any] | None = None) -> bool:
    """Track a Kitaru analytics event via ZenML's pipeline.

    Passes ``event_name`` as a plain string to ZenML's ``track()`` (which
    accepts ``Union[AnalyticsEvent, str]``).  Kitaru event names are defined
    here in the Kitaru repo — no corresponding enum entry is needed in ZenML.

    The current ``interface_context`` value is automatically injected into
    every event's metadata under the ``interface`` key.

    Silently returns False if analytics are disabled or if tracking fails.
    """
    try:
        from zenml.analytics import track as _zenml_track

        merged = {"interface": interface_context.get(), **(metadata or {})}
        return _zenml_track(event=event_name, metadata=merged)
    except Exception:
        logger.debug("Analytics tracking failed", exc_info=True)
        return False
