"""Kitaru product analytics (thin layer over ZenML analytics)."""

from __future__ import annotations

import logging
from contextvars import ContextVar
from typing import Any

logger = logging.getLogger(__name__)

# Map set_source() suffixes to their SourceContextTypes enum values.
# The enum values are the canonical strings used in the Source-Context header.
_SUFFIX_TO_SOURCE_VALUE: dict[str, str] = {
    "python": "kitaru-python",
    "cli": "kitaru-cli",
    "mcp": "kitaru-mcp",
    "api": "kitaru-api",
    "dashboard": "kitaru-dashboard",  # reserved for future Kitaru dashboard UI
}


def set_source(suffix_or_source: str) -> None:
    """Set the ZenML ``Source-Context`` header to the Kitaru source type.

    Accepts either a short suffix (``"python"``, ``"cli"``, ``"mcp"``,
    ``"api"``, ``"dashboard"``) or the full canonical value
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


# Tracks the active Kitaru interface surface for every analytics event.
# Default is "kitaru-python" — overridden at each process entrypoint (CLI, MCP, API).
interface_context: ContextVar[str] = ContextVar(
    "Kitaru-Interface", default="kitaru-python"
)


def track(event_name: str, metadata: dict[str, Any] | None = None) -> bool:
    """Track a Kitaru analytics event via ZenML's pipeline.

    Silently returns False if analytics are disabled or if tracking fails.
    The current ``interface_context`` value is automatically injected into
    every event's metadata under the ``interface`` key.
    """
    try:
        from zenml.analytics import track as _zenml_track
        from zenml.analytics.enums import AnalyticsEvent

        merged = {"interface": interface_context.get(), **(metadata or {})}
        analytics_event = AnalyticsEvent(event_name)
        return _zenml_track(event=analytics_event, metadata=merged)
    except ValueError:
        # A ValueError here means event_name doesn't match any AnalyticsEvent value.
        # This is a programming error, so warn rather than silently drop.
        logger.warning(
            "Unknown analytics event %r — verify it exists in AnalyticsEvent; tracking skipped.",
            event_name,
        )
        return False
    except Exception:
        logger.debug("Analytics tracking failed", exc_info=True)
        return False
