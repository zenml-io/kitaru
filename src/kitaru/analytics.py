"""Kitaru product analytics (thin layer over ZenML analytics).

Event name constants are defined here so every ``track()`` call site
references the same canonical string.  Add new events to
:class:`AnalyticsEvent` rather than scattering raw strings.
"""

from __future__ import annotations

import logging
from enum import StrEnum
from typing import Any

logger = logging.getLogger(__name__)


class AnalyticsEvent(StrEnum):
    """Kitaru analytics event names.

    Members are strings and can be passed directly to ``track()``
    and ZenML's ``track(event=...)``.
    """

    # Entry surfaces
    CLI_INVOKED = "Kitaru CLI invoked"
    MCP_SERVER_STARTED = "Kitaru MCP server started"
    MCP_TOOL_CALLED = "Kitaru MCP tool called"
    PROJECT_INITIALIZED = "Kitaru project initialized"
    LOGIN_COMPLETED = "Kitaru login completed"
    LOCAL_SERVER_STARTED = "Kitaru local server started"
    LOCAL_SERVER_STOPPED = "Kitaru local server stopped"

    # Flow lifecycle
    FLOW_SUBMITTED = "Kitaru flow submitted"
    FLOW_TERMINAL = "Kitaru flow terminal"
    FLOW_REPLAYED = "Kitaru flow replayed"
    REPLAY_REQUESTED = "Kitaru flow replay requested"
    REPLAY_FAILED = "Kitaru flow replay failed"

    # Execution management
    WAIT_CREATED = "Kitaru wait created"
    WAIT_RESOLVED = "Kitaru wait resolved"
    EXECUTION_RETRIED = "Kitaru execution retried"
    EXECUTION_RESUMED = "Kitaru execution resumed"
    EXECUTION_CANCELLED = "Kitaru execution cancelled"

    # Feature adoption
    LLM_CALLED = "Kitaru LLM called"
    ARTIFACT_SAVED = "Kitaru artifact saved"
    ARTIFACT_LOADED = "Kitaru artifact loaded"
    SECRET_UPSERTED = "Kitaru secret upserted"
    STACK_CREATED = "Kitaru stack created"
    STACK_ACTIVATED = "Kitaru stack activated"
    MODEL_ALIAS_REGISTERED = "Kitaru model alias registered"
    LOG_STORE_CONFIGURED = "Kitaru log store configured"

    # Adapter
    PYDANTIC_AI_WRAPPED = "Kitaru PydanticAI wrapped"
    PYDANTIC_AI_RUN_COMPLETED = "Kitaru PydanticAI run completed"


def set_source(suffix_or_source: str) -> None:
    """Set the ZenML ``Source-Context`` header to a Kitaru source type.

    Accepts either a short suffix (``"cli"``) or the full canonical value
    (``"kitaru-cli"``).  The ``kitaru-`` prefix is added automatically
    when a bare suffix is given.

    Silently ignored if ZenML's analytics module is unavailable.
    """
    try:
        from zenml.analytics import source_context
        from zenml.enums import SourceContextTypes

        if not suffix_or_source.startswith("kitaru-"):
            suffix_or_source = f"kitaru-{suffix_or_source}"
        source_context.set(SourceContextTypes(suffix_or_source))
    except Exception:
        logger.debug(
            "Failed to set analytics source context for %r",
            suffix_or_source,
            exc_info=True,
        )


def track(event_name: str, metadata: dict[str, Any] | None = None) -> bool:
    """Track a Kitaru analytics event via ZenML's pipeline.

    Passes ``event_name`` as a plain string to ZenML's ``track()`` (which
    accepts ``Union[AnalyticsEvent, str]``).  Kitaru event names are defined
    here in the Kitaru repo — no corresponding enum entry is needed in ZenML.

    Silently returns False if analytics are disabled or if tracking fails.
    """
    try:
        from zenml.analytics import track as _zenml_track

        return _zenml_track(
            event=event_name, metadata=metadata or {}
        )  # ZenML accepts Union[AnalyticsEvent, str]
    except Exception:
        logger.debug("Analytics tracking failed", exc_info=True)
        return False
