"""Kitaru product analytics (thin layer over ZenML analytics).

Event name constants are defined here so every ``track()`` call site
references the same canonical string.  Add new events to
:class:`AnalyticsEvent` rather than scattering raw strings.
"""

from __future__ import annotations

import functools
import importlib.metadata
import logging
from enum import StrEnum
from typing import Any

from kitaru._version import resolve_installed_version

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
    AUTH_TOKEN_PRINTED = "Kitaru auth token printed"
    AUTH_SERVICE_ACCOUNT_CREATED = "Kitaru auth service account created"
    AUTH_SERVICE_ACCOUNT_UPDATED = "Kitaru auth service account updated"
    AUTH_SERVICE_ACCOUNT_DELETED = "Kitaru auth service account deleted"
    AUTH_API_KEY_CREATED = "Kitaru auth API key created"
    AUTH_API_KEY_UPDATED = "Kitaru auth API key updated"
    AUTH_API_KEY_ROTATED = "Kitaru auth API key rotated"
    AUTH_API_KEY_DELETED = "Kitaru auth API key deleted"
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
    EXECUTION_STATISTICS_QUERIED = "Kitaru execution statistics queried"

    # Feature adoption
    LLM_CALLED = "Kitaru LLM called"
    ARTIFACT_SAVED = "Kitaru artifact saved"
    ARTIFACT_LOADED = "Kitaru artifact loaded"
    SECRET_UPSERTED = "Kitaru secret upserted"
    SECRET_READ = "Kitaru secret read"
    STACK_CREATED = "Kitaru stack created"
    STACK_ACTIVATED = "Kitaru stack activated"
    MODEL_ALIAS_REGISTERED = "Kitaru model alias registered"
    LOG_STORE_CONFIGURED = "Kitaru log store configured"
    CHECKPOINT_INVOKED = "Kitaru checkpoint invoked"
    CLEAN_COMPLETED = "Kitaru clean completed"
    INFO_VIEWED = "Kitaru info viewed"
    INFO_EXPORTED = "Kitaru info exported"
    STATUS_VIEWED = "Kitaru status viewed"
    DEPLOYMENT_BUILT = "Kitaru deployment built"
    DEPLOYMENT_DEPLOYED = "Kitaru deployment deployed"
    DEPLOYMENT_INVOKED = "Kitaru deployment invoked"
    DEPLOYMENT_CURL_GENERATED = "Kitaru deployment curl generated"
    DEPLOYMENT_TAG_CLEANUP_FAILED = "Kitaru deployment tag cleanup failed"
    DEPLOYMENT_TAGGED = "Kitaru deployment tagged"
    DEPLOYMENT_UNTAGGED = "Kitaru deployment untagged"
    DEPLOYMENT_DELETED = "Kitaru deployment deleted"

    # Adapter
    PYDANTIC_AI_WRAPPED = "Kitaru PydanticAI wrapped"
    PYDANTIC_AI_RUN_COMPLETED = "Kitaru PydanticAI run completed"
    OPENAI_AGENTS_WRAPPED = "Kitaru OpenAI Agents wrapped"
    OPENAI_AGENTS_RUN_COMPLETED = "Kitaru OpenAI Agents run completed"
    OPENAI_AGENTS_CALL_CHECKPOINTED = "Kitaru OpenAI Agents call checkpointed"
    CLAUDE_AGENT_SDK_WRAPPED = "Kitaru Claude Agent SDK wrapped"
    CLAUDE_AGENT_SDK_RUN_COMPLETED = "Kitaru Claude Agent SDK run completed"
    GEMINI_INTERACTIONS_WRAPPED = "Kitaru Gemini Interactions wrapped"
    GEMINI_INTERACTIONS_RUN_COMPLETED = "Kitaru Gemini Interactions run completed"
    LANGGRAPH_WRAPPED = "Kitaru LangGraph wrapped"
    LANGGRAPH_RUN_COMPLETED = "Kitaru LangGraph run completed"
    LANGGRAPH_INTERRUPTED = "Kitaru LangGraph interrupted"
    FAST_AGENT_WRAPPED = "Kitaru fast-agent wrapped"


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


@functools.lru_cache(maxsize=1)
def resolve_zenml_version() -> str:
    """Resolve the installed ZenML version lazily."""
    try:
        return importlib.metadata.version("zenml")
    except importlib.metadata.PackageNotFoundError:
        return "unknown"


def _safe_kitaru_version() -> str:
    """Kitaru version with fallback — never raises."""
    try:
        return resolve_installed_version()
    except Exception:
        return "unknown"


def _safe_zenml_version() -> str:
    """ZenML version with fallback — never raises."""
    try:
        return resolve_zenml_version()
    except Exception:
        return "unknown"


def track(event_name: AnalyticsEvent, metadata: dict[str, Any] | None = None) -> bool:
    """Track a Kitaru analytics event via ZenML's pipeline.

    Passes ``event_name`` as a plain string to ZenML's ``track()`` (which
    accepts ``Union[AnalyticsEvent, str]``).  ``AnalyticsEvent`` is a
    ``StrEnum``, so it works as a ``str`` at the ZenML boundary while giving
    callers type-checked event names.

    The caller metadata is copied before central Kitaru fields are added. This
    keeps call sites from mutating shared dictionaries and makes
    ``kitaru_version`` / ``zenml_version`` authoritative for every event.

    Silently returns False if analytics are disabled or if tracking fails.
    """
    try:
        enriched_metadata = dict(metadata or {})
        enriched_metadata["kitaru_version"] = _safe_kitaru_version()
        enriched_metadata["zenml_version"] = _safe_zenml_version()

        # ZenML's analytics import chain emits a few INFO/DEBUG log lines on
        # first import and during track(); those leak into CLI output where
        # Kitaru owns the UX. Silence the root logger for the duration of the
        # ZenML call and restore the prior level afterwards.
        previous_disable_level = logging.root.manager.disable
        try:
            logging.disable(logging.CRITICAL)
            from zenml.analytics import track as _zenml_track

            return _zenml_track(
                event=event_name, metadata=enriched_metadata
            )  # ZenML accepts Union[AnalyticsEvent, str]
        finally:
            logging.disable(previous_disable_level)
    except Exception:
        logger.debug("Analytics tracking failed", exc_info=True)
        return False
