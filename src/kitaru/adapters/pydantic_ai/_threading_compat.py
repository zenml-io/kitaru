"""Compatibility helpers for Pydantic AI sync-tool threading.

Pydantic AI normally moves synchronous tool functions to a worker thread. That
is safe for ordinary tools, but Kitaru waits must be created from the workflow
thread. The upstream hook is run-scoped rather than per-tool-scoped, so callers
should enable it only for run shapes that need direct tool-body waits. Keep the
private Pydantic AI import isolated here so the rest of the adapter has one
small seam to update if the upstream hook changes.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import ExitStack, contextmanager
from typing import Any

from ._logging import logger

_DISABLE_THREADS_ATTR = "disable_threads"


def _resolve_disable_threads() -> Callable[[], Any] | None:
    """Return Pydantic AI's inline-sync-tools hook if it is available."""
    try:
        from pydantic_ai import _utils as pydantic_ai_utils
    except Exception:  # pragma: no cover - adapter import already guards this
        logger.debug(
            "Could not import pydantic_ai._utils for Kitaru thread compatibility.",
            exc_info=True,
        )
        return None

    hook = getattr(pydantic_ai_utils, _DISABLE_THREADS_ATTR, None)
    if callable(hook):
        return hook

    logger.debug(
        "pydantic_ai._utils.disable_threads is unavailable; sync tools may run "
        "on worker threads."
    )
    return None


@contextmanager
def inline_sync_tool_execution(*, enabled: bool) -> Iterator[bool]:
    """Temporarily ask Pydantic AI to run sync tools inline.

    Args:
        enabled: Whether this run shape needs flow-thread-compatible sync tools.

    Yields:
        ``True`` when the Pydantic AI hook was entered, otherwise ``False``.
        Missing or unusable hooks degrade to a no-op so ordinary non-HITL runs do
        not fail at construction time. If a direct tool-body wait later reaches
        the workflow runtime from a worker thread, ``wait_for_input()`` rewrites
        that specific error into user-facing guidance.
    """
    if not enabled:
        yield False
        return

    hook = _resolve_disable_threads()
    if hook is None:
        yield False
        return

    try:
        manager = hook()
    except Exception:
        logger.warning(
            "Could not create Pydantic AI inline-sync-tools context; sync tools "
            "may run on worker threads.",
            exc_info=True,
        )
        yield False
        return

    stack = ExitStack()
    try:
        stack.enter_context(manager)
    except Exception:
        logger.warning(
            "Could not enter Pydantic AI inline-sync-tools context; sync tools "
            "may run on worker threads.",
            exc_info=True,
        )
        yield False
        return

    with stack:
        yield True
