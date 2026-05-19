"""Compatibility helpers for Pydantic AI sync-tool threading.

Pydantic AI normally moves synchronous tool functions to a worker thread. That
is safe for ordinary tools, but Kitaru waits must be created from the workflow
thread. The upstream hook is run-scoped rather than per-tool-scoped, so callers
must enable it explicitly for run shapes that need direct tool-body waits. Keep
the private Pydantic AI import isolated here so the rest of the adapter has one
small seam to update if the upstream hook changes.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import ExitStack, contextmanager
from importlib import metadata
from typing import Any

from kitaru.errors import KitaruUsageError

_DISABLE_THREADS_ATTR = "disable_threads"


def _pydantic_ai_version() -> str:
    """Return installed Pydantic AI distribution details for error messages."""
    versions: list[str] = []
    for distribution in ("pydantic-ai-slim", "pydantic-ai"):
        try:
            versions.append(f"{distribution} {metadata.version(distribution)}")
        except metadata.PackageNotFoundError:
            continue
    return ", ".join(versions) if versions else "unknown Pydantic AI version"


def _compatibility_error(reason: str) -> KitaruUsageError:
    version = _pydantic_ai_version()
    return KitaruUsageError(
        "`allow_sync_tool_body_waits=True` asks Kitaru to keep synchronous "
        "Pydantic AI tool bodies on the workflow thread so direct "
        "`kp.wait_for_input(...)` calls can create flow-scope waits safely. "
        f"Kitaru could not enable that compatibility path because {reason}. "
        f"Installed Pydantic AI: {version}. Use `@hitl_tool(...)`, move the "
        "wait into explicit `@kitaru.flow` code, or install a Pydantic AI "
        "version that still exposes `pydantic_ai._utils.disable_threads`."
    )


def _resolve_disable_threads() -> Callable[[], Any]:
    """Return Pydantic AI's inline-sync-tools hook or raise guidance."""
    try:
        from pydantic_ai import _utils as pydantic_ai_utils
    except Exception as exc:  # pragma: no cover - adapter import already guards this
        raise _compatibility_error("`pydantic_ai._utils` could not be imported") from exc

    hook = getattr(pydantic_ai_utils, _DISABLE_THREADS_ATTR, None)
    if callable(hook):
        return hook

    if hook is None:
        reason = "`pydantic_ai._utils.disable_threads` is missing"
    else:
        reason = "`pydantic_ai._utils.disable_threads` is not callable"
    raise _compatibility_error(reason)


@contextmanager
def inline_sync_tool_execution(*, enabled: bool) -> Iterator[bool]:
    """Temporarily ask Pydantic AI to run sync tools inline.

    Args:
        enabled: Whether this run shape explicitly opted into flow-thread-
            compatible sync tools.

    Yields:
        ``True`` when the Pydantic AI hook was entered, otherwise ``False``.

    Raises:
        KitaruUsageError: If ``enabled`` is true but the private Pydantic AI
            hook is missing, non-callable, or cannot be entered. This fails
            before user sync tool bodies can run side effects on the wrong
            thread.
    """
    if not enabled:
        yield False
        return

    hook = _resolve_disable_threads()

    try:
        manager = hook()
    except Exception as exc:
        raise _compatibility_error(
            "`pydantic_ai._utils.disable_threads()` could not create a context manager"
        ) from exc

    stack = ExitStack()
    try:
        stack.enter_context(manager)
    except Exception as exc:
        raise _compatibility_error(
            "`pydantic_ai._utils.disable_threads()` could not be entered"
        ) from exc

    with stack:
        yield True
