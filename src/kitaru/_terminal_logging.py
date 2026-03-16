"""Terminal log intercept for Kitaru.

This module replaces ZenML's console log handler with a Kitaru-branded handler
that rewrites lifecycle messages (step→checkpoint, pipeline→flow, run→execution)
before they reach the terminal.  ZenML's storage handler is preserved untouched
so ``kitaru executions logs`` continues to see original ZenML text.

The core invariant: **LogRecord objects are never mutated.**  The rewrite is
derived from ``record.getMessage()`` inside the handler's ``emit()`` and only
affects the string written to the terminal.

This module is internal — it is not part of the public API surface.
"""

from __future__ import annotations

import logging
import re
import sys
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from kitaru._source_aliases import normalize_aliases_in_text

# ---------------------------------------------------------------------------
# Decision types
# ---------------------------------------------------------------------------

_TerminalKind = str  # "info" | "detail" | "success" | "warning" | "error"


@dataclass(frozen=True)
class _TerminalDecision:
    """A resolved decision about how to render a log record for the terminal."""

    kind: _TerminalKind
    text: str


# ---------------------------------------------------------------------------
# Rewrite / drop rules
# ---------------------------------------------------------------------------

_REWRITE_RULES: list[tuple[re.Pattern[str], _TerminalKind | None, str]] = [
    # Step lifecycle → Checkpoint lifecycle
    (
        re.compile(r"^Step `(.+?)` has started\.$"),
        "info",
        "Checkpoint `{0}` started.",
    ),
    (
        re.compile(r"^Step `(.+?)` has finished in `(.+?)`\.$"),
        "success",
        "Checkpoint `{0}` finished in {1}.",
    ),
    (
        re.compile(r"^Step `(.+?)` finished successfully in (.+)\.$"),
        "success",
        "Checkpoint `{0}` finished in {1}.",
    ),
    (
        re.compile(r"^Step `(.+?)` finished successfully\.$"),
        "success",
        "Checkpoint `{0}` finished.",
    ),
    (
        re.compile(r"^Step `(.+?)` failed after (.+)\.$"),
        "error",
        "Checkpoint `{0}` failed after {1}.",
    ),
    (
        re.compile(r"^Step `(.+?)` failed\.$"),
        "error",
        "Checkpoint `{0}` failed.",
    ),
    (
        re.compile(r"^Step `(.+?)` failed\. Remaining retries: (\d+)\.$"),
        "warning",
        "Checkpoint `{0}` failed. Retries remaining: {1}.",
    ),
    (
        re.compile(r"^Step `(.+?)` stopped(?:\.|.after .+\.)$"),
        "warning",
        "Checkpoint `{0}` stopped.",
    ),
    (
        re.compile(r"^Step `(.+?)` launched\.$"),
        "info",
        "Checkpoint `{0}` launched.",
    ),
    (
        re.compile(r"^Using cached version of step `(.+?)`\.$"),
        "detail",
        "Checkpoint `{0}` cached.",
    ),
    (
        re.compile(r"^Skipping step `(.+?)`\.$"),
        "info",
        "Skipping checkpoint `{0}`.",
    ),
    (
        re.compile(r"^Failed to run step `(.+?)`: (.+)$"),
        "error",
        "Checkpoint `{0}` failed: {1}",
    ),
    # Pipeline lifecycle → Flow lifecycle
    (
        re.compile(r"^Initiating a new run for the pipeline: `(.+?)`\.$"),
        "info",
        "Starting flow `{0}`.",
    ),
    (
        re.compile(r"^Pipeline completed successfully\.$"),
        "success",
        "Flow completed.",
    ),
    (
        re.compile(r"^Pausing pipeline run `(.+?)`\.$"),
        "warning",
        "Pausing execution `{0}`.",
    ),
    (
        re.compile(r"^Resuming run `(.+?)`\.$"),
        "info",
        "Resuming execution `{0}`.",
    ),
    (
        re.compile(r"^Continuing existing run `(.+?)`\.$"),
        "info",
        "Continuing execution `{0}`.",
    ),
    (
        re.compile(r"^Run `(.+?)` is already finished\.$"),
        "info",
        "Execution `{0}` already finished.",
    ),
    (
        re.compile(r"^Stopping isolated steps\.$"),
        "warning",
        "Stopping isolated checkpoints.",
    ),
    # Stack/config info
    (
        re.compile(r"^Using stack: `(.+?)`$"),
        "detail",
        "Stack: {0}",
    ),
    (
        re.compile(r"^Caching is disabled by default for `(.+?)`\.$"),
        "detail",
        "Caching disabled for `{0}`.",
    ),
    # Dashboard URL
    (
        re.compile(r"^Dashboard URL for Pipeline Run: (.+)$"),
        "detail",
        "Execution URL: {0}",
    ),
]

_DROP_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"^You can visualize your pipeline runs in the `ZenML"),
    re.compile(r"^Using user: "),
    re.compile(r"^Using a build:$"),
    re.compile(r"^\s*Image\(s\): "),
    re.compile(r"^ZenML version \(different"),
    re.compile(r"^Python version \(different"),
    re.compile(r"^Registered new pipeline:"),
    re.compile(r"^\s+\w+: `"),  # component listing ("  orchestrator: `default`")
]


# ---------------------------------------------------------------------------
# Decision logic
# ---------------------------------------------------------------------------


def _level_to_kind(levelno: int) -> _TerminalKind:
    if levelno >= logging.ERROR:
        return "error"
    if levelno >= logging.WARNING:
        return "warning"
    return "info"


def _apply_zenml_rules(
    logger_name: str,
    msg: str,
    levelno: int,
) -> _TerminalDecision | None:
    """Apply rewrite/drop rules to a ZenML log message."""
    for pattern in _DROP_PATTERNS:
        if pattern.search(msg):
            return None

    for pattern, kind, template in _REWRITE_RULES:
        m = pattern.match(msg)
        if m:
            groups = [normalize_aliases_in_text(g) for g in m.groups()]
            text = template.format(*groups)
            resolved_kind = kind if kind is not None else _level_to_kind(levelno)
            return _TerminalDecision(kind=resolved_kind, text=text)

    # Fallback: pass through with alias cleanup
    cleaned = normalize_aliases_in_text(msg)
    return _TerminalDecision(kind=_level_to_kind(levelno), text=cleaned)


def _decide(record: logging.LogRecord) -> _TerminalDecision | None:
    """Decide how to render a log record for the terminal.

    Returns ``None`` to indicate the record should be dropped (not displayed).
    """
    msg = record.getMessage()

    if record.name.startswith("zenml."):
        return _apply_zenml_rules(record.name, msg, record.levelno)

    # Non-ZenML records (Kitaru SDK or user code): pass through with alias
    # cleanup in case alias names leaked into messages.
    kind = _level_to_kind(record.levelno)
    return _TerminalDecision(kind=kind, text=normalize_aliases_in_text(msg))


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

_COLORS: dict[str, str] = {
    "info": "\x1b[37m",  # white
    "detail": "\x1b[90m",  # dim gray
    "success": "\x1b[32m",  # green
    "warning": "\x1b[33m",  # yellow
    "error": "\x1b[31m",  # red
    "reset": "\x1b[0m",
}

_MARKERS: dict[str, str] = {
    "info": "\u203a",
    "detail": "\u203a",
    "success": "\u2713",
    "warning": "!",
    "error": "\u2716",
}


def _render(decision: _TerminalDecision, *, interactive: bool) -> str:
    """Render a terminal decision to a display string."""
    if not interactive:
        return f"Kitaru: {decision.text}"

    color = _COLORS.get(decision.kind, _COLORS["reset"])
    marker = _MARKERS.get(decision.kind, "\u203a")
    reset = _COLORS["reset"]
    return f"{color}Kitaru {marker}{reset} {decision.text}"


# ---------------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------------


def _get_bypass_write() -> Callable[[str], Any]:
    """Get a write callable that bypasses ZenML's stdout wrapper."""
    try:
        from zenml.logger import _original_stdout_write

        if _original_stdout_write is not None:
            return _original_stdout_write
    except ImportError:
        pass
    return sys.stdout.write


class _KitaruTerminalHandler(logging.Handler):
    """Intercepts log records, rewrites ZenML messages, writes to terminal.

    This handler never modifies the ``LogRecord`` — the rewrite is derived
    from ``record.getMessage()`` and only affects the string written to the
    terminal.  Downstream handlers (notably ``ZenMLLoggingHandler``) still
    see the original record.
    """

    def __init__(self) -> None:
        super().__init__()
        self._write = _get_bypass_write()
        self._interactive = hasattr(sys.stdout, "isatty") and sys.stdout.isatty()

    def emit(self, record: logging.LogRecord) -> None:
        try:
            decision = _decide(record)
            if decision is None:
                return
            text = _render(decision, interactive=self._interactive)
            self._write(text + "\n")
        except Exception:
            self.handleError(record)


# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------


def install_terminal_log_intercept() -> None:
    """Replace ZenML's console handler with a Kitaru terminal handler.

    This function is idempotent: calling it multiple times (including across
    ``importlib.reload()``) will not add duplicate handlers.
    """
    from zenml.logger import ConsoleFormatter, ZenMLLoggingHandler

    root = logging.getLogger()

    existing_kitaru: _KitaruTerminalHandler | None = None
    zenml_console_indices: list[int] = []

    for i, handler in enumerate(root.handlers):
        if isinstance(handler, _KitaruTerminalHandler):
            existing_kitaru = handler
            continue
        if isinstance(handler, ZenMLLoggingHandler):
            continue
        if isinstance(getattr(handler, "formatter", None), ConsoleFormatter):
            zenml_console_indices.append(i)

    kitaru_handler = existing_kitaru or _KitaruTerminalHandler()

    if zenml_console_indices:
        # Replace the first ZenML console handler with ours, remove extras
        first_idx = zenml_console_indices[0]
        new_handlers: list[logging.Handler] = []
        for i, handler in enumerate(root.handlers):
            if (
                isinstance(handler, _KitaruTerminalHandler)
                and handler is not kitaru_handler
            ):
                continue
            if i == first_idx:
                if existing_kitaru is None:
                    new_handlers.append(kitaru_handler)
                    handler.close()
                else:
                    # Already have a Kitaru handler in the list; just remove
                    # the ZenML one.
                    handler.close()
                continue
            if i in zenml_console_indices[1:]:
                handler.close()
                continue
            new_handlers.append(handler)

        # Ensure the Kitaru handler is in the list exactly once
        if kitaru_handler not in new_handlers:
            new_handlers.insert(first_idx, kitaru_handler)

        root.handlers = new_handlers
    elif existing_kitaru is None:
        # No ZenML console handler found; add ours as a fallback
        root.addHandler(kitaru_handler)
