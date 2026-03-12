"""Live terminal renderer for Kitaru flow executions.

Provides a Rich Live display that shows checkpoint-by-checkpoint progress
during interactive ``kitaru run`` sessions, replacing ZenML's console output
with Kitaru-branded visuals.
"""

from __future__ import annotations

import logging
import sys
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any

from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.spinner import Spinner
from rich.table import Table
from rich.text import Text

from kitaru.client import (
    CheckpointCall,
    Execution,
    ExecutionStatus,
    KitaruClient,
)

# ---------------------------------------------------------------------------
# Theme constants
# ---------------------------------------------------------------------------
_BORDER_STYLE = "bright_cyan"
_HEADER_STYLE = "bold bright_cyan"
_LABEL_STYLE = "bold cyan"
_DIM_STYLE = "dim"
_SUCCESS_STYLE = "bold green"
_FAIL_STYLE = "bold red"
_WAIT_STYLE = "bold yellow"
_RUNNING_STYLE = "bold blue"

_STATUS_STYLES: dict[str, str] = {
    ExecutionStatus.COMPLETED: _SUCCESS_STYLE,
    ExecutionStatus.FAILED: _FAIL_STYLE,
    ExecutionStatus.WAITING: _WAIT_STYLE,
    ExecutionStatus.RUNNING: _RUNNING_STYLE,
    ExecutionStatus.CANCELLED: _DIM_STYLE,
}

_STATUS_ICONS: dict[str, str] = {
    ExecutionStatus.COMPLETED: "  ",
    ExecutionStatus.FAILED: "  ",
    ExecutionStatus.WAITING: "  ",
    ExecutionStatus.RUNNING: "  ",
    ExecutionStatus.CANCELLED: "  ",
}


# ---------------------------------------------------------------------------
# ZenML console suppression
# ---------------------------------------------------------------------------


@contextmanager
def _suppress_zenml_console() -> Iterator[None]:
    """Temporarily remove ZenML's console logging handler.

    Preserves ``ZenMLLoggingHandler`` instances so stored logs are unaffected.
    Only active when stdout is an interactive terminal.
    """
    root_logger = logging.getLogger()
    removed: list[tuple[int, logging.Handler]] = []

    try:
        for idx, handler in enumerate(list(root_logger.handlers)):
            if not isinstance(handler, logging.StreamHandler):
                continue
            stream = getattr(handler, "stream", None)
            if stream is None:
                continue
            # Identify ZenML's console handler by its stream type
            stream_type_name = type(stream).__name__
            if stream_type_name == "_ZenMLStdoutStream":
                root_logger.removeHandler(handler)
                removed.append((idx, handler))
    except Exception:
        pass

    try:
        yield
    finally:
        for _idx, handler in reversed(removed):
            root_logger.addHandler(handler)


# ---------------------------------------------------------------------------
# Rendering helpers
# ---------------------------------------------------------------------------


def _format_duration(
    started: datetime | None,
    ended: datetime | None,
) -> str:
    """Render elapsed time as a compact human-readable string."""
    if started is None:
        return "-"
    end = ended or datetime.now(UTC)
    delta = end - started
    total_seconds = int(delta.total_seconds())
    if total_seconds < 0:
        return "-"
    if total_seconds < 60:
        return f"{total_seconds}s"
    minutes, seconds = divmod(total_seconds, 60)
    if minutes < 60:
        return f"{minutes}m {seconds}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes}m"


def _status_text(status: ExecutionStatus | str) -> Text:
    """Build a styled Text for an execution/checkpoint status."""
    status_str = status.value if isinstance(status, ExecutionStatus) else str(status)
    style = _STATUS_STYLES.get(status_str, "")
    icon = _STATUS_ICONS.get(status_str, "  ")
    return Text(f"{icon} {status_str}", style=style)


def _build_checkpoint_table(checkpoints: list[CheckpointCall]) -> Table:
    """Render checkpoints as a compact Rich table."""
    table = Table(
        show_header=True,
        header_style=_LABEL_STYLE,
        border_style=_DIM_STYLE,
        expand=False,
        padding=(0, 1),
        show_edge=False,
    )
    table.add_column("Checkpoint", style="bold", min_width=16)
    table.add_column("Status", min_width=10)
    table.add_column("Duration", justify="right", min_width=8)

    sorted_checkpoints = sorted(
        checkpoints,
        key=lambda c: (c.started_at or datetime.min.replace(tzinfo=UTC), c.name),
    )

    for cp in sorted_checkpoints:
        table.add_row(
            cp.name,
            _status_text(cp.status),
            _format_duration(cp.started_at, cp.ended_at),
        )

    return table


def _build_header(
    *,
    target: str,
    exec_id: str | None,
    execution: Execution | None,
) -> Text:
    """Build the header text block with flow metadata."""
    header = Text()
    header.append("  kitaru run", style=_HEADER_STYLE)
    header.append("\n")

    header.append("  Target: ", style=_LABEL_STYLE)
    header.append(target)
    header.append("\n")

    if exec_id:
        header.append("  Execution: ", style=_LABEL_STYLE)
        header.append(exec_id, style=_DIM_STYLE)
        header.append("\n")

    if execution:
        if execution.flow_name:
            header.append("  Flow: ", style=_LABEL_STYLE)
            header.append(execution.flow_name)
            header.append("\n")

        header.append("  Status: ", style=_LABEL_STYLE)
        header.append_text(_status_text(execution.status))
        header.append("\n")

        if execution.runner_name:
            header.append("  Runner: ", style=_LABEL_STYLE)
            header.append(execution.runner_name)
            header.append("\n")

        header.append("  Elapsed: ", style=_LABEL_STYLE)
        header.append(_format_duration(execution.started_at, execution.ended_at))

    return header


def _build_wait_banner(execution: Execution) -> Text | None:
    """Build a waiting-for-input banner, if applicable."""
    if execution.pending_wait is None:
        return None

    banner = Text()
    banner.append("\n  Waiting for input", style=_WAIT_STYLE)
    banner.append(f": {execution.pending_wait.name}", style="bold")
    if execution.pending_wait.question:
        banner.append(f"\n  {execution.pending_wait.question}", style=_DIM_STYLE)
    return banner


def _build_failure_banner(execution: Execution) -> Text | None:
    """Build a failure summary banner, if applicable."""
    if execution.failure is None:
        return None

    banner = Text()
    banner.append("\n  Failure", style=_FAIL_STYLE)
    banner.append(f": {execution.failure.message}")
    return banner


def _render_execution(
    *,
    target: str,
    exec_id: str | None,
    execution: Execution | None,
) -> Panel:
    """Compose the full Rich renderable for the current execution state."""
    elements: list[Any] = []

    header = _build_header(
        target=target,
        exec_id=exec_id,
        execution=execution,
    )
    elements.append(header)

    if execution and execution.checkpoints:
        elements.append(Text())  # spacer
        elements.append(_build_checkpoint_table(execution.checkpoints))

    if execution:
        wait_banner = _build_wait_banner(execution)
        if wait_banner:
            elements.append(wait_banner)

        failure_banner = _build_failure_banner(execution)
        if failure_banner:
            elements.append(failure_banner)

    return Panel(
        Group(*elements),
        border_style=_BORDER_STYLE,
        expand=False,
        padding=(0, 1),
    )


def _render_submitting(target: str) -> Panel:
    """Render the initial "submitting" state before exec ID is known."""
    elements: list[Any] = []

    header = Text()
    header.append("  kitaru run", style=_HEADER_STYLE)
    header.append("\n")
    header.append("  Target: ", style=_LABEL_STYLE)
    header.append(target)
    header.append("\n\n  ")

    elements.append(header)
    elements.append(Spinner("dots", text="Submitting flow...", style=_DIM_STYLE))

    return Panel(
        Group(*elements),
        border_style=_BORDER_STYLE,
        expand=False,
        padding=(0, 1),
    )


# ---------------------------------------------------------------------------
# Live execution renderer
# ---------------------------------------------------------------------------


class LiveExecutionRenderer:
    """Drives a Rich Live display during interactive flow execution.

    Usage::

        renderer = LiveExecutionRenderer(target="agent.py:my_flow")
        with renderer:
            # install as submission observer callback
            with _submission_observer(renderer.publish_exec_id):
                handle = flow_target.run(**inputs)
        # renderer.result holds the final Execution snapshot
    """

    def __init__(
        self,
        target: str,
        *,
        poll_interval: float = 1.0,
        console: Console | None = None,
    ) -> None:
        self._target = target
        self._poll_interval = poll_interval
        self._console = console or Console()

        self._exec_id: str | None = None
        self._execution: Execution | None = None
        self._error: str | None = None
        self._lock = threading.Lock()
        self._exec_id_event = threading.Event()
        self._stop_event = threading.Event()

        self._live: Live | None = None
        self._poll_thread: threading.Thread | None = None
        self.result: Execution | None = None

    def publish_exec_id(self, exec_id: str) -> None:
        """Called by the submission observer when the exec ID is available."""
        with self._lock:
            self._exec_id = exec_id
        self._exec_id_event.set()

    def __enter__(self) -> LiveExecutionRenderer:
        self._live = Live(
            _render_submitting(self._target),
            console=self._console,
            refresh_per_second=4,
            transient=True,
        )
        self._live.__enter__()

        self._poll_thread = threading.Thread(
            target=self._poll_loop,
            daemon=True,
            name="kitaru-live-poll",
        )
        self._poll_thread.start()
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self._stop_event.set()
        self._exec_id_event.set()  # unblock if waiting

        if self._poll_thread is not None:
            self._poll_thread.join(timeout=5.0)

        # One final fetch
        self._do_poll()

        with self._lock:
            self.result = self._execution

        if self._live is not None:
            self._live.__exit__(*exc_info)

        # Print final static summary
        self._print_final_summary()

    def _poll_loop(self) -> None:
        """Background polling loop."""
        self._exec_id_event.wait()

        while not self._stop_event.is_set():
            self._do_poll()
            self._update_display()
            self._stop_event.wait(timeout=self._poll_interval)

    def _do_poll(self) -> None:
        """Fetch the latest execution state."""
        with self._lock:
            exec_id = self._exec_id

        if exec_id is None:
            return

        try:
            execution = KitaruClient().executions.get(exec_id)
            with self._lock:
                self._execution = execution
                self._error = None
        except Exception as exc:
            with self._lock:
                self._error = str(exc)

    def _update_display(self) -> None:
        """Push the latest renderable to Rich Live."""
        if self._live is None:
            return

        with self._lock:
            exec_id = self._exec_id
            execution = self._execution

        if exec_id is None:
            renderable = _render_submitting(self._target)
        else:
            renderable = _render_execution(
                target=self._target,
                exec_id=exec_id,
                execution=execution,
            )

        self._live.update(renderable)

    def _print_final_summary(self) -> None:
        """Print a static final panel after Live is torn down."""
        with self._lock:
            exec_id = self._exec_id
            execution = self._execution

        if execution is None and exec_id is None:
            return

        # Build final panel (non-live, persists in terminal)
        panel = _render_execution(
            target=self._target,
            exec_id=exec_id,
            execution=execution,
        )
        self._console.print(panel)


def is_interactive() -> bool:
    """Check whether stdout is an interactive terminal."""
    return hasattr(sys.stdout, "isatty") and sys.stdout.isatty()
