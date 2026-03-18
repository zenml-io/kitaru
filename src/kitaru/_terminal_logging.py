"""Terminal log intercept for Kitaru.

This module replaces ZenML's console log handler with a Kitaru-branded handler
that rewrites lifecycle messages (step→checkpoint, pipeline→flow, run→execution)
before they reach the terminal. ZenML's storage handler is preserved untouched so
``kitaru executions logs`` continues to see original ZenML text.

The core invariant: **LogRecord objects are never mutated.** The rewrite is
derived from ``record.getMessage()`` inside the handler's ``emit()`` and only
affects the string written to the terminal.

This module is internal — it is not part of the public API surface.
"""

from __future__ import annotations

import functools
import logging
import os
import re
import sys
import time
import traceback
from collections import deque
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field
from io import TextIOBase
from threading import RLock, local
from types import TracebackType
from typing import IO, Any, Literal, cast
from urllib.parse import urlparse

from rich.console import Console, ConsoleOptions, Group, RenderResult
from rich.live import Live
from rich.style import Style
from rich.text import Text

from kitaru._env import KITARU_DEBUG_ENV
from kitaru._source_aliases import (
    normalize_aliases_in_text,
    normalize_checkpoint_name,
    normalize_flow_name,
)
from kitaru._version import resolve_installed_version

# ---------------------------------------------------------------------------
# Decision / tracker types
# ---------------------------------------------------------------------------

_TerminalKind = Literal["info", "detail", "success", "warning", "error"]
_CheckpointStatus = Literal[
    "pending",
    "running",
    "passed",
    "failed",
    "cached",
    "waiting",
]
_TerminalStatus = Literal["completed", "failed"]
_SnapshotTerminalStatus = Literal["running", "waiting", "completed", "failed"]
_TrackerEventKind = Literal[
    "flow_started",
    "flow_completed",
    "stack_selected",
    "execution_url",
    "execution_resumed",
    "execution_continued",
    "execution_already_finished",
    "checkpoint_launched",
    "checkpoint_started",
    "checkpoint_finished",
    "checkpoint_cached",
    "checkpoint_failed",
    "checkpoint_stopped",
    "checkpoint_skipped",
    "wait_condition",
]
_ExcInfo = (
    tuple[type[BaseException], BaseException, TracebackType | None]
    | tuple[None, None, None]
    | None
)


@dataclass(frozen=True)
class _TerminalDecision:
    """A resolved decision about how to render a log record for the terminal."""

    kind: _TerminalKind
    text: str


@dataclass(frozen=True)
class _TrackerEvent:
    """Structured event derived from a terminal log record."""

    kind: _TrackerEventKind
    checkpoint_name: str | None = None
    flow_name: str | None = None
    execution_id: str | None = None
    execution_url: str | None = None
    stack_name: str | None = None
    wait_name: str | None = None
    wait_type: str | None = None
    timeout_seconds: str | None = None
    poll_seconds: str | None = None
    retry_remaining: int | None = None


@dataclass(frozen=True)
class _ResolvedLogRecord:
    """Classification result for a single LogRecord."""

    decision: _TerminalDecision | None
    tracker_event: _TrackerEvent | None = None
    live_managed: bool = False


@dataclass(frozen=True)
class _RewriteRule:
    """Regex rewrite rule plus optional tracker event metadata."""

    pattern: re.Pattern[str]
    kind: _TerminalKind | None
    template: str
    tracker_event_factory: (
        Callable[[re.Match[str], logging.LogRecord], _TrackerEvent | None] | None
    ) = None
    live_managed: bool = False


@dataclass(frozen=True)
class _PriorCheckpointHints:
    """Best-effort checkpoint-name hints from a prior matching run."""

    matched_prior_run: bool
    checkpoint_names: list[str]


@dataclass
class CheckpointState:
    """Mutable execution state tracked for one checkpoint name."""

    name: str
    status: _CheckpointStatus
    started_at: float | None = None
    finished_at: float | None = None
    error: str | None = None
    traceback_frames: list[str] | None = None
    submit_group: int | None = None


@dataclass(frozen=True)
class _TreeRailCheckpointRow:
    """Render-ready checkpoint row for the tree snapshot."""

    name: str
    status: _CheckpointStatus
    duration_text: str | None
    status_text: str
    detail_lines: tuple[str, ...] = ()
    submit_group: int | None = None


@dataclass(frozen=True)
class _TreeRailCompactionRow:
    """Summary row for hidden older successful/cached checkpoints."""

    hidden_count: int
    passed_count: int
    cached_count: int


@dataclass(frozen=True)
class _TreeRailConcurrentGroup:
    """Contiguous concurrent checkpoint block."""

    group_id: int
    children: tuple[_TreeRailCheckpointRow, ...]


@dataclass(frozen=True)
class _TreeRailSnapshot:
    """Full render-ready snapshot for the live tree."""

    flow_name: str | None
    stack_name: str | None
    execution_id: str | None
    execution_url: str | None
    rows: tuple[
        _TreeRailCheckpointRow | _TreeRailCompactionRow | _TreeRailConcurrentGroup, ...
    ]
    wait_lines: tuple[str, ...]
    summary_lines: tuple[str, ...]
    hint_lines: tuple[str, ...]
    terminal_status: _SnapshotTerminalStatus


@dataclass
class CheckpointTracker:
    """Best-effort in-memory tracker for flow/checkpoint lifecycle state."""

    flow_name: str | None = None
    execution_id: str | None = None
    execution_url: str | None = None
    stack_name: str | None = None
    flow_started_at: float | None = None
    flow_finished_at: float | None = None
    matched_prior_run: bool | None = None
    terminal_status: _TerminalStatus | None = None
    terminal_checkpoint_name: str | None = None
    checkpoints: dict[str, CheckpointState] = field(default_factory=dict)
    active_wait_condition: str | None = None
    active_wait_type: str | None = None
    active_wait_timeout_seconds: str | None = None
    active_wait_poll_seconds: str | None = None
    _pending_execution_id: str | None = None
    _pending_stack_name: str | None = None
    _pending_submit_groups: dict[str, deque[int]] = field(default_factory=dict)
    _next_submit_group_id: int = 1
    _lock: RLock = field(default_factory=RLock, repr=False, compare=False)

    def register_execution(self, exec_id: str) -> None:
        """Register an execution ID for the current or next flow session."""
        with self._lock:
            if self.flow_name is not None:
                self.execution_id = exec_id
                self._pending_execution_id = None
            else:
                self._pending_execution_id = exec_id

    def register_submission(self, checkpoint_name: str, *, count: int = 1) -> int:
        """Register a pending checkpoint submission group."""
        normalized_name = normalize_checkpoint_name(checkpoint_name)
        if count < 1:
            return 0

        with self._lock:
            group_id = self._next_submit_group_id
            self._next_submit_group_id += 1
            queue = self._pending_submit_groups.setdefault(normalized_name, deque())
            queue.extend(group_id for _ in range(count))
            return group_id

    def ingest(self, *, event: _TrackerEvent | None, record: logging.LogRecord) -> None:
        """Ingest a classified tracker event."""
        if event is None:
            return

        with self._lock:
            if event.kind == "flow_started":
                self._reset_for_flow(
                    flow_name=event.flow_name,
                    started_at=record.created,
                )
                checkpoint_names: list[str] = []
                if event.flow_name:
                    try:
                        hints = self._lookup_prior_checkpoint_hints(event.flow_name)
                        self.matched_prior_run = hints.matched_prior_run
                        checkpoint_names = hints.checkpoint_names
                    except Exception:
                        self.matched_prior_run = None
                        checkpoint_names = []
                for checkpoint_name in checkpoint_names:
                    self.checkpoints.setdefault(
                        checkpoint_name,
                        CheckpointState(name=checkpoint_name, status="pending"),
                    )
                return

            if event.kind == "flow_completed":
                self.flow_finished_at = record.created
                self.terminal_status = "completed"
                self.terminal_checkpoint_name = None
                self._clear_wait_state()
                return

            if event.kind == "stack_selected":
                if self.flow_name is None:
                    self._pending_stack_name = event.stack_name
                else:
                    self.stack_name = event.stack_name
                return

            if event.kind == "execution_url":
                self.execution_url = event.execution_url
                execution_id = _execution_id_from_url(event.execution_url)
                if execution_id:
                    if self.flow_name is None:
                        self._pending_execution_id = execution_id
                    else:
                        self.execution_id = execution_id
                return

            if event.kind in {
                "execution_resumed",
                "execution_continued",
                "execution_already_finished",
            }:
                self.execution_id = event.execution_id
                if event.kind != "execution_already_finished":
                    self._clear_wait_state()
                return

            if event.kind == "wait_condition":
                self.active_wait_condition = event.wait_name
                self.active_wait_type = event.wait_type
                self.active_wait_timeout_seconds = event.timeout_seconds
                self.active_wait_poll_seconds = event.poll_seconds
                return

            checkpoint_name = event.checkpoint_name
            if checkpoint_name is None:
                return

            state = self._get_or_create_checkpoint(checkpoint_name)

            if event.kind == "checkpoint_launched":
                submit_group = self._consume_submit_group(checkpoint_name)
                if submit_group is not None:
                    state.submit_group = submit_group
                return

            if event.kind == "checkpoint_started":
                if state.status == "failed":
                    state.finished_at = None
                    state.error = None
                    state.traceback_frames = None
                    if self.terminal_status == "failed":
                        self.terminal_status = None
                        self.terminal_checkpoint_name = None
                        self.flow_finished_at = None
                state.status = "running"
                if state.started_at is None:
                    state.started_at = record.created
                self._clear_wait_state()
                return

            if event.kind == "checkpoint_finished":
                state.status = "passed"
                state.finished_at = record.created
                state.error = None
                state.traceback_frames = None
                self._clear_wait_state()
                return

            if event.kind == "checkpoint_cached":
                state.status = "cached"
                state.finished_at = record.created
                state.error = None
                state.traceback_frames = None
                self._clear_wait_state()
                return

            if event.kind == "checkpoint_failed":
                error, traceback_frames = _compact_traceback_frames(record.exc_info)
                state.status = "failed"
                state.finished_at = record.created
                state.error = error or normalize_aliases_in_text(record.getMessage())
                state.traceback_frames = traceback_frames
                if event.retry_remaining is None:
                    self.terminal_status = "failed"
                    self.terminal_checkpoint_name = state.name
                    self.flow_finished_at = record.created
                return

            if event.kind == "checkpoint_stopped":
                state.status = "failed"
                state.finished_at = record.created
                state.error = "stopped"
                state.traceback_frames = None
                self.terminal_status = "failed"
                self.terminal_checkpoint_name = state.name
                self.flow_finished_at = record.created
                return

            if event.kind == "checkpoint_skipped":
                state.status = "passed"
                state.finished_at = record.created
                state.error = None
                state.traceback_frames = None

    def _clear_wait_state(self) -> None:
        self.active_wait_condition = None
        self.active_wait_type = None
        self.active_wait_timeout_seconds = None
        self.active_wait_poll_seconds = None

    def _reset_for_flow(self, *, flow_name: str | None, started_at: float) -> None:
        """Reset all tracker state for a newly started flow."""
        self.flow_name = flow_name
        self.execution_id = self._pending_execution_id
        self.execution_url = None
        self.stack_name = self._pending_stack_name
        self.flow_started_at = started_at
        self.flow_finished_at = None
        self.matched_prior_run = None
        self.terminal_status = None
        self.terminal_checkpoint_name = None
        self.checkpoints = {}
        self._clear_wait_state()
        self._pending_execution_id = None
        self._pending_stack_name = None
        self._pending_submit_groups = {}
        self._next_submit_group_id = 1

    def _get_or_create_checkpoint(self, checkpoint_name: str) -> CheckpointState:
        normalized_name = normalize_checkpoint_name(checkpoint_name)
        state = self.checkpoints.get(normalized_name)
        if state is None:
            state = CheckpointState(name=normalized_name, status="pending")
            self.checkpoints[normalized_name] = state
        return state

    def _consume_submit_group(self, checkpoint_name: str) -> int | None:
        normalized_name = normalize_checkpoint_name(checkpoint_name)
        queue = self._pending_submit_groups.get(normalized_name)
        if not queue:
            return None
        group_id = queue.popleft()
        if not queue:
            self._pending_submit_groups.pop(normalized_name, None)
        return group_id

    def _lookup_prior_checkpoint_hints(self, flow_name: str) -> _PriorCheckpointHints:
        """Best-effort lookup of checkpoint names from a recent matching flow."""
        with _suppress_tracker_logs():
            from zenml.client import Client

            client = Client()
            run_page = client.list_pipeline_runs(
                sort_by="desc:created",
                page=1,
                size=20,
                hydrate=True,
            )
            for run in run_page.items:
                candidate_run = run
                candidate_flow_name = _run_flow_name(candidate_run)
                if candidate_flow_name is None:
                    candidate_run = client.get_pipeline_run(
                        run.id,
                        allow_name_prefix_match=False,
                        hydrate=True,
                    )
                    candidate_flow_name = _run_flow_name(candidate_run)
                if candidate_flow_name != flow_name:
                    continue

                checkpoint_names = _checkpoint_names_from_run(candidate_run)
                return _PriorCheckpointHints(
                    matched_prior_run=True,
                    checkpoint_names=checkpoint_names,
                )
        return _PriorCheckpointHints(matched_prior_run=False, checkpoint_names=[])


# ---------------------------------------------------------------------------
# Tracker helpers
# ---------------------------------------------------------------------------


def _run_flow_name(run: Any) -> str | None:
    pipeline = getattr(run, "pipeline", None)
    pipeline_name = getattr(pipeline, "name", None)
    return normalize_flow_name(pipeline_name)


def _checkpoint_names_from_run(run: Any) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()
    steps = getattr(run, "steps", {})
    for step_name in steps:
        normalized = normalize_checkpoint_name(str(step_name))
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        names.append(normalized)
    return names


def _execution_id_from_url(url: str | None) -> str | None:
    """Best-effort extraction of an execution ID from a dashboard URL."""
    if not url:
        return None
    try:
        path_parts = [part for part in urlparse(url).path.split("/") if part]
    except Exception:
        return None
    if not path_parts:
        return None
    return path_parts[-1]


def _compact_traceback_frames(
    exc_info: _ExcInfo,
) -> tuple[str | None, list[str] | None]:
    """Build a compact traceback focused on user frames."""
    if exc_info is None or exc_info[0] is None or exc_info[1] is None:
        return None, None

    exc_type, exc, tb = exc_info
    error = f"{exc_type.__name__}: {exc}" if exc else exc_type.__name__
    if tb is None:
        return error, None

    try:
        extracted = traceback.extract_tb(tb)
    except Exception:
        return error, None

    preferred = [
        frame
        for frame in extracted
        if "/site-packages/" not in frame.filename
        and "/zenml/" not in frame.filename
        and "/src/kitaru/" not in frame.filename
    ]
    selected = preferred[-5:] if preferred else extracted[-5:]
    if not selected:
        return error, None

    formatted_frames: list[str] = []
    for frame in selected:
        line = f'File "{frame.filename}", line {frame.lineno}, in {frame.name}'
        if frame.line:
            line = f"{line}\n  {frame.line.strip()}"
        formatted_frames.append(line)

    return error, formatted_frames or None


def _format_duration(seconds: float | None) -> str | None:
    """Format a duration for human-facing terminal output."""
    if seconds is None:
        return None
    if seconds >= 10:
        return f"{seconds:.1f}s"
    if seconds >= 1:
        return f"{seconds:.2f}s"
    return f"{seconds:.3f}s"


def _checkpoint_duration_text(state: CheckpointState, *, now: float) -> str | None:
    if state.status == "running" and state.started_at is not None:
        return _format_duration(max(now - state.started_at, 0.0))
    if state.started_at is not None and state.finished_at is not None:
        return _format_duration(max(state.finished_at - state.started_at, 0.0))
    return None


def _checkpoint_status_text(state: CheckpointState) -> str:
    if state.status == "running":
        return "running…"
    if state.status == "passed":
        return "✓"
    if state.status == "failed":
        return "✗"
    if state.status == "cached":
        return "cached ⟳"
    if state.status == "waiting":
        return "waiting for input…"
    return "pending"


def _snapshot_terminal_status(tracker: CheckpointTracker) -> _SnapshotTerminalStatus:
    if tracker.terminal_status == "completed":
        return "completed"
    if tracker.terminal_status == "failed":
        return "failed"
    if tracker.active_wait_condition is not None:
        return "waiting"
    return "running"


def _checkpoint_row_from_state(
    state: CheckpointState,
    *,
    now: float,
) -> _TreeRailCheckpointRow:
    return _TreeRailCheckpointRow(
        name=state.name,
        status=state.status,
        duration_text=_checkpoint_duration_text(state, now=now),
        status_text=_checkpoint_status_text(state),
        detail_lines=tuple(state.traceback_frames or ()),
        submit_group=state.submit_group,
    )


def _compact_rows(
    rows: list[_TreeRailCheckpointRow],
) -> list[_TreeRailCheckpointRow | _TreeRailCompactionRow]:
    completed_positions = [
        index for index, row in enumerate(rows) if row.status in {"passed", "cached"}
    ]
    if len(completed_positions) <= 10:
        return list(rows)

    keep_positions = set(completed_positions[-3:])
    completed_set = set(completed_positions)
    hidden_rows = [
        rows[index] for index in completed_positions if index not in keep_positions
    ]
    compaction = _TreeRailCompactionRow(
        hidden_count=len(hidden_rows),
        passed_count=sum(1 for row in hidden_rows if row.status == "passed"),
        cached_count=sum(1 for row in hidden_rows if row.status == "cached"),
    )

    compacted: list[_TreeRailCheckpointRow | _TreeRailCompactionRow] = []
    inserted_summary = False
    first_hidden_position = min(
        index for index in completed_positions if index not in keep_positions
    )
    for index, row in enumerate(rows):
        if index not in keep_positions and index in completed_set:
            if not inserted_summary and index == first_hidden_position:
                compacted.append(compaction)
                inserted_summary = True
            continue
        compacted.append(row)
    return compacted


def _group_concurrent_rows(
    rows: list[_TreeRailCheckpointRow | _TreeRailCompactionRow],
) -> list[_TreeRailCheckpointRow | _TreeRailCompactionRow | _TreeRailConcurrentGroup]:
    grouped: list[
        _TreeRailCheckpointRow | _TreeRailCompactionRow | _TreeRailConcurrentGroup
    ] = []
    index = 0
    while index < len(rows):
        row = rows[index]
        if not isinstance(row, _TreeRailCheckpointRow) or row.submit_group is None:
            grouped.append(row)
            index += 1
            continue

        contiguous_children = [row]
        next_index = index + 1
        while next_index < len(rows):
            next_row = rows[next_index]
            if not isinstance(next_row, _TreeRailCheckpointRow):
                break
            if next_row.submit_group != row.submit_group:
                break
            contiguous_children.append(next_row)
            next_index += 1

        if len(contiguous_children) == 1:
            grouped.append(row)
        else:
            grouped.append(
                _TreeRailConcurrentGroup(
                    group_id=row.submit_group,
                    children=tuple(contiguous_children),
                )
            )
        index = next_index
    return grouped


def _build_tally_line(total: int, passed: int, cached: int, failed: int) -> str | None:
    """Build a checkpoint tally summary like '3 checkpoints  ✓ 2 passed  ⟳ 1 cached'."""
    bits = [f"{total} checkpoints"] if total else []
    if passed:
        bits.append(f"✓ {passed} passed")
    if cached:
        bits.append(f"⟳ {cached} cached")
    if failed:
        bits.append(f"✗ {failed} failed")
    return "  ".join(bits) if bits else None


def _build_tree_rail_snapshot(
    tracker: CheckpointTracker,
    *,
    now: float,
) -> _TreeRailSnapshot:
    """Build a pure render snapshot from tracker state."""

    terminal_status = _snapshot_terminal_status(tracker)
    checkpoint_states = list(tracker.checkpoints.values())

    if tracker.terminal_status is not None:
        checkpoint_states = [
            state
            for state in checkpoint_states
            if not (
                state.status == "pending"
                and state.started_at is None
                and state.finished_at is None
            )
        ]

    checkpoint_rows = [
        _checkpoint_row_from_state(state, now=now) for state in checkpoint_states
    ]
    compacted_rows = _compact_rows(checkpoint_rows)
    grouped_rows = _group_concurrent_rows(compacted_rows)

    wait_lines: list[str] = []
    hint_lines: list[str] = []
    summary_lines: list[str] = []

    if tracker.active_wait_condition is not None:
        wait_lines.append(
            "Waiting on "
            f"{tracker.active_wait_condition} "
            f"(type={tracker.active_wait_type or 'unknown'}, "
            f"timeout={tracker.active_wait_timeout_seconds or '?'}s, "
            f"poll={tracker.active_wait_poll_seconds or '?'}s)"
        )
        if tracker.execution_id:
            hint_lines.append(f"kitaru executions input {tracker.execution_id}")

    executed_states = [
        state
        for state in tracker.checkpoints.values()
        if state.status in {"passed", "failed", "cached", "running", "waiting"}
    ]
    passed_count = sum(1 for state in executed_states if state.status == "passed")
    cached_count = sum(1 for state in executed_states if state.status == "cached")
    failed_count = sum(1 for state in executed_states if state.status == "failed")
    running_count = sum(1 for state in executed_states if state.status == "running")
    total_count = len(executed_states) - running_count

    duration_text = None
    if tracker.flow_started_at is not None:
        end_time = (
            tracker.flow_finished_at if tracker.flow_finished_at is not None else now
        )
        duration_text = _format_duration(max(end_time - tracker.flow_started_at, 0.0))

    if tracker.terminal_status == "completed":
        if duration_text:
            summary_lines.append(f"Flow completed in {duration_text}")
        else:
            summary_lines.append("Flow completed")
        tally = _build_tally_line(total_count, passed_count, cached_count, failed_count)
        if tally:
            summary_lines.append(tally)
        if tracker.matched_prior_run is False and tracker.execution_id:
            hint_lines.append(f"kitaru executions logs {tracker.execution_id}")
    elif tracker.terminal_status == "failed":
        if duration_text and tracker.terminal_checkpoint_name:
            summary_lines.append(
                "Flow failed at "
                f"{tracker.terminal_checkpoint_name} after {duration_text}"
            )
        elif duration_text:
            summary_lines.append(f"Flow failed after {duration_text}")
        else:
            summary_lines.append("Flow failed")
        tally = _build_tally_line(total_count, passed_count, cached_count, failed_count)
        if tally:
            summary_lines.append(tally)
        if tracker.execution_id:
            hint_lines.extend(
                [
                    f"kitaru executions retry {tracker.execution_id}",
                    f"kitaru executions logs {tracker.execution_id} --traceback",
                ]
            )

    return _TreeRailSnapshot(
        flow_name=tracker.flow_name,
        stack_name=tracker.stack_name,
        execution_id=tracker.execution_id,
        execution_url=tracker.execution_url,
        rows=tuple(grouped_rows),
        wait_lines=tuple(wait_lines),
        summary_lines=tuple(summary_lines),
        hint_lines=tuple(hint_lines),
        terminal_status=terminal_status,
    )


_TRACKER_REENTRANCY = local()
_ACTIVE_TRACKER: CheckpointTracker | None = None
_ACTIVE_TERMINAL_HANDLER: _KitaruTerminalHandler | None = None


def _is_kitaru_terminal_handler_instance(handler: logging.Handler) -> bool:
    """Return whether a handler is a Kitaru terminal handler across reloads.

    ``importlib.reload(kitaru)`` creates a new ``_KitaruTerminalHandler`` class
    object. Existing root handlers from the pre-reload module are still valid,
    but ``isinstance(old_handler, _KitaruTerminalHandler)`` will then return
    ``False``. Detect those legacy instances by a stable marker and a
    conservative module/class-name fallback so we don't install a second Kitaru
    handler with a fresh tracker mid-run.
    """
    if isinstance(handler, _KitaruTerminalHandler):
        return True
    if bool(getattr(handler, "_kitaru_terminal_handler", False)):
        return True
    return (
        handler.__class__.__name__ == "_KitaruTerminalHandler"
        and handler.__class__.__module__ == __name__
    )


def _running_in_zenml_entrypoint_subprocess() -> bool:
    """Return whether the current process is a ZenML entrypoint subprocess."""
    argv = tuple(str(arg) for arg in sys.argv)
    return "--entrypoint_config_source" in argv or any(
        arg.startswith("--entrypoint_config_source=") for arg in argv
    )


def _tracker_logs_suppressed() -> bool:
    """Return whether tracker lookup logs should be ignored on this thread."""
    return bool(getattr(_TRACKER_REENTRANCY, "suppressed", False))


@contextmanager
def _suppress_tracker_logs() -> Iterator[None]:
    """Suppress recursive terminal handling during tracker lookups."""
    previous = _tracker_logs_suppressed()
    _TRACKER_REENTRANCY.suppressed = True
    try:
        yield
    finally:
        _TRACKER_REENTRANCY.suppressed = previous


def register_checkpoint_submission(checkpoint_name: str, *, count: int = 1) -> None:
    """Register a pending checkpoint submission on the active tracker."""
    tracker = _ACTIVE_TRACKER
    if tracker is None:
        return
    try:
        tracker.register_submission(checkpoint_name, count=count)
    except Exception:
        return


def register_flow_execution(exec_id: str) -> None:
    """Register a flow execution ID so the live header can resolve in-place."""
    tracker = _ACTIVE_TRACKER
    handler = _ACTIVE_TERMINAL_HANDLER
    if tracker is None:
        return
    try:
        tracker.register_execution(exec_id)
        if handler is not None:
            if tracker.terminal_status in {"completed", "failed"}:
                handler._finalize_live_session_if_terminal()
            else:
                handler.refresh_live_session()
    except Exception:
        return


# ---------------------------------------------------------------------------
# Rewrite / drop rules
# ---------------------------------------------------------------------------


def _normalized_group(match: re.Match[str], index: int) -> str:
    return normalize_aliases_in_text(str(match.group(index)))


def _checkpoint_event(
    kind: _TrackerEventKind,
    *,
    name_group: int = 1,
    retry_group: int | None = None,
) -> Callable[[re.Match[str], logging.LogRecord], _TrackerEvent]:
    def _factory(match: re.Match[str], record: logging.LogRecord) -> _TrackerEvent:
        retry_remaining: int | None = None
        if retry_group is not None:
            retry_remaining = int(match.group(retry_group))
        return _TrackerEvent(
            kind=kind,
            checkpoint_name=normalize_checkpoint_name(
                _normalized_group(match, name_group)
            ),
            retry_remaining=retry_remaining,
        )

    return _factory


def _flow_started_event(
    match: re.Match[str], record: logging.LogRecord
) -> _TrackerEvent:
    return _TrackerEvent(
        kind="flow_started",
        flow_name=normalize_flow_name(_normalized_group(match, 1)),
    )


def _execution_event(
    kind: _TrackerEventKind,
) -> Callable[[re.Match[str], logging.LogRecord], _TrackerEvent]:
    def _factory(match: re.Match[str], record: logging.LogRecord) -> _TrackerEvent:
        return _TrackerEvent(kind=kind, execution_id=_normalized_group(match, 1))

    return _factory


def _stack_selected_event(
    match: re.Match[str], record: logging.LogRecord
) -> _TrackerEvent:
    return _TrackerEvent(kind="stack_selected", stack_name=_normalized_group(match, 1))


def _execution_url_event(
    match: re.Match[str], record: logging.LogRecord
) -> _TrackerEvent:
    return _TrackerEvent(
        kind="execution_url", execution_url=_normalized_group(match, 1)
    )


def _wait_condition_event(
    match: re.Match[str], record: logging.LogRecord
) -> _TrackerEvent:
    return _TrackerEvent(
        kind="wait_condition",
        wait_name=_normalized_group(match, 1),
        wait_type=_normalized_group(match, 2),
        timeout_seconds=_normalized_group(match, 3),
        poll_seconds=_normalized_group(match, 4),
    )


_REWRITE_RULES: list[_RewriteRule] = [
    # Step lifecycle → Checkpoint lifecycle
    _RewriteRule(
        re.compile(r"^Step `(.+?)` has started\.$"),
        "info",
        "Checkpoint `{0}` started.",
        tracker_event_factory=_checkpoint_event("checkpoint_started"),
        live_managed=True,
    ),
    _RewriteRule(
        re.compile(r"^Step `(.+?)` has finished in `(.+?)`\.$"),
        "success",
        "Checkpoint `{0}` finished in {1}.",
        tracker_event_factory=_checkpoint_event("checkpoint_finished"),
        live_managed=True,
    ),
    _RewriteRule(
        re.compile(r"^Step `(.+?)` finished successfully in (.+)\.$"),
        "success",
        "Checkpoint `{0}` finished in {1}.",
        tracker_event_factory=_checkpoint_event("checkpoint_finished"),
        live_managed=True,
    ),
    _RewriteRule(
        re.compile(r"^Step `(.+?)` finished successfully\.$"),
        "success",
        "Checkpoint `{0}` finished.",
        tracker_event_factory=_checkpoint_event("checkpoint_finished"),
        live_managed=True,
    ),
    _RewriteRule(
        re.compile(r"^Step `(.+?)` failed after (.+)\.$"),
        "error",
        "Checkpoint `{0}` failed after {1}.",
        tracker_event_factory=_checkpoint_event("checkpoint_failed"),
        live_managed=True,
    ),
    _RewriteRule(
        re.compile(r"^Step `(.+?)` failed\.$"),
        "error",
        "Checkpoint `{0}` failed.",
        tracker_event_factory=_checkpoint_event("checkpoint_failed"),
        live_managed=True,
    ),
    _RewriteRule(
        re.compile(r"^Step `(.+?)` failed(?: after .+)?\. Remaining retries: (\d+)\.$"),
        "warning",
        "Checkpoint `{0}` failed. Retries remaining: {1}.",
        tracker_event_factory=_checkpoint_event(
            "checkpoint_failed",
            retry_group=2,
        ),
        live_managed=True,
    ),
    _RewriteRule(
        re.compile(r"^Step `(.+?)` stopped(?:\.|.after .+\.)$"),
        "warning",
        "Checkpoint `{0}` stopped.",
        tracker_event_factory=_checkpoint_event("checkpoint_stopped"),
        live_managed=True,
    ),
    _RewriteRule(
        re.compile(r"^Step `(.+?)` launched\.$"),
        "info",
        "Checkpoint `{0}` launched.",
        tracker_event_factory=_checkpoint_event("checkpoint_launched"),
        live_managed=True,
    ),
    _RewriteRule(
        re.compile(r"^Using cached version of step `(.+?)`\.$"),
        "detail",
        "Checkpoint `{0}` cached.",
        tracker_event_factory=_checkpoint_event("checkpoint_cached"),
        live_managed=True,
    ),
    _RewriteRule(
        re.compile(r"^Skipping step `(.+?)`\.$"),
        "info",
        "Skipping checkpoint `{0}`.",
        tracker_event_factory=_checkpoint_event("checkpoint_skipped"),
        live_managed=True,
    ),
    _RewriteRule(
        re.compile(r"^Failed to run step `(.+?)`: (.+)$"),
        "error",
        "Checkpoint `{0}` failed: {1}",
        tracker_event_factory=_checkpoint_event("checkpoint_failed"),
        live_managed=True,
    ),
    # Pipeline lifecycle → Flow lifecycle
    _RewriteRule(
        re.compile(r"^Initiating a new run for the pipeline: `(.+?)`\.$"),
        "info",
        "Starting flow `{0}`.",
        tracker_event_factory=_flow_started_event,
        live_managed=True,
    ),
    _RewriteRule(
        re.compile(r"^Pipeline completed successfully\.$"),
        "success",
        "Flow completed.",
        tracker_event_factory=lambda match, record: _TrackerEvent(
            kind="flow_completed"
        ),
        live_managed=True,
    ),
    _RewriteRule(
        re.compile(
            r"^Waiting on wait condition `(.+?)` "
            r"\(type=(.+?), timeout=(.+?)s, poll=(.+?)s\)\.$"
        ),
        "info",
        "Waiting on `{0}` (type={1}, timeout={2}s, poll={3}s).",
        tracker_event_factory=_wait_condition_event,
        live_managed=True,
    ),
    _RewriteRule(
        re.compile(r"^Pausing pipeline run `(.+?)`\.$"),
        "warning",
        "Pausing execution `{0}`.",
    ),
    _RewriteRule(
        re.compile(
            r"^Stopping pipeline run `(.+?)` because a wait condition was aborted\.$"
        ),
        "warning",
        "Execution `{0}` stopped because a wait condition was aborted.",
    ),
    _RewriteRule(
        re.compile(r"^Resuming run `(.+?)`\.$"),
        "info",
        "Resuming execution `{0}`.",
        tracker_event_factory=_execution_event("execution_resumed"),
        live_managed=True,
    ),
    _RewriteRule(
        re.compile(r"^Continuing existing run `(.+?)`\.$"),
        "info",
        "Continuing execution `{0}`.",
        tracker_event_factory=_execution_event("execution_continued"),
        live_managed=True,
    ),
    _RewriteRule(
        re.compile(r"^Run `(.+?)` is already finished\.$"),
        "info",
        "Execution `{0}` already finished.",
        tracker_event_factory=_execution_event("execution_already_finished"),
        live_managed=True,
    ),
    _RewriteRule(
        re.compile(r"^Stopping isolated steps\.$"),
        "warning",
        "Stopping isolated checkpoints.",
    ),
    # Stack/config info
    _RewriteRule(
        re.compile(r"^Using stack: `(.+?)`$"),
        "detail",
        "Stack: {0}",
        tracker_event_factory=_stack_selected_event,
        live_managed=True,
    ),
    _RewriteRule(
        re.compile(r"^Caching is disabled by default for `(.+?)`\.$"),
        "detail",
        "Caching disabled for `{0}`.",
    ),
    # Dashboard URL
    _RewriteRule(
        re.compile(r"^Dashboard URL for Pipeline Run: (.+)$"),
        "detail",
        "Execution URL: {0}",
        tracker_event_factory=_execution_url_event,
        live_managed=True,
    ),
    # Pipeline run completion (local/local-docker orchestrators)
    _RewriteRule(
        re.compile(r"^Pipeline run has finished in `(.+?)`\.$"),
        "success",
        "Execution finished in {0}.",
        live_managed=True,
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
    re.compile(r"^\[ZML\d+\]\("),  # structured ZenML warnings
    re.compile(r"^Uploading external artifact to "),
    re.compile(r"^Finished uploading external artifact "),
]


# ---------------------------------------------------------------------------
# Decision logic
# ---------------------------------------------------------------------------


@functools.lru_cache(maxsize=1)
def _terminal_debug_enabled() -> bool:
    """Return whether terminal-only debug chatter should remain visible.

    This intentionally keys off ``KITARU_DEBUG`` only. ``ZENML_DEBUG`` may be
    enabled for broader backend diagnostics, but we still want the interactive
    Kitaru terminal to suppress noisy provider/library chatter unless the user
    explicitly opts into Kitaru terminal debug output.

    Cached after first call — env vars don't change mid-process.
    Call ``_terminal_debug_enabled.cache_clear()`` in tests.
    """
    raw = os.getenv(KITARU_DEBUG_ENV)
    if raw is None:
        return False
    normalized = raw.strip().lower()
    if not normalized:
        return False
    if normalized in _DEBUG_TRUTHY:
        return True
    if normalized in _DEBUG_FALSY:
        return False
    return False


def _should_drop_terminal_record(record: logging.LogRecord, msg: str) -> bool:
    """Return whether a record should be hidden from the terminal only."""
    if _terminal_debug_enabled():
        return False

    logger_name = record.name.lower()
    if logger_name.startswith("litellm") and record.levelno < logging.WARNING:
        return True
    return any(pattern.search(msg) for pattern in _LITELLM_TERMINAL_DROP_PATTERNS)


def _level_to_kind(levelno: int) -> _TerminalKind:
    if levelno >= logging.ERROR:
        return "error"
    if levelno >= logging.WARNING:
        return "warning"
    return "info"


def _apply_zenml_rules(
    msg: str,
    levelno: int,
    record: logging.LogRecord,
) -> _ResolvedLogRecord:
    """Apply rewrite/drop rules to a ZenML log message."""
    for pattern in _DROP_PATTERNS:
        if pattern.search(msg):
            return _ResolvedLogRecord(decision=None)

    for rule in _REWRITE_RULES:
        match = rule.pattern.match(msg)
        if not match:
            continue

        groups = [normalize_aliases_in_text(group) for group in match.groups()]
        text = rule.template.format(*groups)
        resolved_kind = rule.kind if rule.kind is not None else _level_to_kind(levelno)
        tracker_event = None
        if rule.tracker_event_factory is not None:
            tracker_event = rule.tracker_event_factory(match, record)
        return _ResolvedLogRecord(
            decision=_TerminalDecision(kind=resolved_kind, text=text),
            tracker_event=tracker_event,
            live_managed=rule.live_managed,
        )

    cleaned = normalize_aliases_in_text(msg)
    return _ResolvedLogRecord(
        decision=_TerminalDecision(kind=_level_to_kind(levelno), text=cleaned),
        live_managed=False,
    )


def _classify(record: logging.LogRecord) -> _ResolvedLogRecord:
    """Classify a log record for terminal rendering and tracker ingestion."""
    msg = record.getMessage()

    if _should_drop_terminal_record(record, msg):
        return _ResolvedLogRecord(decision=None)

    if record.name.startswith("zenml."):
        return _apply_zenml_rules(msg, record.levelno, record)

    return _ResolvedLogRecord(
        decision=_TerminalDecision(
            kind=_level_to_kind(record.levelno),
            text=normalize_aliases_in_text(msg),
        ),
        live_managed=False,
    )


def _decide(record: logging.LogRecord) -> _TerminalDecision | None:
    """Decide how to render a log record for the terminal.

    Returns ``None`` to indicate the record should be dropped (not displayed).
    """
    return _classify(record).decision


# ---------------------------------------------------------------------------
# Flat rendering
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


_DEBUG_TRUTHY = {"1", "true", "yes", "on"}
_DEBUG_FALSY = {"0", "false", "no", "off"}
_LITELLM_TERMINAL_DROP_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"^LiteLLM completion\(\) model="),
    re.compile(r"^Wrapper: Completed Call, calling success_handler$"),
)


_TREE_RAIL_COLORS: dict[str, str] = {
    "brand": "#b87a2e",
    "text": "#e8dfd2",
    "dim": "#8a7e6e",
    "success": "#3da67a",
    "warning": "#c47a28",
    "error": "#d44040",
    "link": "#4580c9",
    "info": "#5aada5",
}

_RUNNING_FRAMES = ("○", "◔", "◑", "◕", "●")


def _render(decision: _TerminalDecision, *, interactive: bool) -> str:
    """Render a terminal decision to a display string."""
    if not interactive:
        return f"Kitaru: {decision.text}"

    color = _COLORS.get(decision.kind, _COLORS["reset"])
    marker = _MARKERS.get(decision.kind, "\u203a")
    reset = _COLORS["reset"]
    return f"{color}Kitaru {marker}{reset} {decision.text}"


_EXCEPTION_FORMATTER = logging.Formatter()


def _render_exception_text(record: logging.LogRecord) -> str | None:
    """Render exception/stack details from a log record, if present."""
    sections: list[str] = []

    if record.exc_info:
        sections.append(_EXCEPTION_FORMATTER.formatException(record.exc_info).rstrip())
    if record.stack_info:
        sections.append(str(record.stack_info).rstrip())

    if not sections:
        return None
    return "\n".join(section for section in sections if section)


# ---------------------------------------------------------------------------
# Rich tree rendering
# ---------------------------------------------------------------------------


def _spinner_frame(now: float) -> str:
    index = int(now * 8) % len(_RUNNING_FRAMES)
    return _RUNNING_FRAMES[index]


def _checkpoint_icon(status: _CheckpointStatus, *, now: float) -> tuple[str, str]:
    if status == "running":
        return _spinner_frame(now), _TREE_RAIL_COLORS["warning"]
    if status == "passed":
        return "●", _TREE_RAIL_COLORS["success"]
    if status == "failed":
        return "●", _TREE_RAIL_COLORS["error"]
    if status == "cached":
        return "●", _TREE_RAIL_COLORS["dim"]
    if status == "waiting":
        return "◎", _TREE_RAIL_COLORS["warning"]
    return "○", _TREE_RAIL_COLORS["dim"]


def _append_filler(line: Text, right_text: str, *, width: int) -> None:
    left_width = len(line.plain)
    filler_width = max(width - left_width - len(right_text), 1)
    if filler_width > 1:
        line.append(" " + ("─" * filler_width) + " ", style=_TREE_RAIL_COLORS["dim"])
    else:
        line.append(" ", style=_TREE_RAIL_COLORS["dim"])
    line.append(right_text, style=_TREE_RAIL_COLORS["text"])


def _checkpoint_line(
    prefix: str,
    row: _TreeRailCheckpointRow,
    *,
    now: float,
    width: int,
) -> Text:
    icon, icon_color = _checkpoint_icon(row.status, now=now)
    line = Text(style=_TREE_RAIL_COLORS["text"])
    line.append(prefix, style=_TREE_RAIL_COLORS["dim"])
    line.append(icon, style=icon_color)
    line.append(" ")
    line.append(row.name, style=_TREE_RAIL_COLORS["text"])

    right_bits: list[str] = []
    if row.duration_text is not None:
        right_bits.append(row.duration_text)
    right_bits.append(row.status_text)
    _append_filler(line, " ".join(right_bits), width=width)
    return line


def _compaction_line(prefix: str, row: _TreeRailCompactionRow, *, width: int) -> Text:
    line = Text(style=_TREE_RAIL_COLORS["text"])
    line.append(prefix, style=_TREE_RAIL_COLORS["dim"])
    line.append("… ", style=_TREE_RAIL_COLORS["dim"])
    line.append(
        f"{row.hidden_count} checkpoints hidden", style=_TREE_RAIL_COLORS["dim"]
    )
    right_bits: list[str] = []
    if row.passed_count:
        right_bits.append(f"✓ {row.passed_count} passed")
    if row.cached_count:
        right_bits.append(f"⟳ {row.cached_count} cached")
    _append_filler(line, "  ".join(right_bits) or "older results", width=width)
    return line


def _header_lines(
    snapshot: _TreeRailSnapshot,
    *,
    version: str,
    width: int,
) -> list[Text]:
    lines: list[Text] = []

    header = Text()
    header.append("○ ", style=_TREE_RAIL_COLORS["brand"])
    header.append(f"kitaru v{version}", style=_TREE_RAIL_COLORS["brand"])
    lines.append(header)
    lines.append(Text("│", style=_TREE_RAIL_COLORS["dim"]))

    metadata_rows = [
        ("Flow", snapshot.flow_name or "resolving…"),
        ("Stack", snapshot.stack_name or "resolving…"),
        ("Exec", snapshot.execution_id or "…"),
    ]
    for label, value in metadata_rows:
        line = Text("│  ", style=_TREE_RAIL_COLORS["dim"])
        line.append(label.ljust(8), style=_TREE_RAIL_COLORS["dim"])
        if label == "Exec" and snapshot.execution_url is not None:
            line.append(
                value,
                style=Style(
                    color=_TREE_RAIL_COLORS["link"],
                    link=snapshot.execution_url,
                ),
            )
        else:
            line.append(value, style=_TREE_RAIL_COLORS["text"])
        lines.append(line)

    if (
        snapshot.rows
        or snapshot.wait_lines
        or snapshot.summary_lines
        or snapshot.hint_lines
    ):
        lines.append(Text("│", style=_TREE_RAIL_COLORS["dim"]))

    return lines


def _tree_lines(
    snapshot: _TreeRailSnapshot,
    *,
    version: str,
    now: float,
    width: int,
) -> list[Text]:
    lines = _header_lines(snapshot, version=version, width=width)

    for row in snapshot.rows:
        if isinstance(row, _TreeRailCheckpointRow):
            lines.append(_checkpoint_line("├─ ", row, now=now, width=width))
            for detail_line in row.detail_lines:
                detail = Text("│  ", style=_TREE_RAIL_COLORS["dim"])
                detail.append(detail_line, style=_TREE_RAIL_COLORS["dim"])
                lines.append(detail)
            continue

        if isinstance(row, _TreeRailCompactionRow):
            lines.append(_compaction_line("├─ ", row, width=width))
            continue

        lines.append(
            Text.assemble(
                ("├┬ ", _TREE_RAIL_COLORS["dim"]),
                ("concurrent", _TREE_RAIL_COLORS["dim"]),
            )
        )
        for child in row.children:
            lines.append(_checkpoint_line("│├─ ", child, now=now, width=width))
            for detail_line in child.detail_lines:
                detail = Text("││  ", style=_TREE_RAIL_COLORS["dim"])
                detail.append(detail_line, style=_TREE_RAIL_COLORS["dim"])
                lines.append(detail)
        lines.append(Text("├┘", style=_TREE_RAIL_COLORS["dim"]))

    for wait_line in snapshot.wait_lines:
        line = Text("│  ", style=_TREE_RAIL_COLORS["dim"])
        line.append(wait_line, style=_TREE_RAIL_COLORS["warning"])
        lines.append(line)

    for summary_line in snapshot.summary_lines:
        line = Text("│  ", style=_TREE_RAIL_COLORS["dim"])
        summary_style = (
            _TREE_RAIL_COLORS["success"]
            if snapshot.terminal_status == "completed"
            else _TREE_RAIL_COLORS["error"]
            if snapshot.terminal_status == "failed"
            else _TREE_RAIL_COLORS["text"]
        )
        line.append(summary_line, style=summary_style)
        lines.append(line)

    for hint_line in snapshot.hint_lines:
        line = Text("│  ", style=_TREE_RAIL_COLORS["dim"])
        line.append("Hint: ", style=_TREE_RAIL_COLORS["dim"])
        line.append(hint_line, style=_TREE_RAIL_COLORS["link"])
        lines.append(line)

    lines.append(Text("╵", style=_TREE_RAIL_COLORS["dim"]))
    return lines


class _TerminalStreamProxy(TextIOBase):
    """Small file-like adapter so Rich writes through the bypassed stream."""

    def __init__(self, write: Callable[[str], Any], stream: Any) -> None:
        self._write = write
        self._stream = stream

    def write(self, text: str) -> int:
        self._write(text)
        return len(text)

    def flush(self) -> None:
        flush = getattr(self._stream, "flush", None)
        if callable(flush):
            flush()

    def isatty(self) -> bool:
        isatty = getattr(self._stream, "isatty", None)
        if callable(isatty):
            return bool(isatty())
        return False

    @property
    def encoding(self) -> str:
        return getattr(self._stream, "encoding", "utf-8")

    def fileno(self) -> int:
        fileno = getattr(self._stream, "fileno", None)
        if callable(fileno):
            return fileno()
        raise OSError("terminal stream has no file descriptor")


class _FlowTreeRenderable:
    """Rich renderable that converts tracker state into a tree-rail view."""

    def __init__(self, tracker: CheckpointTracker, *, version: str) -> None:
        self._tracker = tracker
        self._version = version

    def __rich_console__(
        self,
        console: Console,
        options: ConsoleOptions,
    ) -> RenderResult:
        now = time.time()
        snapshot = _build_tree_rail_snapshot(self._tracker, now=now)
        width = max(options.max_width - 2, 20)
        yield Group(*_tree_lines(snapshot, version=self._version, now=now, width=width))


class _FlowLiveSession:
    """Own the Rich Live lifecycle for one interactive flow tree."""

    def __init__(
        self,
        tracker: CheckpointTracker,
        *,
        version: str,
        write: Callable[[str], Any],
        stream: Any,
    ) -> None:
        self._tracker = tracker
        self._version = version
        self._write = write
        self._stream = stream
        self._console: Console | None = None
        self._live: Live | None = None
        self._renderable: _FlowTreeRenderable | None = None
        self._started = False
        self._closed = False
        self._lock = RLock()

    def ensure_started(self) -> None:
        with self._lock:
            if self._closed or self._started:
                return
            proxy = _TerminalStreamProxy(self._write, self._stream)
            self._renderable = _FlowTreeRenderable(self._tracker, version=self._version)
            self._console = Console(
                file=cast(IO[str], proxy),
                force_terminal=True,
                markup=False,
                highlight=False,
                soft_wrap=False,
            )
            self._live = Live(
                self._renderable,
                console=self._console,
                # Keep the live tree transient while it animates so temporary
                # redraw frames don't linger if we ever have to tear the
                # session down early. Terminal finalization flips this off so
                # Rich leaves the last rendered tree in place exactly once.
                transient=True,
                # Keep the interactive tree event-driven. Rich's background
                # auto-refresh loop leaks repeated intermediate frames into
                # scrollback/captured terminal output, especially for long-
                # running checkpoints that don't emit new lifecycle logs.
                auto_refresh=False,
                redirect_stdout=False,
                redirect_stderr=False,
            )
            self._live.start()
            self._started = True

    def refresh(self) -> None:
        with self._lock:
            if not self._started or self._closed or self._live is None:
                return
            self._live.refresh()

    def print_framework_line(self, text: str) -> None:
        self.ensure_started()
        with self._lock:
            if self._console is None or self._closed:
                return
            self._console.print(text, markup=False, highlight=False)

    def print_user_line(self, text: str, *, exception_text: str | None = None) -> None:
        self.ensure_started()
        with self._lock:
            if self._console is None or self._closed:
                return
            self._console.print(text, markup=False, highlight=False)
            if exception_text:
                self._console.print(exception_text, markup=False, highlight=False)

    def stop(self) -> None:
        with self._lock:
            if self._closed:
                return
            try:
                if self._live is not None:
                    self._live.stop()
            finally:
                self._closed = True

    def finalize(self) -> None:
        with self._lock:
            if self._closed:
                return
            try:
                if self._live is not None:
                    # Tell Rich to keep the last in-place frame instead of
                    # restoring the cursor and then forcing us to print a
                    # second static copy of the tree.
                    self._live.transient = False
                    self._live.stop()
            finally:
                self._closed = True


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
    """Intercepts log records, rewrites ZenML messages, writes to terminal."""

    def __init__(self, *, tracker: CheckpointTracker | None = None) -> None:
        super().__init__()
        self._write = _get_bypass_write()
        self._stream = sys.stdout
        self._interactive = hasattr(sys.stdout, "isatty") and sys.stdout.isatty()
        from kitaru.config import resolve_machine_mode

        self._machine_mode = resolve_machine_mode(interactive=self._interactive)
        self._tracker = tracker or CheckpointTracker()
        self._version = resolve_installed_version()
        self._live_session: _FlowLiveSession | None = None
        self._kitaru_terminal_handler = True

        global _ACTIVE_TRACKER, _ACTIVE_TERMINAL_HANDLER
        _ACTIVE_TRACKER = self._tracker
        _ACTIVE_TERMINAL_HANDLER = self

    def _should_use_live(
        self,
        resolved: _ResolvedLogRecord,
    ) -> bool:
        if not self._interactive or self._machine_mode:
            return False
        return resolved.live_managed or self._live_session is not None

    def _ensure_live_session(self) -> _FlowLiveSession:
        if self._live_session is None:
            self._live_session = _FlowLiveSession(
                self._tracker,
                version=self._version,
                write=self._write,
                stream=self._stream,
            )
        self._live_session.ensure_started()
        return self._live_session

    def refresh_live_session(self) -> None:
        if self._live_session is None:
            return
        try:
            self._live_session.refresh()
        except Exception:
            self._disable_live_session()

    def _disable_live_session(self) -> None:
        if self._live_session is None:
            return
        try:
            self._live_session.stop()
        finally:
            self._live_session = None

    def _finalize_live_session_if_terminal(self) -> None:
        if self._live_session is None:
            return
        if self._tracker.terminal_status not in {"completed", "failed"}:
            return
        if self._tracker.execution_id is None and self._tracker.execution_url is None:
            return
        self._live_session.finalize()
        self._live_session = None

    def _finalize_failed_live_session(self) -> None:
        if self._live_session is None:
            return
        if self._tracker.terminal_status is None:
            failed_states = [
                state
                for state in self._tracker.checkpoints.values()
                if state.status == "failed"
            ]
            if failed_states:
                self._tracker.terminal_status = "failed"
                self._tracker.terminal_checkpoint_name = failed_states[-1].name
                if self._tracker.flow_finished_at is None:
                    self._tracker.flow_finished_at = time.time()
        self._live_session.finalize()
        self._live_session = None

    def close(self) -> None:
        global _ACTIVE_TERMINAL_HANDLER, _ACTIVE_TRACKER

        try:
            self._finalize_failed_live_session()
        finally:
            if _ACTIVE_TERMINAL_HANDLER is self:
                _ACTIVE_TERMINAL_HANDLER = None
            if _ACTIVE_TRACKER is self._tracker:
                _ACTIVE_TRACKER = None
            super().close()

    def _emit_flat(
        self, decision: _TerminalDecision, record: logging.LogRecord
    ) -> None:
        """Write a record as plain text, outside any Rich Live session."""
        text = _render(
            decision,
            interactive=self._interactive and not self._machine_mode,
        )
        self._write(text + "\n")
        if self._machine_mode:
            exception_text = _render_exception_text(record)
            if exception_text:
                self._write(exception_text.rstrip() + "\n")

    def emit(self, record: logging.LogRecord) -> None:
        if _tracker_logs_suppressed():
            return

        try:
            resolved = _classify(record)
            if resolved.decision is None:
                return

            if (
                self._live_session is not None
                and resolved.tracker_event is not None
                and resolved.tracker_event.kind == "flow_started"
            ):
                self._disable_live_session()

            with suppress(Exception):
                self._tracker.ingest(event=resolved.tracker_event, record=record)

            if not self._should_use_live(resolved):
                self._emit_flat(resolved.decision, record)
                return

            try:
                if (
                    resolved.live_managed
                    and self._tracker.terminal_status
                    in {
                        "completed",
                        "failed",
                    }
                    and (
                        self._tracker.execution_id is not None
                        or self._tracker.execution_url is not None
                    )
                ):
                    self._finalize_live_session_if_terminal()
                    return

                live_session = self._ensure_live_session()
                if resolved.live_managed:
                    live_session.refresh()
                    return

                exception_text = _render_exception_text(record)
                if record.name.startswith("zenml."):
                    live_session.print_framework_line(
                        f"Kitaru: {resolved.decision.text}"
                    )
                else:
                    live_session.print_user_line(
                        resolved.decision.text,
                        exception_text=exception_text,
                    )
            except Exception:
                self._disable_live_session()
                self._emit_flat(resolved.decision, record)
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
    global _ACTIVE_TRACKER, _ACTIVE_TERMINAL_HANDLER

    from zenml.logger import ConsoleFormatter, ZenMLLoggingHandler

    root = logging.getLogger()

    existing_kitaru: logging.Handler | None = None
    zenml_console_indices: list[int] = []

    for i, handler in enumerate(root.handlers):
        if _is_kitaru_terminal_handler_instance(handler):
            if existing_kitaru is None:
                existing_kitaru = handler
            else:
                handler.close()
            continue
        if isinstance(handler, ZenMLLoggingHandler):
            continue
        if isinstance(getattr(handler, "formatter", None), ConsoleFormatter):
            zenml_console_indices.append(i)

    if _running_in_zenml_entrypoint_subprocess():
        new_handlers: list[logging.Handler] = []
        for handler in root.handlers:
            if _is_kitaru_terminal_handler_instance(handler):
                handler.close()
                continue
            if isinstance(getattr(handler, "formatter", None), ConsoleFormatter):
                handler.close()
                continue
            new_handlers.append(handler)

        root.handlers = new_handlers

        _ACTIVE_TRACKER = None
        _ACTIVE_TERMINAL_HANDLER = None
        return

    kitaru_handler = cast(
        _KitaruTerminalHandler,
        existing_kitaru or _KitaruTerminalHandler(),
    )

    _ACTIVE_TRACKER = kitaru_handler._tracker
    _ACTIVE_TERMINAL_HANDLER = kitaru_handler

    if zenml_console_indices:
        first_idx = zenml_console_indices[0]
        new_handlers: list[logging.Handler] = []
        for i, handler in enumerate(root.handlers):
            if (
                _is_kitaru_terminal_handler_instance(handler)
                and handler is not kitaru_handler
            ):
                handler.close()
                continue
            if i == first_idx:
                if existing_kitaru is None:
                    new_handlers.append(kitaru_handler)
                    handler.close()
                else:
                    handler.close()
                continue
            if i in zenml_console_indices[1:]:
                handler.close()
                continue
            new_handlers.append(handler)

        if kitaru_handler not in new_handlers:
            new_handlers.insert(first_idx, kitaru_handler)

        root.handlers = new_handlers
    elif existing_kitaru is None:
        root.addHandler(kitaru_handler)
