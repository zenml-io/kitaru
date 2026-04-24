"""Shared execution log follow loop."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from kitaru._client._models import Execution, ExecutionStatus, LogEntry, PendingWait
from kitaru.errors import build_recovery_command


@dataclass(frozen=True)
class FollowResult:
    """Outcome of following execution logs until terminal status."""

    execution: Execution
    exit_code: int


class FollowEventSink(Protocol):
    """Callbacks used by the shared follow loop to emit user-visible events."""

    def emit_logs(self, entries: Sequence[LogEntry]) -> None:
        """Emit new runtime log entries."""

    def emit_waiting(self, wait: PendingWait | None) -> None:
        """Emit a wait-state transition."""

    def emit_terminal(
        self,
        execution: Execution,
        *,
        message: str,
        recovery_command: str | None,
    ) -> None:
        """Emit a terminal execution status."""


def log_entry_dedup_key(entry: LogEntry) -> tuple[Any, ...]:
    """Build a stable key for follow-mode log deduplication."""
    return (
        entry.timestamp,
        entry.level,
        entry.checkpoint_name,
        entry.module,
        entry.filename,
        entry.lineno,
        entry.message,
    )


def _terminal_message(execution: Execution) -> str:
    """Return the stable terminal message used by follow-mode callers."""
    if execution.status == ExecutionStatus.COMPLETED:
        return "Execution completed successfully"
    if execution.status == ExecutionStatus.CANCELLED:
        return "Execution cancelled"

    failure_reason = execution.status_reason or "execution failed"
    if execution.failure is not None:
        failure_reason = execution.failure.message
    return failure_reason


def follow_execution_logs(
    *,
    client: Any,
    exec_id: str,
    checkpoint: str | None,
    source: str,
    limit: int | None,
    interval: float,
    sink: FollowEventSink,
    sleep: Callable[[float], None],
) -> FollowResult:
    """Poll execution logs until terminal status and stream only new entries.

    This loop intentionally owns the polling, deduplication, wait-status, and
    terminal-status behavior shared by CLI and SDK callers. It does not format
    output or catch backend errors; callers decide how to present events and
    whether a log retrieval failure should abort or fall back.
    """
    seen_entries: set[tuple[Any, ...]] = set()
    last_wait_name: str | None = None

    while True:
        entries = client.executions.logs(
            exec_id,
            checkpoint=checkpoint,
            source=source,
            limit=limit,
        )

        new_entries: list[LogEntry] = []
        for entry in entries:
            key = log_entry_dedup_key(entry)
            if key in seen_entries:
                continue
            seen_entries.add(key)
            new_entries.append(entry)

        if new_entries:
            sink.emit_logs(new_entries)

        execution = client.executions.get(exec_id)
        if execution.status == ExecutionStatus.COMPLETED:
            sink.emit_terminal(
                execution,
                message=_terminal_message(execution),
                recovery_command=None,
            )
            return FollowResult(execution=execution, exit_code=0)
        if execution.status == ExecutionStatus.FAILED:
            status_value = ExecutionStatus.FAILED.value
            recovery_command = build_recovery_command(exec_id, status=status_value)
            sink.emit_terminal(
                execution,
                message=_terminal_message(execution),
                recovery_command=recovery_command,
            )
            return FollowResult(execution=execution, exit_code=1)
        if execution.status == ExecutionStatus.CANCELLED:
            sink.emit_terminal(
                execution,
                message=_terminal_message(execution),
                recovery_command=None,
            )
            return FollowResult(execution=execution, exit_code=1)

        if execution.status == ExecutionStatus.WAITING:
            wait_name = "unknown"
            if execution.pending_wait is not None:
                wait_name = execution.pending_wait.name
            if wait_name != last_wait_name:
                sink.emit_waiting(execution.pending_wait)
                last_wait_name = wait_name

        sleep(interval)
