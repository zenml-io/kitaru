"""Tests for the kitaru terminal live renderer."""

from __future__ import annotations

import logging
import threading
from datetime import UTC, datetime
from io import StringIO
from unittest.mock import MagicMock, patch

from rich.console import Console

from kitaru.client import CheckpointCall, Execution, ExecutionStatus, PendingWait
from kitaru.runtime import _SUBMISSION_OBSERVER, _submission_observer
from kitaru.terminal import (
    LiveExecutionRenderer,
    _build_checkpoint_table,
    _build_failure_banner,
    _build_header,
    _build_wait_banner,
    _format_duration,
    _render_execution,
    _render_submitting,
    _status_text,
    _suppress_zenml_console,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_execution(
    *,
    exec_id: str = "exec-123",
    flow_name: str | None = "my_flow",
    status: ExecutionStatus = ExecutionStatus.COMPLETED,
    checkpoints: list[CheckpointCall] | None = None,
    pending_wait: PendingWait | None = None,
    failure: MagicMock | None = None,
    started_at: datetime | None = None,
    ended_at: datetime | None = None,
) -> Execution:
    return Execution(
        exec_id=exec_id,
        flow_name=flow_name,
        status=status,
        started_at=started_at or datetime(2026, 3, 7, 10, 0, 0, tzinfo=UTC),
        ended_at=ended_at or datetime(2026, 3, 7, 10, 1, 0, tzinfo=UTC),
        stack_name="local",
        metadata={},
        status_reason=None,
        failure=failure,
        pending_wait=pending_wait,
        frozen_execution_spec=None,
        original_exec_id=None,
        checkpoints=checkpoints or [],
        artifacts=[],
        _client=MagicMock(),
    )


def _make_checkpoint(
    *,
    name: str = "summarize",
    status: ExecutionStatus = ExecutionStatus.COMPLETED,
    started_at: datetime | None = None,
    ended_at: datetime | None = None,
) -> CheckpointCall:
    return CheckpointCall(
        call_id=f"call-{name}",
        name=name,
        status=status,
        started_at=started_at or datetime(2026, 3, 7, 10, 0, 5, tzinfo=UTC),
        ended_at=ended_at or datetime(2026, 3, 7, 10, 0, 15, tzinfo=UTC),
        metadata={},
        original_call_id=None,
        parent_call_ids=[],
        failure=None,
        attempts=[],
        artifacts=[],
    )


# ---------------------------------------------------------------------------
# _format_duration
# ---------------------------------------------------------------------------


class TestFormatDuration:
    def test_none_start(self) -> None:
        assert _format_duration(None, None) == "-"

    def test_seconds(self) -> None:
        start = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
        end = datetime(2026, 1, 1, 0, 0, 45, tzinfo=UTC)
        assert _format_duration(start, end) == "45s"

    def test_minutes(self) -> None:
        start = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
        end = datetime(2026, 1, 1, 0, 3, 15, tzinfo=UTC)
        assert _format_duration(start, end) == "3m 15s"

    def test_hours(self) -> None:
        start = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
        end = datetime(2026, 1, 1, 2, 30, 0, tzinfo=UTC)
        assert _format_duration(start, end) == "2h 30m"

    def test_ongoing_uses_now(self) -> None:
        start = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
        result = _format_duration(start, None)
        assert result != "-"


# ---------------------------------------------------------------------------
# _status_text
# ---------------------------------------------------------------------------


class TestStatusText:
    def test_completed_style(self) -> None:
        text = _status_text(ExecutionStatus.COMPLETED)
        assert "completed" in text.plain

    def test_failed_style(self) -> None:
        text = _status_text(ExecutionStatus.FAILED)
        assert "failed" in text.plain

    def test_string_status(self) -> None:
        text = _status_text("running")
        assert "running" in text.plain


# ---------------------------------------------------------------------------
# _build_checkpoint_table
# ---------------------------------------------------------------------------


class TestBuildCheckpointTable:
    def test_empty_checkpoints(self) -> None:
        table = _build_checkpoint_table([])
        assert table.row_count == 0

    def test_multiple_checkpoints_sorted(self) -> None:
        cp1 = _make_checkpoint(
            name="extract",
            started_at=datetime(2026, 3, 7, 10, 0, 20, tzinfo=UTC),
        )
        cp2 = _make_checkpoint(
            name="summarize",
            started_at=datetime(2026, 3, 7, 10, 0, 5, tzinfo=UTC),
        )
        table = _build_checkpoint_table([cp1, cp2])
        assert table.row_count == 2


# ---------------------------------------------------------------------------
# _build_header
# ---------------------------------------------------------------------------


class TestBuildHeader:
    def test_header_contains_target(self) -> None:
        header = _build_header(target="agent.py:flow", exec_id=None, execution=None)
        assert "agent.py:flow" in header.plain

    def test_header_contains_exec_id(self) -> None:
        header = _build_header(
            target="agent.py:flow", exec_id="exec-123", execution=None
        )
        assert "exec-123" in header.plain

    def test_header_contains_flow_name(self) -> None:
        execution = _make_execution(flow_name="my_flow")
        header = _build_header(
            target="agent.py:flow", exec_id="exec-123", execution=execution
        )
        assert "my_flow" in header.plain


# ---------------------------------------------------------------------------
# _build_wait_banner / _build_failure_banner
# ---------------------------------------------------------------------------


class TestBanners:
    def test_wait_banner_none_when_no_wait(self) -> None:
        execution = _make_execution(pending_wait=None)
        assert _build_wait_banner(execution) is None

    def test_wait_banner_shown(self) -> None:
        wait = PendingWait(
            wait_id="w-1",
            name="approval",
            question="Approve?",
            schema=None,
            metadata={},
            entered_waiting_at=None,
        )
        execution = _make_execution(pending_wait=wait)
        banner = _build_wait_banner(execution)
        assert banner is not None
        assert "approval" in banner.plain
        assert "Approve?" in banner.plain

    def test_failure_banner_none_when_no_failure(self) -> None:
        execution = _make_execution(failure=None)
        assert _build_failure_banner(execution) is None

    def test_failure_banner_shown(self) -> None:
        failure = MagicMock()
        failure.message = "Step crashed"
        execution = _make_execution(failure=failure, status=ExecutionStatus.FAILED)
        banner = _build_failure_banner(execution)
        assert banner is not None
        assert "Step crashed" in banner.plain


# ---------------------------------------------------------------------------
# _render_execution / _render_submitting
# ---------------------------------------------------------------------------


class TestRendering:
    def test_render_submitting_returns_panel(self) -> None:
        panel = _render_submitting("agent.py:flow")
        assert panel is not None

    def test_render_execution_without_exec(self) -> None:
        panel = _render_execution(target="agent.py:flow", exec_id=None, execution=None)
        assert panel is not None

    def test_render_execution_with_checkpoints(self) -> None:
        cp = _make_checkpoint(name="extract")
        execution = _make_execution(checkpoints=[cp])
        panel = _render_execution(
            target="agent.py:flow", exec_id="exec-123", execution=execution
        )
        assert panel is not None

    def test_render_execution_to_console(self) -> None:
        """Verify the panel can be printed to a real Console without error."""
        execution = _make_execution(checkpoints=[_make_checkpoint(name="step1")])
        panel = _render_execution(
            target="agent.py:flow", exec_id="exec-123", execution=execution
        )
        console = Console(file=StringIO())
        console.print(panel)


# ---------------------------------------------------------------------------
# _suppress_zenml_console
# ---------------------------------------------------------------------------


class TestSuppressZenmlConsole:
    def test_removes_zenml_console_handler(self) -> None:
        """A handler whose stream type is _ZenMLStdoutStream gets removed."""

        class _ZenMLStdoutStream:
            def write(self, text: str) -> int:
                return len(text)

            def flush(self) -> None:
                pass

        handler = logging.StreamHandler(_ZenMLStdoutStream())
        root_logger = logging.getLogger()
        root_logger.addHandler(handler)

        try:
            with _suppress_zenml_console():
                assert handler not in root_logger.handlers

            # Restored after context exits
            assert handler in root_logger.handlers
        finally:
            root_logger.removeHandler(handler)

    def test_preserves_non_zenml_handlers(self) -> None:
        """Non-ZenML handlers are left in place."""
        handler = logging.StreamHandler(StringIO())
        root_logger = logging.getLogger()
        root_logger.addHandler(handler)

        try:
            with _suppress_zenml_console():
                assert handler in root_logger.handlers
        finally:
            root_logger.removeHandler(handler)


# ---------------------------------------------------------------------------
# _submission_observer
# ---------------------------------------------------------------------------


class TestSubmissionObserver:
    def test_observer_callback_receives_exec_id(self) -> None:
        received: list[str] = []

        with _submission_observer(received.append):
            assert _SUBMISSION_OBSERVER.get() is not None
            observer = _SUBMISSION_OBSERVER.get()
            assert observer is not None
            observer("exec-456")

        assert received == ["exec-456"]
        assert _SUBMISSION_OBSERVER.get() is None

    def test_observer_restored_after_exit(self) -> None:
        assert _SUBMISSION_OBSERVER.get() is None

        with _submission_observer(lambda _: None):
            assert _SUBMISSION_OBSERVER.get() is not None

        assert _SUBMISSION_OBSERVER.get() is None


# ---------------------------------------------------------------------------
# LiveExecutionRenderer
# ---------------------------------------------------------------------------


class TestLiveExecutionRenderer:
    def test_publish_exec_id_sets_id(self) -> None:
        renderer = LiveExecutionRenderer(
            target="agent.py:flow",
            console=Console(file=StringIO()),
        )
        renderer.publish_exec_id("exec-789")
        assert renderer._exec_id == "exec-789"
        assert renderer._exec_id_event.is_set()

    def test_renderer_context_manager_without_polling(self) -> None:
        """Renderer enters and exits cleanly even without a published ID."""
        console = Console(file=StringIO())
        renderer = LiveExecutionRenderer(
            target="agent.py:flow",
            console=console,
            poll_interval=0.05,
        )

        with renderer:
            pass

        assert renderer.result is None

    def test_renderer_polls_and_captures_result(self) -> None:
        """When an exec_id is published, the renderer polls and captures result."""
        execution = _make_execution(exec_id="exec-poll")
        mock_client = MagicMock()
        mock_client.return_value.executions.get.return_value = execution

        console = Console(file=StringIO())
        renderer = LiveExecutionRenderer(
            target="agent.py:flow",
            console=console,
            poll_interval=0.05,
        )

        with patch("kitaru.terminal.KitaruClient", mock_client), renderer:
            renderer.publish_exec_id("exec-poll")
            # Give polling thread time to fetch
            threading.Event().wait(0.2)

        assert renderer.result is not None
        assert renderer.result.exec_id == "exec-poll"

    def test_renderer_tolerates_poll_errors(self) -> None:
        """Transient fetch errors don't crash the renderer."""
        mock_client = MagicMock()
        mock_client.return_value.executions.get.side_effect = Exception("transient")

        console = Console(file=StringIO())
        renderer = LiveExecutionRenderer(
            target="agent.py:flow",
            console=console,
            poll_interval=0.05,
        )

        with patch("kitaru.terminal.KitaruClient", mock_client), renderer:
            renderer.publish_exec_id("exec-err")
            threading.Event().wait(0.2)

        # Should exit cleanly with no result
        assert renderer.result is None
