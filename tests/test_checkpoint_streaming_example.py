"""Tests for the checkpoint streaming example watcher."""

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from examples.features.checkpoint_streaming.watch_checkpoint_events import (
    format_event,
    iter_formatted_events,
    wait_for_next_execution,
)

from kitaru import ExecutionEvent


def _event(
    *,
    kind: str,
    payload: dict[str, object],
    timestamp: datetime,
    checkpoint_name: str | None = "draft_brief",
) -> ExecutionEvent:
    return ExecutionEvent(
        exec_id="exec-1",
        kind=kind,
        payload=payload,
        stream_id="stream-1",
        index=0,
        timestamp=timestamp,
        checkpoint_id="checkpoint-1",
        checkpoint_name=checkpoint_name,
    )


def test_format_event_prints_progress_percent_and_message() -> None:
    event = _event(
        kind="kitaru.checkpoint.progress",
        timestamp=datetime(2026, 5, 9, 12, 0, 2, tzinfo=UTC),
        payload={
            "message": "Collecting source notes",
            "data": {"percent": 0.2, "documents": 3},
        },
    )

    line = format_event(event)

    assert line == "12:00:02  draft_brief         20.0%  Collecting source notes"


def test_format_event_uses_public_event_checkpoint_name() -> None:
    event = _event(
        kind="demo.brief.section.ready",
        timestamp=datetime(2026, 5, 9, 12, 0, 8, tzinfo=UTC),
        payload={
            "message": "Recommendation section is ready",
            "data": {"section": "recommendation"},
        },
    )

    line = format_event(event)

    assert line == "12:00:08  draft_brief         Recommendation section is ready"


def test_wait_for_next_execution_polls_until_demo_run_exists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = {"count": 0}

    class FakeExecutions:
        def latest(self, *, flow: str, status: str) -> SimpleNamespace:
            calls["count"] += 1
            assert flow == "streaming_brief"
            assert status == "running"
            if calls["count"] == 1:
                raise LookupError("not yet")
            return SimpleNamespace(exec_id="exec-next")

    class FakeClient:
        executions = FakeExecutions()

    monkeypatch.setattr("kitaru.KitaruClient", FakeClient)
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert wait_for_next_execution(timeout_seconds=5) == "exec-next"
    assert calls["count"] == 2


def test_iter_formatted_events_keeps_lifecycle_order() -> None:
    events = [
        _event(
            kind="kitaru.checkpoint.started",
            timestamp=datetime(2026, 5, 9, 12, 0, 1, tzinfo=UTC),
            payload={"status": "started"},
        ),
        _event(
            kind="kitaru.checkpoint.completed",
            timestamp=datetime(2026, 5, 9, 12, 0, 10, tzinfo=UTC),
            payload={"status": "completed"},
        ),
    ]

    assert list(iter_formatted_events(events)) == [
        "12:00:01  draft_brief        started",
        "12:00:10  draft_brief        completed",
    ]
