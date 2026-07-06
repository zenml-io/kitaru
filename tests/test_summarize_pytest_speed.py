"""Tests for pytest speed probe summarization."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

import summarize_pytest_speed


def _write_events(path: Path, events: list[dict[str, Any]]) -> None:
    path.write_text(
        "".join(f"{json.dumps(event, sort_keys=True)}\n" for event in events),
        encoding="utf-8",
    )


def test_summarizer_reports_malformed_numeric_field_with_source(
    tmp_path: Path,
) -> None:
    event_path = tmp_path / "events-master-1.jsonl"
    _write_events(
        event_path,
        [
            {
                "kind": "test_phase",
                "nodeid": "tests/test_example.py::test_example",
                "phase": "call",
                "pid": 123,
                "seconds": "slow",
                "session_id": "run-a",
                "timestamp": 1.0,
                "worker": "master",
            }
        ],
    )

    with pytest.raises(SystemExit) as exc_info:
        summarize_pytest_speed.main(["summarize_pytest_speed.py", str(tmp_path)])

    message = str(exc_info.value)
    assert "Invalid numeric field 'seconds'" in message
    assert "events-master-1.jsonl:1" in message
    assert "tests/test_example.py::test_example" in message


def test_summarizer_rejects_multiple_sessions(tmp_path: Path) -> None:
    _write_events(
        tmp_path / "events-master-1.jsonl",
        [
            {
                "kind": "session_finished",
                "pid": 123,
                "session_id": "run-a",
                "timestamp": 1.0,
                "worker": "master",
            }
        ],
    )
    _write_events(
        tmp_path / "events-master-2.jsonl",
        [
            {
                "kind": "session_finished",
                "pid": 456,
                "session_id": "run-b",
                "timestamp": 2.0,
                "worker": "master",
            }
        ],
    )

    with pytest.raises(SystemExit) as exc_info:
        summarize_pytest_speed.main(["summarize_pytest_speed.py", str(tmp_path)])

    message = str(exc_info.value)
    assert "multiple pytest speed-probe sessions" in message
    assert "run-a" in message
    assert "run-b" in message
    assert "events-master-1.jsonl:1" in message
    assert "events-master-2.jsonl:1" in message


def test_summarizer_rejects_missing_session_id(tmp_path: Path) -> None:
    _write_events(
        tmp_path / "events-master-1.jsonl",
        [
            {
                "kind": "session_finished",
                "pid": 123,
                "timestamp": 1.0,
                "worker": "master",
            }
        ],
    )

    with pytest.raises(SystemExit) as exc_info:
        summarize_pytest_speed.main(["summarize_pytest_speed.py", str(tmp_path)])

    message = str(exc_info.value)
    assert "session_id" in message
    assert "events-master-1.jsonl:1" in message
