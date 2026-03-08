"""Unit tests for the Phase 15 wait/resume example helper behavior."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from examples import wait_and_resume

from kitaru.errors import KitaruStateError


def test_run_workflow_calls_resume_when_available(monkeypatch) -> None:
    """run_workflow should attempt manual resume after resolving wait input."""
    fake_handle = SimpleNamespace(wait=Mock(return_value="done"))
    fake_thread = Mock()
    fake_thread.is_alive.return_value = False
    monkeypatch.setattr(
        wait_and_resume,
        "_start_flow_in_background",
        Mock(return_value=(fake_thread, {"handle": fake_handle, "error": None})),
    )
    monkeypatch.setattr(
        wait_and_resume,
        "_wait_for_pending_wait",
        Mock(return_value=("kr-15", "approve_release:0")),
    )

    fake_client = Mock()
    fake_client.executions.input.return_value = SimpleNamespace(
        status=SimpleNamespace(value="waiting")
    )
    fake_client.executions.resume.return_value = SimpleNamespace(
        status=SimpleNamespace(value="running")
    )
    monkeypatch.setattr(wait_and_resume, "KitaruClient", Mock(return_value=fake_client))

    exec_id, status_after_input, result = wait_and_resume.run_workflow(topic="kitaru")

    fake_client.executions.input.assert_called_once_with(
        "kr-15",
        wait="approve_release:0",
        value=True,
    )
    fake_client.executions.resume.assert_called_once_with("kr-15")
    fake_thread.join.assert_called_once_with(timeout=60.0)
    assert exec_id == "kr-15"
    assert status_after_input == "waiting"
    assert result == "done"


def test_run_workflow_ignores_resume_state_errors(monkeypatch) -> None:
    """Auto-resume backends may reject explicit resume calls; helper ignores that."""
    fake_handle = SimpleNamespace(wait=Mock(return_value="done"))
    fake_thread = Mock()
    fake_thread.is_alive.return_value = False
    monkeypatch.setattr(
        wait_and_resume,
        "_start_flow_in_background",
        Mock(return_value=(fake_thread, {"handle": fake_handle, "error": None})),
    )
    monkeypatch.setattr(
        wait_and_resume,
        "_wait_for_pending_wait",
        Mock(return_value=("kr-15", "approve_release:0")),
    )

    fake_client = Mock()
    fake_client.executions.input.return_value = SimpleNamespace(
        status=SimpleNamespace(value="running")
    )
    fake_client.executions.resume.side_effect = KitaruStateError("already running")
    monkeypatch.setattr(wait_and_resume, "KitaruClient", Mock(return_value=fake_client))

    _, status_after_input, result = wait_and_resume.run_workflow(topic="kitaru")

    fake_client.executions.resume.assert_called_once_with("kr-15")
    assert status_after_input == "running"
    assert result == "done"


def test_run_workflow_times_out_if_starter_thread_never_finishes(monkeypatch) -> None:
    """run_workflow should fail clearly if background starter never returns."""
    fake_thread = Mock()
    fake_thread.is_alive.return_value = True
    monkeypatch.setattr(
        wait_and_resume,
        "_start_flow_in_background",
        Mock(return_value=(fake_thread, {"handle": None, "error": None})),
    )
    monkeypatch.setattr(
        wait_and_resume,
        "_wait_for_pending_wait",
        Mock(return_value=("kr-15", "approve_release:0")),
    )

    fake_client = Mock()
    fake_client.executions.input.return_value = SimpleNamespace(
        status=SimpleNamespace(value="waiting")
    )
    monkeypatch.setattr(wait_and_resume, "KitaruClient", Mock(return_value=fake_client))

    with pytest.raises(TimeoutError, match="background flow start"):
        wait_and_resume.run_workflow(topic="kitaru")


def test_watch_prints_manual_unblock_commands(monkeypatch, capsys) -> None:
    """Watcher should emit CLI commands once the pending wait becomes visible."""
    stop_event = wait_and_resume.threading.Event()

    wait_lookup = Mock(side_effect=[None, ("kr-15", "wait-uuid")])
    monkeypatch.setattr(wait_and_resume, "_find_pending_wait_for_topic", wait_lookup)
    monkeypatch.setattr(wait_and_resume.time, "sleep", lambda _: None)

    wait_and_resume._watch_and_print_unblock_commands(
        client=Mock(),
        topic="kitaru",
        stop_event=stop_event,
    )

    output = capsys.readouterr().out
    assert "kitaru executions input kr-15 --wait wait-uuid --value true" in output
    assert "kitaru executions resume kr-15" in output


def test_run_workflow_interactive_uses_main_thread_flow_call(monkeypatch) -> None:
    """Interactive mode should call flow directly and return its final result."""
    fake_client = Mock()
    monkeypatch.setattr(wait_and_resume, "KitaruClient", Mock(return_value=fake_client))
    monkeypatch.setattr(
        wait_and_resume,
        "_watch_and_print_unblock_commands",
        lambda **_: None,
    )

    fake_flow_call = Mock(return_value="done")
    monkeypatch.setattr(wait_and_resume, "wait_for_approval_flow", fake_flow_call)

    result = wait_and_resume.run_workflow_interactive(topic="kitaru")

    fake_flow_call.assert_called_once_with("kitaru")
    assert result == "done"
