from __future__ import annotations

import builtins
from dataclasses import dataclass, field
from typing import Any

import pytest
from examples.chatbot import drive_local
from examples.chatbot.chatbot import (
    CHATBOT_SESSION_LABEL_METADATA_KEY,
    CHATBOT_TURN_METADATA_KEY,
    chatbot_wait_metadata,
)

from kitaru.client import ExecutionStatus


@dataclass
class FakePendingWait:
    wait_id: str
    name: str
    question: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class FakeExecution:
    exec_id: str
    status: ExecutionStatus = ExecutionStatus.WAITING
    pending_wait: FakePendingWait | None = None


class FakeExecutionsAPI:
    def __init__(
        self,
        *,
        list_snapshots: builtins.list[builtins.list[FakeExecution]] | None = None,
        get_sequences: dict[str, builtins.list[FakeExecution]] | None = None,
    ) -> None:
        self._list_snapshots = list_snapshots or []
        self._get_sequences = get_sequences or {}
        self._list_calls = 0
        self._get_calls: dict[str, int] = {}
        self._last_snapshot = self._list_snapshots[0] if self._list_snapshots else []
        self.list_calls: builtins.list[dict[str, Any]] = []
        self.input_calls: builtins.list[dict[str, Any]] = []

    def list(self, **kwargs: Any) -> builtins.list[FakeExecution]:
        self.list_calls.append(kwargs)
        if self._list_snapshots:
            index = min(self._list_calls, len(self._list_snapshots) - 1)
            self._last_snapshot = self._list_snapshots[index]
        self._list_calls += 1
        return self._last_snapshot

    def get(self, exec_id: str) -> FakeExecution:
        if exec_id in self._get_sequences:
            sequence = self._get_sequences[exec_id]
            index = min(self._get_calls.get(exec_id, 0), len(sequence) - 1)
            self._get_calls[exec_id] = index + 1
            return sequence[index]

        for execution in self._last_snapshot:
            if execution.exec_id == exec_id:
                return execution
        raise ValueError(f"Unknown fake execution {exec_id}")

    def input(self, exec_id: str, *, wait: str, value: Any) -> None:
        self.input_calls.append({"exec_id": exec_id, "wait": wait, "value": value})


class FakeClient:
    def __init__(self, executions: FakeExecutionsAPI) -> None:
        self.executions = executions


class FakeHandle:
    def __init__(self, exec_id: str, result: str = "done") -> None:
        self.exec_id = exec_id
        self._result = result
        self.wait_called = False

    def wait(self) -> str:
        self.wait_called = True
        return self._result


class FakeThread:
    def __init__(self, *, alive: bool = False) -> None:
        self._alive = alive

    def is_alive(self) -> bool:
        return self._alive


def _wait(
    *,
    wait_id: str,
    session_label: str,
    turn: int,
    question: str | None = "Assistant question?",
) -> FakePendingWait:
    return FakePendingWait(
        wait_id=wait_id,
        name=f"user_turn_{turn}",
        question=question,
        metadata={
            CHATBOT_SESSION_LABEL_METADATA_KEY: session_label,
            CHATBOT_TURN_METADATA_KEY: turn,
        },
    )


def test_chatbot_wait_metadata_contains_driver_discovery_fields() -> None:
    metadata = chatbot_wait_metadata(
        session_label="chatbot-local-contract",
        turn=3,
    )

    assert metadata == {
        CHATBOT_SESSION_LABEL_METADATA_KEY: "chatbot-local-contract",
        CHATBOT_TURN_METADATA_KEY: 3,
    }


def test_find_pending_wait_for_session_matches_wait_metadata() -> None:
    target_label = "chatbot-local-target"
    executions = FakeExecutionsAPI(
        list_snapshots=[
            [
                FakeExecution(
                    "exec-other",
                    pending_wait=_wait(
                        wait_id="wait-other",
                        session_label="other-session",
                        turn=0,
                    ),
                ),
                FakeExecution(
                    "exec-target",
                    pending_wait=_wait(
                        wait_id="wait-target",
                        session_label=target_label,
                        turn=2,
                    ),
                ),
            ]
        ]
    )
    client: Any = FakeClient(executions)

    match = drive_local.find_pending_wait_for_session(
        client=client,
        session_label=target_label,
    )

    assert match == drive_local.PendingWaitMatch(
        exec_id="exec-target",
        wait_id="wait-target",
        wait_name="user_turn_2",
        question="Assistant question?",
        turn=2,
    )
    assert executions.list_calls == [
        {"flow": "chatbot", "status": ExecutionStatus.WAITING.value, "limit": 20}
    ]


def test_find_pending_wait_for_session_rejects_multiple_matches() -> None:
    session_label = "chatbot-local-duplicate"
    client: Any = FakeClient(
        FakeExecutionsAPI(
            list_snapshots=[
                [
                    FakeExecution(
                        "exec-one",
                        pending_wait=_wait(
                            wait_id="wait-one",
                            session_label=session_label,
                            turn=0,
                        ),
                    ),
                    FakeExecution(
                        "exec-two",
                        pending_wait=_wait(
                            wait_id="wait-two",
                            session_label=session_label,
                            turn=1,
                        ),
                    ),
                ]
            ]
        )
    )

    with pytest.raises(RuntimeError, match="multiple pending chatbot waits"):
        drive_local.find_pending_wait_for_session(
            client=client,
            session_label=session_label,
        )


def test_wait_for_pending_wait_polls_until_metadata_match_appears() -> None:
    session_label = "chatbot-local-later"
    client: Any = FakeClient(
        FakeExecutionsAPI(
            list_snapshots=[
                [],
                [
                    FakeExecution(
                        "exec-later",
                        pending_wait=_wait(
                            wait_id="wait-later",
                            session_label=session_label,
                            turn=0,
                        ),
                    )
                ],
            ]
        )
    )

    match = drive_local.wait_for_pending_wait(
        client=client,
        session_label=session_label,
        state=drive_local.BackgroundRunState(),
        timeout_seconds=1.0,
        poll_interval_seconds=0.01,
    )

    assert match.exec_id == "exec-later"
    assert match.wait_id == "wait-later"


def test_wait_for_pending_wait_stops_on_terminal_execution() -> None:
    session_label = "chatbot-local-finished"
    state = drive_local.BackgroundRunState(handle=FakeHandle("exec-finished"))
    client: Any = FakeClient(
        FakeExecutionsAPI(
            get_sequences={
                "exec-finished": [
                    FakeExecution("exec-finished", status=ExecutionStatus.COMPLETED)
                ]
            }
        )
    )

    with pytest.raises(RuntimeError, match="terminal status"):
        drive_local.wait_for_pending_wait(
            client=client,
            session_label=session_label,
            state=state,
            timeout_seconds=1.0,
            poll_interval_seconds=0.01,
        )


def test_wait_for_pending_wait_surfaces_background_thread_errors() -> None:
    state = drive_local.BackgroundRunState(error=ValueError("model exploded"))
    client: Any = FakeClient(FakeExecutionsAPI())

    with pytest.raises(RuntimeError, match="background chatbot run failed") as exc_info:
        drive_local.wait_for_pending_wait(
            client=client,
            session_label="chatbot-local-error",
            state=state,
            timeout_seconds=1.0,
            poll_interval_seconds=0.01,
        )

    assert isinstance(exc_info.value.__cause__, ValueError)


def test_drive_chatbot_rejects_non_positive_public_poll_interval() -> None:
    client: Any = FakeClient(FakeExecutionsAPI())

    with pytest.raises(ValueError, match="poll_interval_seconds"):
        drive_local.drive_chatbot(
            ("hello",),
            client=client,
            poll_interval_seconds=0.0,
        )


def test_drive_chatbot_submits_messages_to_matched_wait_ids(monkeypatch) -> None:
    session_label = "chatbot-local-scripted"
    handle = FakeHandle("exec-scripted", result="finished")
    fake_thread = FakeThread(alive=False)
    state = drive_local.BackgroundRunState(handle=handle)

    def fake_start_chatbot_run(
        label: str,
    ) -> tuple[drive_local.BackgroundRunState, FakeThread]:
        assert label == session_label
        return state, fake_thread

    monkeypatch.setattr(drive_local, "_start_chatbot_run", fake_start_chatbot_run)

    executions = FakeExecutionsAPI(
        get_sequences={
            "exec-scripted": [
                FakeExecution(
                    "exec-scripted",
                    pending_wait=_wait(
                        wait_id="wait-one",
                        session_label=session_label,
                        turn=0,
                    ),
                ),
                FakeExecution(
                    "exec-scripted",
                    pending_wait=_wait(
                        wait_id="wait-two",
                        session_label=session_label,
                        turn=1,
                    ),
                ),
                FakeExecution("exec-scripted", status=ExecutionStatus.COMPLETED),
            ]
        }
    )
    client: Any = FakeClient(executions)

    result = drive_local.drive_chatbot(
        ("hello", "bye"),
        client=client,
        session_label=session_label,
        wait_timeout_seconds=1.0,
        finish_timeout_seconds=2.0,
        poll_interval_seconds=0.01,
    )

    assert result == "finished"
    assert handle.wait_called is True
    assert executions.input_calls == [
        {"exec_id": "exec-scripted", "wait": "wait-one", "value": "hello"},
        {"exec_id": "exec-scripted", "wait": "wait-two", "value": "bye"},
    ]


def test_drive_chatbot_raises_if_messages_run_out_before_completion(
    monkeypatch,
) -> None:
    session_label = "chatbot-local-needs-more-input"
    handle = FakeHandle("exec-needs-more", result="finished")
    fake_thread = FakeThread(alive=False)
    state = drive_local.BackgroundRunState(handle=handle)

    def fake_start_chatbot_run(
        label: str,
    ) -> tuple[drive_local.BackgroundRunState, FakeThread]:
        assert label == session_label
        return state, fake_thread

    monkeypatch.setattr(drive_local, "_start_chatbot_run", fake_start_chatbot_run)

    executions = FakeExecutionsAPI(
        get_sequences={
            "exec-needs-more": [
                FakeExecution(
                    "exec-needs-more",
                    pending_wait=_wait(
                        wait_id="wait-one",
                        session_label=session_label,
                        turn=0,
                    ),
                ),
                FakeExecution(
                    "exec-needs-more",
                    pending_wait=_wait(
                        wait_id="wait-two",
                        session_label=session_label,
                        turn=1,
                    ),
                ),
            ]
        }
    )
    client: Any = FakeClient(executions)

    with pytest.raises(RuntimeError, match="asked for another input"):
        drive_local.drive_chatbot(
            ("hello",),
            client=client,
            session_label=session_label,
            wait_timeout_seconds=1.0,
            finish_timeout_seconds=2.0,
            poll_interval_seconds=0.01,
        )

    assert handle.wait_called is False
    assert executions.input_calls == [
        {"exec_id": "exec-needs-more", "wait": "wait-one", "value": "hello"}
    ]
