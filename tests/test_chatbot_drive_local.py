from __future__ import annotations

import builtins
import sys
from dataclasses import dataclass
from typing import Any

import pytest
from examples.chatbot import drive_local
from examples.chatbot.chatbot import (
    CHATBOT_SESSION_LABEL_METADATA_KEY,
    CHATBOT_TURN_METADATA_KEY,
    chatbot_wait_metadata,
)

from kitaru.client import ExecutionStatus, PendingWait


@dataclass
class FakeExecution:
    exec_id: str
    status: ExecutionStatus = ExecutionStatus.WAITING
    pending_wait: PendingWait | None = None


class FakeExecutionsAPI:
    def __init__(
        self,
        *,
        list_snapshots: builtins.list[builtins.list[FakeExecution]] | None = None,
        get_sequences: dict[str, builtins.list[FakeExecution | BaseException]]
        | None = None,
    ) -> None:
        self._list_snapshots = list_snapshots or []
        self._get_sequences = get_sequences or {}
        self._last_snapshot = self._list_snapshots[0] if self._list_snapshots else []
        self.list_calls: builtins.list[dict[str, Any]] = []
        self.get_calls: builtins.list[str] = []
        self.input_calls: builtins.list[dict[str, Any]] = []

    def list(self, **kwargs: Any) -> builtins.list[FakeExecution]:
        self.list_calls.append(kwargs)
        if self._list_snapshots:
            index = min(len(self.list_calls) - 1, len(self._list_snapshots) - 1)
            self._last_snapshot = self._list_snapshots[index]
        return self._last_snapshot

    def get(self, exec_id: str) -> FakeExecution:
        self.get_calls.append(exec_id)
        if exec_id in self._get_sequences:
            sequence = self._get_sequences[exec_id]
            index = min(self.get_calls.count(exec_id) - 1, len(sequence) - 1)
            item = sequence[index]
            if isinstance(item, BaseException):
                raise item
            return item

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


def _pending_wait(
    *,
    wait_id: str,
    name: str,
    question: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> PendingWait:
    return PendingWait(
        wait_id=wait_id,
        name=name,
        question=question,
        schema=None,
        metadata=metadata or {},
        entered_waiting_at=None,
    )


def _wait(
    *,
    wait_id: str,
    session_label: str,
    turn: int,
    question: str | None = "Assistant question?",
) -> PendingWait:
    return _pending_wait(
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


def test_find_pending_wait_for_session_matches_wait_metadata_without_get() -> None:
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
        {
            "flow": "chatbot",
            "status": ExecutionStatus.WAITING.value,
            "limit": drive_local.DEFAULT_WAIT_SEARCH_LIMIT,
        }
    ]
    assert executions.get_calls == []


def test_find_pending_wait_for_session_searches_beyond_twenty_stale_waits() -> None:
    target_label = "chatbot-local-after-stale-waits"
    stale_waits = [
        FakeExecution(
            f"exec-stale-{index}",
            pending_wait=_wait(
                wait_id=f"wait-stale-{index}",
                session_label=f"stale-session-{index}",
                turn=index,
            ),
        )
        for index in range(25)
    ]
    executions = FakeExecutionsAPI(
        list_snapshots=[
            [
                *stale_waits,
                FakeExecution(
                    "exec-target",
                    pending_wait=_wait(
                        wait_id="wait-target",
                        session_label=target_label,
                        turn=0,
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

    assert match is not None
    assert match.exec_id == "exec-target"
    assert executions.list_calls == [
        {
            "flow": "chatbot",
            "status": ExecutionStatus.WAITING.value,
            "limit": drive_local.DEFAULT_WAIT_SEARCH_LIMIT,
        }
    ]


def test_find_pending_wait_for_session_hydrates_list_result_missing_metadata() -> None:
    target_label = "chatbot-local-hydrated"
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
                    pending_wait=_pending_wait(
                        wait_id="wait-target",
                        name="user_turn_2",
                        question="Assistant question?",
                    ),
                ),
            ]
        ],
        get_sequences={
            "exec-target": [
                FakeExecution(
                    "exec-target",
                    pending_wait=_wait(
                        wait_id="wait-target",
                        session_label=target_label,
                        turn=2,
                    ),
                )
            ]
        },
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
    assert executions.get_calls == ["exec-target"]


def test_find_pending_wait_for_session_continues_after_get_backend_error() -> None:
    target_label = "chatbot-local-after-get-error"
    executions = FakeExecutionsAPI(
        list_snapshots=[
            [
                FakeExecution(
                    "exec-needs-hydration",
                    pending_wait=_pending_wait(
                        wait_id="wait-needs-hydration",
                        name="user_turn_0",
                    ),
                ),
                FakeExecution(
                    "exec-target",
                    pending_wait=_wait(
                        wait_id="wait-target",
                        session_label=target_label,
                        turn=1,
                    ),
                ),
            ]
        ],
        get_sequences={
            "exec-needs-hydration": [RuntimeError("backend hydration failed")]
        },
    )
    client: Any = FakeClient(executions)

    match = drive_local.find_pending_wait_for_session(
        client=client,
        session_label=target_label,
    )

    assert match is not None
    assert match.exec_id == "exec-target"
    assert executions.get_calls == ["exec-needs-hydration"]
    assert executions.list_calls == [
        {
            "flow": "chatbot",
            "status": ExecutionStatus.WAITING.value,
            "limit": drive_local.DEFAULT_WAIT_SEARCH_LIMIT,
        }
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


def test_wait_for_pending_wait_uses_list_fallback_when_get_fails() -> None:
    session_label = "chatbot-local-list-fallback"
    state = drive_local.BackgroundRunState(handle=FakeHandle("exec-target"))
    executions = FakeExecutionsAPI(
        list_snapshots=[
            [
                FakeExecution(
                    "exec-target",
                    pending_wait=_wait(
                        wait_id="wait-target",
                        session_label=session_label,
                        turn=0,
                    ),
                )
            ]
        ],
        get_sequences={"exec-target": [RuntimeError("hydration race")]},
    )
    client: Any = FakeClient(executions)

    match = drive_local.wait_for_pending_wait(
        client=client,
        session_label=session_label,
        state=state,
        timeout_seconds=1.0,
        poll_interval_seconds=0.01,
    )

    assert match.exec_id == "exec-target"
    assert match.wait_id == "wait-target"
    assert executions.get_calls == ["exec-target"]
    assert executions.list_calls == [
        {"flow": "chatbot", "limit": drive_local.DEFAULT_WAIT_SEARCH_LIMIT}
    ]


def test_wait_for_pending_wait_timeout_mentions_search_limit_and_lookup_error() -> None:
    state = drive_local.BackgroundRunState(handle=FakeHandle("exec-missing"))
    executions = FakeExecutionsAPI(
        list_snapshots=[[]],
        get_sequences={"exec-missing": [RuntimeError("backend still hydrating")]},
    )
    client: Any = FakeClient(executions)

    with pytest.raises(TimeoutError) as exc_info:
        drive_local.wait_for_pending_wait(
            client=client,
            session_label="chatbot-local-timeout",
            state=state,
            timeout_seconds=0.01,
            poll_interval_seconds=0.01,
            wait_search_limit=7,
        )

    message = str(exc_info.value)
    assert "most recent 7 waiting chatbot executions" in message
    assert "stale waiting executions" in message
    assert "backend still hydrating" in message


def test_wait_for_pending_wait_stops_on_terminal_execution() -> None:
    session_label = "chatbot-local-finished"
    state = drive_local.BackgroundRunState(handle=FakeHandle("exec-finished"))
    executions = FakeExecutionsAPI(
        list_snapshots=[
            [FakeExecution("exec-finished", status=ExecutionStatus.COMPLETED)]
        ],
        get_sequences={"exec-finished": [RuntimeError("hydration race")]},
    )
    client: Any = FakeClient(executions)

    with pytest.raises(RuntimeError, match="terminal status"):
        drive_local.wait_for_pending_wait(
            client=client,
            session_label=session_label,
            state=state,
            timeout_seconds=1.0,
            poll_interval_seconds=0.01,
        )

    assert executions.get_calls == ["exec-finished"]
    assert executions.list_calls == [
        {"flow": "chatbot", "limit": drive_local.DEFAULT_WAIT_SEARCH_LIMIT}
    ]


@pytest.mark.parametrize(
    "status",
    [ExecutionStatus.RUNNING, ExecutionStatus.WAITING],
)
def test_wait_for_pending_wait_keeps_polling_non_terminal_without_wait(
    status: ExecutionStatus,
) -> None:
    # Regression guard: between turns the execution can be RUNNING (or WAITING
    # without hydrated wait data) with no pending wait yet. The driver must keep
    # polling instead of treating those statuses as terminal.
    session_label = "chatbot-local-between-turns"
    state = drive_local.BackgroundRunState(handle=FakeHandle("exec-between"))
    executions = FakeExecutionsAPI(
        get_sequences={
            "exec-between": [
                FakeExecution("exec-between", status=status),
                FakeExecution(
                    "exec-between",
                    pending_wait=_wait(
                        wait_id="wait-next",
                        session_label=session_label,
                        turn=1,
                    ),
                ),
            ]
        }
    )
    client: Any = FakeClient(executions)

    match = drive_local.wait_for_pending_wait(
        client=client,
        session_label=session_label,
        state=state,
        timeout_seconds=1.0,
        poll_interval_seconds=0.01,
    )

    assert match.exec_id == "exec-between"
    assert match.wait_id == "wait-next"
    assert executions.get_calls == ["exec-between", "exec-between"]


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


def test_drive_chatbot_rejects_empty_messages() -> None:
    client: Any = FakeClient(FakeExecutionsAPI())

    with pytest.raises(ValueError, match="messages"):
        drive_local.drive_chatbot((), client=client)


def test_wait_for_completion_or_extra_wait_uses_list_fallback_for_extra_wait() -> None:
    session_label = "chatbot-local-extra-fallback"
    state = drive_local.BackgroundRunState()
    runner_thread: Any = FakeThread(alive=False)
    executions = FakeExecutionsAPI(
        list_snapshots=[
            [
                FakeExecution(
                    "exec-extra",
                    pending_wait=_wait(
                        wait_id="wait-extra",
                        session_label=session_label,
                        turn=1,
                    ),
                )
            ]
        ],
        get_sequences={"exec-extra": [RuntimeError("hydration race")]},
    )
    client: Any = FakeClient(executions)

    with pytest.raises(RuntimeError, match="asked for another input"):
        drive_local.wait_for_completion_or_extra_wait(
            client=client,
            session_label=session_label,
            state=state,
            exec_id="exec-extra",
            runner_thread=runner_thread,
            answered_wait_ids={"wait-one"},
            timeout_seconds=1.0,
            poll_interval_seconds=0.01,
        )

    assert executions.get_calls == ["exec-extra"]
    assert executions.list_calls == [
        {"flow": "chatbot", "limit": drive_local.DEFAULT_WAIT_SEARCH_LIMIT}
    ]


def test_completion_uses_list_fallback_for_completed_status() -> None:
    state = drive_local.BackgroundRunState()
    runner_thread: Any = FakeThread(alive=False)
    executions = FakeExecutionsAPI(
        list_snapshots=[[FakeExecution("exec-done", status=ExecutionStatus.COMPLETED)]],
        get_sequences={"exec-done": [RuntimeError("hydration race")]},
    )
    client: Any = FakeClient(executions)

    drive_local.wait_for_completion_or_extra_wait(
        client=client,
        session_label="chatbot-local-completed-fallback",
        state=state,
        exec_id="exec-done",
        runner_thread=runner_thread,
        answered_wait_ids={"wait-one"},
        timeout_seconds=1.0,
        poll_interval_seconds=0.01,
    )

    assert executions.get_calls == ["exec-done"]
    assert executions.list_calls == [
        {"flow": "chatbot", "limit": drive_local.DEFAULT_WAIT_SEARCH_LIMIT}
    ]


@pytest.mark.parametrize(
    ("timing_kwargs", "match"),
    [
        ({"wait_timeout_seconds": 0.0}, "wait_timeout_seconds"),
        ({"finish_timeout_seconds": 0.0}, "finish_timeout_seconds"),
        ({"poll_interval_seconds": 0.0}, "poll_interval_seconds"),
        ({"wait_timeout_seconds": float("nan")}, "finite number greater than 0"),
        ({"finish_timeout_seconds": float("inf")}, "finite number greater than 0"),
        ({"poll_interval_seconds": float("-inf")}, "finite number greater than 0"),
    ],
)
def test_drive_chatbot_rejects_invalid_timing_values(
    timing_kwargs: dict[str, Any], match: str
) -> None:
    client: Any = FakeClient(FakeExecutionsAPI())

    with pytest.raises(ValueError, match=match):
        drive_local.drive_chatbot(("hello",), client=client, **timing_kwargs)


def test_parse_args_rejects_non_finite_cli_timing_values(monkeypatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        ["drive_local.py", "--wait-timeout", "nan", "hello"],
    )

    with pytest.raises(SystemExit):
        drive_local._parse_args()


def _patch_start_chatbot_run(
    monkeypatch: pytest.MonkeyPatch,
    *,
    expected_label: str,
    state: drive_local.BackgroundRunState,
    thread: FakeThread,
) -> None:
    def fake_start_chatbot_run(
        label: str,
    ) -> tuple[drive_local.BackgroundRunState, FakeThread]:
        assert label == expected_label
        return state, thread

    monkeypatch.setattr(drive_local, "_start_chatbot_run", fake_start_chatbot_run)


def test_drive_chatbot_submits_messages_to_matched_wait_ids(monkeypatch) -> None:
    session_label = "chatbot-local-scripted"
    handle = FakeHandle("exec-scripted", result="finished")
    state = drive_local.BackgroundRunState(handle=handle)
    _patch_start_chatbot_run(
        monkeypatch,
        expected_label=session_label,
        state=state,
        thread=FakeThread(alive=False),
    )

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
    state = drive_local.BackgroundRunState(handle=handle)
    _patch_start_chatbot_run(
        monkeypatch,
        expected_label=session_label,
        state=state,
        thread=FakeThread(alive=False),
    )

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
