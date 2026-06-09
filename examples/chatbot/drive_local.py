"""Drive the durable chatbot locally without blocking on ``handle.wait()``.

This script keeps the two jobs separate:

1. a background thread starts ``chatbot.run(...)``;
2. the foreground thread finds this run's pending wait and submits messages with
   ``client.executions.input(...)``.

That split matters because ``handle.wait()`` waits for the whole conversation to
finish. It does not return just because the flow is waiting for human input.
"""

from __future__ import annotations

import argparse
import threading
import time
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from kitaru.client import ExecutionStatus, KitaruClient

try:
    from .chatbot import (
        CHATBOT_SESSION_LABEL_METADATA_KEY,
        CHATBOT_TURN_METADATA_KEY,
        chatbot,
    )
except ImportError:
    from chatbot import (  # type: ignore[no-redef]
        CHATBOT_SESSION_LABEL_METADATA_KEY,
        CHATBOT_TURN_METADATA_KEY,
        chatbot,
    )

FLOW_NAME = "chatbot"
DEFAULT_MESSAGES = (
    "Hello! Please answer in one short sentence.",
    "Thanks, bye.",
    "Bye.",
)
DEFAULT_WAIT_TIMEOUT_SECONDS = 300.0
DEFAULT_FINISH_TIMEOUT_SECONDS = 120.0
DEFAULT_POLL_INTERVAL_SECONDS = 1.0


@dataclass(frozen=True)
class PendingWaitMatch:
    """The one pending wait that belongs to this local chatbot session."""

    exec_id: str
    wait_id: str
    wait_name: str
    question: str | None
    turn: int | None


@dataclass
class BackgroundRunState:
    """State shared between the background chatbot thread and foreground driver."""

    handle: Any | None = None
    error: BaseException | None = None


def _coerce_execution_status(status: Any) -> ExecutionStatus | None:
    """Return STATUS as an ExecutionStatus when possible."""
    if isinstance(status, ExecutionStatus):
        return status
    try:
        return ExecutionStatus(str(getattr(status, "value", status)).lower())
    except ValueError:
        return None


def _status_value(status: Any) -> str:
    """Return a public status value from a string or enum-like object."""
    coerced = _coerce_execution_status(status)
    if coerced is not None:
        return coerced.value
    return str(getattr(status, "value", status)).lower()


def _is_terminal_execution(execution: Any) -> bool:
    """Return whether EXECUTION has finished and cannot produce another wait."""
    status = _coerce_execution_status(getattr(execution, "status", None))
    return bool(status is not None and status.is_finished)


def _pending_wait_metadata(pending_wait: Any) -> dict[str, Any]:
    """Return pending-wait metadata as a plain dict."""
    metadata = getattr(pending_wait, "metadata", {})
    if isinstance(metadata, dict):
        return metadata
    return {}


def _match_pending_wait(
    *,
    execution: Any,
    session_label: str,
    ignored_wait_ids: set[str] | None = None,
) -> PendingWaitMatch | None:
    """Return the pending wait on EXECUTION if its metadata matches this session."""
    pending_wait = getattr(execution, "pending_wait", None)
    if pending_wait is None:
        return None

    wait_id = pending_wait.wait_id
    if ignored_wait_ids is not None and wait_id in ignored_wait_ids:
        return None

    metadata = _pending_wait_metadata(pending_wait)
    if metadata.get(CHATBOT_SESSION_LABEL_METADATA_KEY) != session_label:
        return None

    turn = metadata.get(CHATBOT_TURN_METADATA_KEY)
    return PendingWaitMatch(
        exec_id=execution.exec_id,
        wait_id=wait_id,
        wait_name=getattr(pending_wait, "name", "<unnamed>"),
        question=getattr(pending_wait, "question", None),
        turn=turn if isinstance(turn, int) else None,
    )


def find_pending_wait_for_session(
    *,
    client: KitaruClient,
    session_label: str,
    flow: str = FLOW_NAME,
    limit: int = 20,
    ignored_wait_ids: set[str] | None = None,
) -> PendingWaitMatch | None:
    """Find the single pending chatbot wait with metadata for SESSION_LABEL."""
    matches: list[PendingWaitMatch] = []
    executions = client.executions.list(
        flow=flow,
        status=ExecutionStatus.WAITING.value,
        limit=limit,
    )
    for execution in executions:
        match = _match_pending_wait(
            execution=execution,
            session_label=session_label,
            ignored_wait_ids=ignored_wait_ids,
        )
        if match is not None:
            matches.append(match)

    if len(matches) > 1:
        exec_ids = ", ".join(match.exec_id for match in matches)
        raise RuntimeError(
            "Found multiple pending chatbot waits for session "
            f"{session_label!r}: {exec_ids}. Each local driver session should "
            "have at most one pending wait."
        )
    return matches[0] if matches else None


def _find_pending_wait_on_execution(
    *,
    client: KitaruClient,
    exec_id: str,
    session_label: str,
    ignored_wait_ids: set[str] | None = None,
) -> PendingWaitMatch | None:
    """Inspect one execution for the next pending wait in this session."""
    execution = client.executions.get(exec_id)
    match = _match_pending_wait(
        execution=execution,
        session_label=session_label,
        ignored_wait_ids=ignored_wait_ids,
    )
    if match is not None:
        return match
    if _is_terminal_execution(execution):
        raise RuntimeError(
            f"Execution {exec_id} reached terminal status "
            f"{_status_value(getattr(execution, 'status', None))!r} before "
            "another chatbot wait appeared."
        )
    return None


def _raise_background_error(state: BackgroundRunState) -> None:
    """Surface a background-thread exception in the foreground driver."""
    if state.error is not None:
        raise RuntimeError(
            "The background chatbot run failed before the driver could submit "
            "the next message."
        ) from state.error


def _validate_public_poll_interval(poll_interval_seconds: float) -> None:
    """Reject poll intervals that would create a tight user-facing loop."""
    if poll_interval_seconds <= 0:
        raise ValueError("poll_interval_seconds must be greater than 0.")


def _raise_extra_wait_after_messages(match: PendingWaitMatch) -> None:
    """Tell the user the scripted driver ran out of messages."""
    raise RuntimeError(
        "Submitted all configured messages, but the chatbot asked for another "
        f"input at {match.wait_name} ({match.wait_id}) on execution "
        f"{match.exec_id}. Add another scripted message and rerun the driver. "
        "If you answer from another terminal instead, run "
        f"kitaru executions input {match.exec_id} --value '\"hello\"'. "
        "If the local driver process exits before the run continues, you may "
        "also need to resume the execution after providing input."
    )


def wait_for_pending_wait(
    *,
    client: KitaruClient,
    session_label: str,
    state: BackgroundRunState,
    known_exec_id: str | None = None,
    runner_thread: threading.Thread | None = None,
    ignored_wait_ids: set[str] | None = None,
    timeout_seconds: float = DEFAULT_WAIT_TIMEOUT_SECONDS,
    poll_interval_seconds: float = DEFAULT_POLL_INTERVAL_SECONDS,
) -> PendingWaitMatch:
    """Poll until this local chatbot session reaches a pending wait."""
    deadline = time.monotonic() + timeout_seconds
    exec_id = known_exec_id

    while time.monotonic() < deadline:
        _raise_background_error(state)

        if exec_id is None and state.handle is not None:
            exec_id = getattr(state.handle, "exec_id", None)

        if exec_id is not None:
            match = _find_pending_wait_on_execution(
                client=client,
                exec_id=exec_id,
                session_label=session_label,
                ignored_wait_ids=ignored_wait_ids,
            )
        else:
            match = find_pending_wait_for_session(
                client=client,
                session_label=session_label,
                ignored_wait_ids=ignored_wait_ids,
            )

        if match is not None:
            return match

        if (
            runner_thread is not None
            and not runner_thread.is_alive()
            and state.handle is None
        ):
            raise RuntimeError(
                "The background chatbot thread stopped before returning a "
                "handle or reaching a pending wait."
            )

        time.sleep(poll_interval_seconds)

    _raise_background_error(state)
    raise TimeoutError(
        f"Timed out after {timeout_seconds:.0f}s waiting for chatbot session "
        f"{session_label!r} to reach a pending wait."
    )


def wait_for_completion_or_extra_wait(
    *,
    client: KitaruClient,
    session_label: str,
    state: BackgroundRunState,
    exec_id: str | None,
    runner_thread: threading.Thread,
    answered_wait_ids: set[str],
    timeout_seconds: float = DEFAULT_FINISH_TIMEOUT_SECONDS,
    poll_interval_seconds: float = DEFAULT_POLL_INTERVAL_SECONDS,
) -> None:
    """Wait until the run finishes, or fail if it reaches another wait."""
    deadline = time.monotonic() + timeout_seconds

    while time.monotonic() < deadline:
        _raise_background_error(state)

        if exec_id is not None:
            try:
                execution = client.executions.get(exec_id)
            except ValueError:
                execution = None

            if execution is not None:
                match = _match_pending_wait(
                    execution=execution,
                    session_label=session_label,
                    ignored_wait_ids=answered_wait_ids,
                )
                if match is not None:
                    _raise_extra_wait_after_messages(match)
                if _is_terminal_execution(execution) and not runner_thread.is_alive():
                    return
        elif not runner_thread.is_alive():
            return

        time.sleep(poll_interval_seconds)

    _raise_background_error(state)
    if runner_thread.is_alive():
        raise RuntimeError(
            "Submitted all configured messages, but the chatbot is still "
            "running. It may need another user message soon; inspect pending "
            "waits with `kitaru executions list`."
        )
    raise TimeoutError(
        f"Timed out after {timeout_seconds:.0f}s waiting for chatbot session "
        f"{session_label!r} to finish after the final scripted message."
    )


def _start_chatbot_run(
    session_label: str,
) -> tuple[BackgroundRunState, threading.Thread]:
    """Start ``chatbot.run(...)`` on a background thread."""
    state = BackgroundRunState()

    def _runner() -> None:
        try:
            state.handle = chatbot.run(session_label=session_label, cache=False)
        except Exception as exc:
            state.error = exc

    runner_thread = threading.Thread(
        target=_runner,
        name=f"chatbot-local-{session_label}",
        daemon=True,
    )
    runner_thread.start()
    return state, runner_thread


def submit_message(
    *,
    client: KitaruClient,
    match: PendingWaitMatch,
    message: str,
) -> None:
    """Submit MESSAGE to the matched pending wait."""
    client.executions.input(match.exec_id, wait=match.wait_id, value=message)


def drive_chatbot(
    messages: Sequence[str] = DEFAULT_MESSAGES,
    *,
    client: KitaruClient | None = None,
    session_label: str | None = None,
    wait_timeout_seconds: float = DEFAULT_WAIT_TIMEOUT_SECONDS,
    finish_timeout_seconds: float = DEFAULT_FINISH_TIMEOUT_SECONDS,
    poll_interval_seconds: float = DEFAULT_POLL_INTERVAL_SECONDS,
) -> Any:
    """Run the chatbot locally and feed it scripted messages."""
    _validate_public_poll_interval(poll_interval_seconds)
    client = client or KitaruClient()
    session_label = session_label or f"chatbot-local-{uuid4().hex}"
    state, runner_thread = _start_chatbot_run(session_label)
    exec_id: str | None = None
    answered_wait_ids: set[str] = set()

    print(f"Started local chatbot session {session_label!r}.")

    for message in messages:
        match = wait_for_pending_wait(
            client=client,
            session_label=session_label,
            state=state,
            known_exec_id=exec_id,
            runner_thread=runner_thread,
            ignored_wait_ids=answered_wait_ids,
            timeout_seconds=wait_timeout_seconds,
            poll_interval_seconds=poll_interval_seconds,
        )
        exec_id = match.exec_id

        label = f"turn {match.turn}" if match.turn is not None else match.wait_name
        if match.question:
            print(f"\nAssistant ({label}): {match.question}")
        else:
            print(f"\nAssistant is waiting at {match.wait_name} ({match.wait_id}).")
        print(f"User: {message}")
        submit_message(client=client, match=match, message=message)
        answered_wait_ids.add(match.wait_id)

    wait_for_completion_or_extra_wait(
        client=client,
        session_label=session_label,
        state=state,
        exec_id=exec_id,
        runner_thread=runner_thread,
        answered_wait_ids=answered_wait_ids,
        timeout_seconds=finish_timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
    )
    if state.handle is None:
        raise RuntimeError("The background chatbot thread finished without a handle.")

    result = state.handle.wait()
    print("\nConversation ended.")
    return result


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run examples/chatbot/chatbot.py locally while this foreground "
            "process answers pending waits."
        ),
    )
    parser.add_argument(
        "messages",
        nargs="*",
        help=(
            "Scripted user messages to submit. If omitted, a tiny hello/bye "
            "conversation is used."
        ),
    )
    parser.add_argument(
        "--wait-timeout",
        type=float,
        default=DEFAULT_WAIT_TIMEOUT_SECONDS,
        help="Seconds to wait for each pending chatbot turn.",
    )
    parser.add_argument(
        "--finish-timeout",
        type=float,
        default=DEFAULT_FINISH_TIMEOUT_SECONDS,
        help="Seconds to wait for the chatbot run to finish after all messages.",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=DEFAULT_POLL_INTERVAL_SECONDS,
        help="Seconds between pending-wait polling attempts. Must be greater than 0.",
    )
    args = parser.parse_args()
    if args.poll_interval <= 0:
        parser.error("--poll-interval must be greater than 0")
    return args


def main() -> None:
    args = _parse_args()
    messages = tuple(args.messages) if args.messages else DEFAULT_MESSAGES
    drive_chatbot(
        messages,
        wait_timeout_seconds=args.wait_timeout,
        finish_timeout_seconds=args.finish_timeout,
        poll_interval_seconds=args.poll_interval,
    )


if __name__ == "__main__":
    main()
