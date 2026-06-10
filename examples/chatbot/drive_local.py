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
import math
import threading
import time
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from kitaru.client import Execution, ExecutionStatus, KitaruClient

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
DEFAULT_WAIT_SEARCH_LIMIT = 50
_POSITIVE_SECONDS_MESSAGE = "must be a finite number greater than 0"


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


def _is_terminal_execution(execution: Execution) -> bool:
    """Return whether EXECUTION has finished and cannot produce another wait."""
    return execution.status.is_finished


def _fallback_execution_from_recent_list(
    *,
    client: KitaruClient,
    exec_id: str,
    limit: int = DEFAULT_WAIT_SEARCH_LIMIT,
) -> Execution | None:
    """Return EXEC_ID from recent list data when detailed get fails.

    ``executions.get`` hydrates extra details and can fail while the lighter
    list endpoint still reports enough information for local polling. The list
    is not filtered by status so terminal executions stay visible, and the scan
    stays deliberately bounded so a permanently broken ``get`` does not trigger
    broad backend reads forever.
    """
    executions = client.executions.list(flow=FLOW_NAME, limit=limit)
    return next(
        (execution for execution in executions if execution.exec_id == exec_id),
        None,
    )


def _try_get_execution(
    *,
    client: KitaruClient,
    exec_id: str,
    list_limit: int = DEFAULT_WAIT_SEARCH_LIMIT,
) -> tuple[Execution | None, BaseException | None]:
    """Fetch EXEC_ID, using bounded list data if detailed hydration fails."""
    try:
        return client.executions.get(exec_id), None
    except Exception as exc:
        fallback = _fallback_execution_from_recent_list(
            client=client,
            exec_id=exec_id,
            limit=list_limit,
        )
        return fallback, exc


def _match_pending_wait(
    *,
    execution: Execution,
    session_label: str,
    ignored_wait_ids: set[str] | None = None,
) -> PendingWaitMatch | None:
    """Return the pending wait on EXECUTION if its metadata matches this session."""
    pending_wait = execution.pending_wait
    if pending_wait is None:
        return None
    if ignored_wait_ids is not None and pending_wait.wait_id in ignored_wait_ids:
        return None
    if pending_wait.metadata.get(CHATBOT_SESSION_LABEL_METADATA_KEY) != session_label:
        return None

    turn = pending_wait.metadata.get(CHATBOT_TURN_METADATA_KEY)
    return PendingWaitMatch(
        exec_id=execution.exec_id,
        wait_id=pending_wait.wait_id,
        wait_name=pending_wait.name,
        question=pending_wait.question,
        turn=turn if isinstance(turn, int) else None,
    )


def _hydrate_pending_wait_match_if_needed(
    *,
    client: KitaruClient,
    execution: Execution,
    session_label: str,
    ignored_wait_ids: set[str] | None = None,
) -> PendingWaitMatch | None:
    """Hydrate one list result only when list data cannot identify the session."""
    match = _match_pending_wait(
        execution=execution,
        session_label=session_label,
        ignored_wait_ids=ignored_wait_ids,
    )
    if match is not None:
        return match

    pending_wait = execution.pending_wait
    if (
        pending_wait is not None
        and CHATBOT_SESSION_LABEL_METADATA_KEY in pending_wait.metadata
    ):
        # List data already identifies this wait's session; it is just not ours.
        return None

    # The caller already has a bounded list result for this execution. Try the
    # detailed endpoint once, but do not immediately repeat the same list query
    # for this candidate.
    try:
        hydrated_execution = client.executions.get(execution.exec_id)
    except Exception:
        return None

    return _match_pending_wait(
        execution=hydrated_execution,
        session_label=session_label,
        ignored_wait_ids=ignored_wait_ids,
    )


def find_pending_wait_for_session(
    *,
    client: KitaruClient,
    session_label: str,
    limit: int = DEFAULT_WAIT_SEARCH_LIMIT,
    ignored_wait_ids: set[str] | None = None,
) -> PendingWaitMatch | None:
    """Find the single pending chatbot wait with metadata for SESSION_LABEL."""
    matches: list[PendingWaitMatch] = []
    executions = client.executions.list(
        flow=FLOW_NAME,
        status=ExecutionStatus.WAITING.value,
        limit=limit,
    )
    for execution in executions:
        match = _hydrate_pending_wait_match_if_needed(
            client=client,
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
    list_limit: int = DEFAULT_WAIT_SEARCH_LIMIT,
) -> tuple[PendingWaitMatch | None, BaseException | None]:
    """Inspect one execution for the next pending wait in this session."""
    execution, lookup_error = _try_get_execution(
        client=client,
        exec_id=exec_id,
        list_limit=list_limit,
    )
    if execution is None:
        return None, lookup_error

    match = _match_pending_wait(
        execution=execution,
        session_label=session_label,
        ignored_wait_ids=ignored_wait_ids,
    )
    if match is not None:
        return match, lookup_error
    if _is_terminal_execution(execution):
        raise RuntimeError(
            f"Execution {exec_id} reached terminal status "
            f"{execution.status.value!r} before another chatbot wait appeared."
        )
    return None, lookup_error


def _raise_background_error(state: BackgroundRunState) -> None:
    """Surface a background-thread exception in the foreground driver."""
    if state.error is not None:
        raise RuntimeError(
            "The background chatbot run failed before the driver could submit "
            "the next message."
        ) from state.error


def _is_positive_finite_seconds(value: float) -> bool:
    """Return whether VALUE is safe to use as a public duration."""
    return math.isfinite(value) and value > 0


def _validate_positive_seconds(value: float, *, name: str) -> None:
    """Reject non-finite and non-positive durations at the public entrypoint."""
    if not _is_positive_finite_seconds(value):
        raise ValueError(f"{name} {_POSITIVE_SECONDS_MESSAGE}.")


def _positive_seconds_arg(raw: str) -> float:
    """argparse type for durations: a finite number of seconds greater than 0."""
    value = float(raw)
    if not _is_positive_finite_seconds(value):
        raise argparse.ArgumentTypeError(_POSITIVE_SECONDS_MESSAGE)
    return value


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
    wait_search_limit: int = DEFAULT_WAIT_SEARCH_LIMIT,
) -> PendingWaitMatch:
    """Poll until this local chatbot session reaches a pending wait."""
    deadline = time.monotonic() + timeout_seconds
    exec_id = known_exec_id
    last_lookup_error: BaseException | None = None

    while time.monotonic() < deadline:
        _raise_background_error(state)

        if exec_id is None and state.handle is not None:
            exec_id = getattr(state.handle, "exec_id", None)

        if exec_id is not None:
            match, lookup_error = _find_pending_wait_on_execution(
                client=client,
                exec_id=exec_id,
                session_label=session_label,
                ignored_wait_ids=ignored_wait_ids,
                list_limit=wait_search_limit,
            )
            if lookup_error is not None:
                last_lookup_error = lookup_error
        else:
            match = find_pending_wait_for_session(
                client=client,
                session_label=session_label,
                ignored_wait_ids=ignored_wait_ids,
                limit=wait_search_limit,
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
    message = (
        f"Timed out after {timeout_seconds:.0f}s waiting for chatbot session "
        f"{session_label!r} to reach a pending wait. The driver searched the "
        f"most recent {wait_search_limit} waiting chatbot executions on each "
        "poll; stale waiting executions can hide a new local run if that limit "
        "is too low."
    )
    if last_lookup_error is not None:
        message += f" Last execution lookup failed with: {last_lookup_error}"
    raise TimeoutError(message)


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
    last_lookup_error: BaseException | None = None

    while time.monotonic() < deadline:
        _raise_background_error(state)

        if exec_id is not None:
            execution, lookup_error = _try_get_execution(
                client=client,
                exec_id=exec_id,
            )
            if lookup_error is not None:
                last_lookup_error = lookup_error

            if execution is not None:
                match = _match_pending_wait(
                    execution=execution,
                    session_label=session_label,
                    ignored_wait_ids=answered_wait_ids,
                )
                if match is not None:
                    raise RuntimeError(
                        "Submitted all configured messages, but the chatbot "
                        f"asked for another input at {match.wait_name} "
                        f"({match.wait_id}) on execution {match.exec_id}. Add "
                        "another scripted message and rerun the driver. If you "
                        "answer from another terminal instead, run "
                        f"kitaru executions input {match.exec_id} "
                        "--value '\"hello\"'. If the local driver process "
                        "exits before the run continues, you may also need to "
                        "resume the execution after providing input."
                    )
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
    message = (
        f"Timed out after {timeout_seconds:.0f}s waiting for chatbot session "
        f"{session_label!r} to finish after the final scripted message."
    )
    if last_lookup_error is not None:
        message += f" Last execution lookup failed with: {last_lookup_error}"
    raise TimeoutError(message)


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
    if not messages:
        raise ValueError("messages must contain at least one scripted message.")
    for name, value in (
        ("wait_timeout_seconds", wait_timeout_seconds),
        ("finish_timeout_seconds", finish_timeout_seconds),
        ("poll_interval_seconds", poll_interval_seconds),
    ):
        _validate_positive_seconds(value, name=name)
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
        client.executions.input(match.exec_id, wait=match.wait_id, value=message)
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
        type=_positive_seconds_arg,
        default=DEFAULT_WAIT_TIMEOUT_SECONDS,
        help="Seconds to wait for each pending chatbot turn.",
    )
    parser.add_argument(
        "--finish-timeout",
        type=_positive_seconds_arg,
        default=DEFAULT_FINISH_TIMEOUT_SECONDS,
        help="Seconds to wait for the chatbot run to finish after all messages.",
    )
    parser.add_argument(
        "--poll-interval",
        type=_positive_seconds_arg,
        default=DEFAULT_POLL_INTERVAL_SECONDS,
        help="Seconds between pending-wait polling attempts. Must be greater than 0.",
    )
    return parser.parse_args()


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
