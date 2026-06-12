"""Drive the durable chatbot to completion with scripted user turns.

The chatbot flow runs in-process and suspends at each ``wait_for_input``. A
single process therefore cannot both *run* the flow and *answer* its waits — the
executor must stay alive to resume when a wait is resolved. This driver runs the
flow on a background thread (the executor) and answers pending waits from the
main thread via ``executions.input``.

This produces a complete, real chatbot execution on the active stack with model
and tool-call checkpoints — useful both as the live Act-1 artifact and as a
cohort source for Replay Lab.

Run:
    uv run python examples/chatbot/run_scripted_chat.py
"""

from __future__ import annotations

import sys
import threading
import time

try:
    from .chatbot import chatbot
except ImportError:
    from chatbot import chatbot  # type: ignore[no-redef]

from kitaru.client import Execution, ExecutionStatus, KitaruClient

client = KitaruClient()

# A scripted support conversation that exercises a read-only tool and the
# guarded refund tool, so the run shows real tool-call checkpoints.
DEFAULT_SCRIPT = [
    "Hi, is the cordless drill in stock?",
    "Can you also refund my order A1002 for 80 dollars?",
    "Okay, understood. Thanks, bye!",
]


def _recent_chatbot_ids() -> set[str]:
    """Current chatbot execution ids, used to detect the one we start."""
    return {ex.exec_id for ex in client.executions.list(flow="chatbot")}


def drive(script: list[str], max_turns: int = 12) -> str:
    """Run one chatbot flow on a thread and answer its waits to completion.

    ``chatbot.run()`` executes the flow in-process and blocks at each wait, so
    it runs on a daemon thread (the executor that resumes when we answer). It
    does not return a handle while blocked, so the new execution id is
    discovered from the executions list.
    """
    before = _recent_chatbot_ids()
    executor = threading.Thread(target=chatbot.run, daemon=True)
    executor.start()

    exec_id = ""
    deadline = time.time() + 60
    while time.time() < deadline:
        new = _recent_chatbot_ids() - before
        if new:
            exec_id = next(iter(new))
            break
        time.sleep(1)
    if not exec_id:
        raise TimeoutError("chatbot execution never appeared")
    print(f"started execution {exec_id}", flush=True)

    pending = list(script)
    answered: set[str] = set()
    for _ in range(max_turns):
        ex = _wait_for_state(exec_id)
        if ex.status in _TERMINAL:
            break
        pw = ex.pending_wait
        if pw is None or pw.name in answered:
            time.sleep(1)
            continue
        reply = pending.pop(0) if pending else "Thanks, that's all — goodbye!"
        print(f"  agent: {(pw.question or '').strip()[:90]}", flush=True)
        print(f"  user : {reply}", flush=True)
        client.executions.input(exec_id, wait=pw.name, value=reply)
        answered.add(pw.name)

    executor.join(timeout=30)
    final = client.executions.get(exec_id)
    print(f"finished: {final.status.value}", flush=True)
    url = getattr(final, "url", None)
    if url:
        print(url, flush=True)
    return exec_id


_TERMINAL = {
    ExecutionStatus.COMPLETED,
    ExecutionStatus.FAILED,
    ExecutionStatus.CANCELLED,
}


def _try_get(exec_id: str) -> Execution | None:
    """Fetch the execution, tolerating transient step-hydration races.

    ``executions.get`` hydrates every step (``include_details=True``) and can
    transiently fail with ``Unable to load the configuration for step ...`` when
    a checkpoint is mid-creation — especially on a local orchestrator writing to
    a remote server. The list endpoint skips that hydration but still populates
    ``status`` and ``pending_wait``, which is all the poll loop needs.
    """
    try:
        return client.executions.get(exec_id)
    except Exception:
        for ex in client.executions.list(flow="chatbot", limit=20):
            if ex.exec_id == exec_id:
                return ex
        return None


def _wait_for_state(exec_id: str, timeout: float = 120.0) -> Execution:
    """Poll until the execution is waiting-with-a-pending-wait or terminal."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        ex = _try_get(exec_id)
        if ex is not None:
            if ex.status in _TERMINAL:
                return ex
            if ex.status == ExecutionStatus.WAITING and ex.pending_wait is not None:
                return ex
        time.sleep(1)
    raise TimeoutError(f"execution {exec_id} stalled")


def main() -> int:
    """Drive one scripted conversation to completion."""
    drive(DEFAULT_SCRIPT)
    return 0


if __name__ == "__main__":
    sys.exit(main())
