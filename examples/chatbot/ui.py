"""Gradio chat UI for the durable Kitaru chatbot.

The chatbot flow runs as a **server-side deployment** — the UI never spawns a
local Python subprocess. Each "New chat" calls ``client.deployments.invoke()``
which triggers a remote execution; the server hosts the flow and owns its
wait/resume lifecycle. The UI just polls execution state and pipes user input
into pending waits via ``executions.input``.

One-time deploy (rerun after editing chatbot.py):
    kitaru deploy chatbot.py:chatbot --tag prod --stack <remote-stack>

Then:
    uv add --dev gradio
    export OPENAI_API_KEY=sk-...
    uv run examples/chatbot/ui.py
"""

import sys
import time
from pathlib import Path
from typing import Any

import gradio as gr

from kitaru.client import ExecutionStatus, KitaruClient

_REPO_ROOT = str(Path(__file__).resolve().parents[2])
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

FLOW_NAME = "chatbot"
DEPLOYMENT_TAG = "prod"
SESSION_LIMIT = 10
# `executions.get` itself takes ~1.5s, so a long sleep just adds dead time.
POLL_INTERVAL = 0.2
POLL_TIMEOUT = 180  # deployment invocation cold start can be slow

client = KitaruClient()

# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def _short(exec_id: str) -> str:
    return exec_id.split("-")[0]


def _history_sort_key(a: object) -> tuple[int, int]:
    """Order history artifacts by (length, list-position).

    ``producing_call`` is just ``"greet"`` / ``"chat_turn"`` — no per-call
    digit suffix — so we can't extract a turn index from it. Instead, every
    saved history is strictly longer than the previous one (greet=1, then
    +2 messages per chat_turn), and ``metadata["length"]`` records that.
    """
    length = 0
    metadata = getattr(a, "metadata", None) or {}
    raw = metadata.get("length") if isinstance(metadata, dict) else None
    if isinstance(raw, int):
        length = raw
    return (length, 0)


def _latest_history(
    exec_id: str,
    *,
    retries: int = 1,
    retry_sleep: float = 0.5,
) -> list[dict[str, str]]:
    """Load the most recent 'history' artifact for an execution.

    Set ``retries`` > 1 right after a flow transitions to WAITING — the
    artifact list can briefly trail the status update.
    """
    for attempt in range(retries):
        try:
            arts = client.artifacts.list(exec_id, name="history")
        except Exception:
            # Transient hydration race on freshly invoked deployments.
            arts = []
        if arts:
            # Stable sort by (length, index) → highest length wins; ties broken
            # by later list position (insertion order is roughly chronological).
            ordered = sorted(
                enumerate(arts),
                key=lambda ia: (_history_sort_key(ia[1])[0], ia[0]),
            )
            latest = ordered[-1][1]
            raw = latest.load() or []
            out: list[dict[str, str]] = []
            for m in raw:
                if isinstance(m, dict):
                    out.append({"role": str(m["role"]), "content": str(m["content"])})
                else:
                    out.append({"role": str(m.role), "content": str(m.content)})
            return out
        if attempt + 1 < retries:
            time.sleep(retry_sleep)
    return []


def _wait_id_for(exec_id: str) -> str | None:
    ex = client.executions.get(exec_id)
    if ex.status == ExecutionStatus.WAITING and ex.pending_wait is not None:
        return ex.pending_wait.wait_id
    return None


def _lightweight_status(exec_id: str) -> Any | None:
    """Return an Execution from the list endpoint (no step-config hydration).

    ``executions.get`` does ``include_details=True``, which can transiently
    fail with `Unable to load the configuration for step ...` on a freshly
    invoked deployment whose metadata hasn't fully committed. The list
    endpoint skips that hydration and still populates ``status`` and
    ``pending_wait`` — enough for status polling.
    """
    for ex in client.executions.list(flow=FLOW_NAME, limit=20):
        if ex.exec_id == exec_id:
            return ex
    return None


def _poll_until_ready(exec_id: str) -> Any | None:
    """Wait until the execution is WAITING or finished.

    Uses the hydrated ``executions.get`` so the caller gets ``artifacts``
    populated (avoids a second roundtrip for history). If hydration trips on
    transient DB-config races, falls back to the lightweight list endpoint
    for status detection and re-tries hydration after.
    """
    deadline = time.time() + POLL_TIMEOUT
    while time.time() < deadline:
        try:
            ex = client.executions.get(exec_id)
        except Exception:
            # Transient hydration error — fall back to lightweight status.
            ex = _lightweight_status(exec_id)
            if ex is None:
                time.sleep(POLL_INTERVAL)
                continue
        if ex.status == ExecutionStatus.WAITING and ex.pending_wait is not None:
            return ex
        if ex.status.is_finished:
            return ex
        time.sleep(POLL_INTERVAL)
    return None


def _history_from_artifacts(arts: list[Any]) -> list[dict[str, str]]:
    """Pick the latest 'history' artifact and normalize to messages-format dicts."""
    history_arts = [a for a in arts if a.name == "history"]
    if not history_arts:
        return []
    latest = max(history_arts, key=lambda a: _history_sort_key(a)[0])
    raw = latest.load() or []
    out: list[dict[str, str]] = []
    for m in raw:
        if isinstance(m, dict):
            out.append({"role": str(m["role"]), "content": str(m["content"])})
        else:
            out.append({"role": str(m.role), "content": str(m.content)})
    return out


# Preview cache — previews only change when a session is currently live, and
# finished sessions are immutable, so this saves 1-2s per cached session on
# every sidebar refresh.
_preview_cache: dict[str, tuple[str, ExecutionStatus]] = {}


_NO_MESSAGES_PREVIEW = "(no messages yet)"


def _session_preview(exec_id: str, status: ExecutionStatus) -> str:
    """One-line conversation preview, cached aggressively per exec.

    Once a session has at least one user message, the preview text is fixed
    forever — the first user message never changes. So we cache the snippet
    permanently and only re-fetch if the cached value is the empty-state
    placeholder (meaning the session was still on the greeting last time).
    """
    cached = _preview_cache.get(exec_id)
    if cached is not None and cached[0] != _NO_MESSAGES_PREVIEW:
        return cached[0]

    history = _latest_history(exec_id)
    if not history:
        snippet = _NO_MESSAGES_PREVIEW
    else:
        first_user = next((m["content"] for m in history if m["role"] == "user"), None)
        raw = first_user or history[0]["content"]
        raw = " ".join(raw.split())
        snippet = raw[:48] + ("…" if len(raw) > 48 else "")
    _preview_cache[exec_id] = (snippet, status)
    return snippet


def _session_label(
    exec_id: str, status: ExecutionStatus, started_at: object, preview: str
) -> str:
    when = ""
    if started_at is not None and hasattr(started_at, "strftime"):
        when = started_at.strftime("%m-%d %H:%M")  # type: ignore[union-attr]
    marker = "●" if status == ExecutionStatus.WAITING else "○"
    return f"{marker} {preview}  ·  {when}"


def _sessions_from(execs: list[Any]) -> list[tuple[str, str]]:
    keep = {ExecutionStatus.WAITING, ExecutionStatus.COMPLETED, ExecutionStatus.RUNNING}
    out: list[tuple[str, str]] = []
    for e in execs:
        if e.status not in keep:
            continue
        preview = _session_preview(e.exec_id, e.status)
        out.append(
            (_session_label(e.exec_id, e.status, e.started_at, preview), e.exec_id)
        )
        if len(out) >= SESSION_LIMIT:
            break
    return out


def _list_sessions() -> list[tuple[str, str]]:
    """Recent live + completed chats, newest first, with a content preview."""
    execs = client.executions.list(flow=FLOW_NAME, limit=SESSION_LIMIT * 2)
    return _sessions_from(execs)


# ---------------------------------------------------------------------------
# Gradio callbacks
# ---------------------------------------------------------------------------


def refresh_sessions() -> gr.Dropdown:
    return gr.Dropdown(choices=_list_sessions())


_PLACEHOLDER_GREETING = [
    {
        "role": "assistant",
        "content": "Hey 👋 — getting set up. I'll be with you in a moment.",
    }
]


def new_chat():
    """Invoke the deployed chatbot flow; show a placeholder greeting while it boots.

    The placeholder is stack-agnostic — works the same whether the deployment
    runs on local, k8s, vertex, etc. Once the real greet checkpoint completes,
    we swap the placeholder for the actual greeting from the history artifact.
    """
    started = time.time()
    # First yield must be cheap — anything that does a server roundtrip here
    # (like _list_sessions) delays the bubble from reaching the browser.
    # `gr.skip()` leaves the existing dropdown untouched; we refresh it later.
    yield (
        _PLACEHOLDER_GREETING,
        {},
        gr.skip(),
        gr.Textbox(interactive=False),
        "Starting a new chat…",
    )

    try:
        handle = client.deployments.invoke(flow=FLOW_NAME, tag=DEPLOYMENT_TAG)
    except Exception as exc:
        yield (
            [],
            {},
            gr.Dropdown(choices=_list_sessions()),
            gr.Textbox(interactive=False),
            f"Failed to invoke deployment: {exc}",
        )
        return

    exec_id: str = handle.exec_id
    wait_id: str | None = None
    deadline = time.time() + POLL_TIMEOUT
    last_status_yield = 0.0

    while time.time() < deadline:
        ex = _lightweight_status(exec_id)
        if ex is not None:
            if ex.status == ExecutionStatus.WAITING and ex.pending_wait is not None:
                wait_id = ex.pending_wait.wait_id
                break
            if ex.status.is_finished:
                break

        # Tick a subtle elapsed-time indicator in the status bar every second
        # without redrawing the chat bubble.
        now = time.time()
        if now - last_status_yield > 1.0:
            last_status_yield = now
            yield (
                _PLACEHOLDER_GREETING,
                {},
                gr.skip(),  # don't re-fetch sidebar each tick
                gr.Textbox(interactive=False),
                f"Starting a new chat… {int(now - started)}s",
            )
        time.sleep(POLL_INTERVAL)

    history = _latest_history(exec_id, retries=10, retry_sleep=0.5)
    state = {"exec_id": exec_id, "wait_id": wait_id}
    status = f"New chat · {_short(exec_id)}"
    # NOTE: refresh sidebar choices but DON'T set `value=exec_id` here —
    # doing so would fire `sessions.change → load_session`, racing with
    # the state/history we just produced.
    yield (
        history,
        state,
        gr.Dropdown(choices=_list_sessions()),
        gr.Textbox(interactive=wait_id is not None),
        status,
    )


def load_session(
    exec_id: str | None,
) -> tuple[list[dict], dict, gr.Textbox, str]:
    if not exec_id:
        return [], {}, gr.Textbox(interactive=False), ""
    history = _latest_history(exec_id, retries=5, retry_sleep=0.4)
    wait_id = _wait_id_for(exec_id)
    # No owner to adopt: the deployment runtime on the server owns the session.
    state = {"exec_id": exec_id, "wait_id": wait_id}
    status = (
        f"Resumed · {_short(exec_id)}"
        if wait_id
        else f"Read-only · {_short(exec_id)} (conversation ended)"
    )
    return history, state, gr.Textbox(interactive=wait_id is not None), status


def initial_load() -> tuple[list[dict], dict, gr.Dropdown, gr.Textbox, str]:
    """On page open, auto-resume the most recent live (WAITING) chat if any.

    Single ``executions.list`` call is reused for both the sidebar and the
    live-pick. We also pull ``wait_id`` straight off the returned Execution
    instead of doing a second ``executions.get``.
    """
    execs = client.executions.list(flow=FLOW_NAME, limit=SESSION_LIMIT)
    live = next(
        (
            e
            for e in execs
            if e.status == ExecutionStatus.WAITING and e.pending_wait is not None
        ),
        None,
    )

    # Load the live session's history once and prime the preview cache before
    # `_sessions_from` runs — avoids a second 1.5s artifact-list roundtrip.
    history: list[dict[str, str]] = []
    if live is not None:
        history = _latest_history(live.exec_id, retries=3, retry_sleep=0.3)
        if history:
            first_user = next(
                (m["content"] for m in history if m["role"] == "user"), None
            )
            raw = first_user or history[0]["content"]
            raw = " ".join(raw.split())
            snippet = raw[:48] + ("…" if len(raw) > 48 else "")
            _preview_cache[live.exec_id] = (snippet, live.status)

    sessions = _sessions_from(execs)

    if live is None:
        return (
            [],
            {},
            gr.Dropdown(choices=sessions),
            gr.Textbox(
                interactive=False,
                placeholder="No live chats — click + New chat to start one.",
            ),
            "Welcome — no active chats yet.",
        )

    assert live.pending_wait is not None
    state = {"exec_id": live.exec_id, "wait_id": live.pending_wait.wait_id}
    return (
        history,
        state,
        gr.Dropdown(choices=sessions, value=live.exec_id),
        gr.Textbox(interactive=True),
        f"Resumed · {_short(live.exec_id)}",
    )


def respond(
    message: str,
    history: list[dict],
    state: dict,
):
    """Stream the chat update so the user's message appears immediately."""
    exec_id: str | None = state.get("exec_id")
    wait_id: str | None = state.get("wait_id")

    if not exec_id or not wait_id or not message.strip():
        yield history, state, gr.Textbox(value=message), ""
        return

    pending = [*history, {"role": "user", "content": message}]
    thinking = [
        *pending,
        {"role": "assistant", "content": "…", "metadata": {"title": "Thinking"}},
    ]
    # 1. Echo the user message + an animated "Thinking" bubble immediately.
    yield (
        thinking,
        state,
        gr.Textbox(value="", interactive=False),
        "Thinking…",
    )

    # `input` writes the value into the server-side wait condition; the
    # deployment runtime that owns the session picks it up and runs the next
    # chat_turn. The UI never resumes — that's the server's job.
    client.executions.input(exec_id, wait=wait_id, value=message)
    ex = _poll_until_ready(exec_id)

    if ex is None:
        new_history = pending
        next_wait_id: str | None = None
    else:
        next_wait_id = ex.pending_wait.wait_id if ex.pending_wait else None
        # Reuse artifacts from the executions.get we just did — saves a
        # second ~1.5s `executions.get` that `_latest_history` would have
        # done internally.
        new_history = _history_from_artifacts(ex.artifacts) or pending

    new_state = {"exec_id": exec_id, "wait_id": next_wait_id}
    interactive = next_wait_id is not None
    status = "" if interactive else "Conversation ended."
    # 2. Replace with the persisted history (includes the assistant reply).
    yield (
        new_history,
        new_state,
        gr.Textbox(value="", interactive=interactive),
        status,
    )


# ---------------------------------------------------------------------------
# Layout
# ---------------------------------------------------------------------------

CSS = """
#sidebar { border-right: 1px solid var(--border-color-primary); padding-right: 12px; }
#status-bar { color: var(--body-text-color-subdued); font-size: 0.85em; }
"""

with gr.Blocks(title="Kitaru Chatbot") as demo:
    state = gr.State({})

    with gr.Row():
        with gr.Column(scale=1, min_width=240, elem_id="sidebar"):
            gr.Markdown("### Sessions")
            new_btn = gr.Button("+ New chat", variant="primary")
            refresh_btn = gr.Button("Refresh", variant="secondary", size="sm")
            sessions = gr.Dropdown(
                choices=_list_sessions(),
                label="Recent",
                interactive=True,
                container=False,
            )

        with gr.Column(scale=4):
            gr.Markdown("## Kitaru Chatbot")
            chatbot_ui = gr.Chatbot(height=520, show_label=False)
            msg = gr.Textbox(
                placeholder='Pick a session or start a new chat. Type "exit" to end.',
                show_label=False,
                interactive=False,
                autofocus=True,
            )
            status_bar = gr.Markdown("", elem_id="status-bar")

    new_btn.click(
        new_chat,
        outputs=[chatbot_ui, state, sessions, msg, status_bar],
    )
    refresh_btn.click(refresh_sessions, outputs=[sessions])
    sessions.change(
        load_session,
        inputs=[sessions],
        outputs=[chatbot_ui, state, msg, status_bar],
    )
    msg.submit(
        respond,
        [msg, chatbot_ui, state],
        [chatbot_ui, state, msg, status_bar],
    )
    demo.load(
        initial_load,
        outputs=[chatbot_ui, state, sessions, msg, status_bar],
    )


if __name__ == "__main__":
    demo.launch(css=CSS)
