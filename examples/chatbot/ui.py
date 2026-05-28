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

from kitaru.client import ArtifactRef, Execution, ExecutionStatus, KitaruClient

_REPO_ROOT = str(Path(__file__).resolve().parents[2])
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

FLOW_NAME = "chatbot"
DEPLOYMENT_TAG = "prod"
SESSION_LIMIT = 10
# `executions.get` itself takes ~1.5s, so a long sleep just adds dead time.
POLL_INTERVAL = 0.2
POLL_TIMEOUT = 180  # deployment invocation cold start can be slow
_PREVIEW_MAX = 48
_NO_MESSAGES_PREVIEW = "(no messages yet)"

client = KitaruClient()

# ---------------------------------------------------------------------------
# Execution + artifact helpers
# ---------------------------------------------------------------------------


def _short(exec_id: str) -> str:
    return exec_id.split("-")[0]


def _is_ready(ex: Execution) -> bool:
    """An execution is ready for the next UI action when waiting or finished."""
    if ex.status.is_finished:
        return True
    return ex.status == ExecutionStatus.WAITING and ex.pending_wait is not None


def _try_get(exec_id: str) -> Execution | None:
    """Fetch the hydrated Execution; fall back to the list endpoint on hydration races.

    ``executions.get`` does ``include_details=True`` which can transiently fail
    with ``Unable to load the configuration for step ...`` on a freshly invoked
    deployment whose metadata hasn't fully committed. The list endpoint skips
    that hydration and still populates ``status`` and ``pending_wait`` — enough
    for status polling.
    """
    try:
        return client.executions.get(exec_id)
    except Exception:
        for ex in client.executions.list(flow=FLOW_NAME, limit=20):
            if ex.exec_id == exec_id:
                return ex
        return None


def _poll_until_ready(exec_id: str) -> Execution | None:
    """Poll until the execution is WAITING or finished; return hydrated Execution."""
    deadline = time.time() + POLL_TIMEOUT
    while time.time() < deadline:
        ex = _try_get(exec_id)
        if ex is not None and _is_ready(ex):
            return ex
        time.sleep(POLL_INTERVAL)
    return None


# ---------------------------------------------------------------------------
# History (single source of truth: the latest 'history' artifact)
# ---------------------------------------------------------------------------


def _history_length(a: ArtifactRef) -> int:
    """Saved history length is monotonically increasing — use it as a turn index.

    ``producing_call`` is just ``"greet"`` / ``"chat_turn"`` (no per-call digit
    suffix), so we can't extract a turn index from it. Each saved history is
    strictly longer than the previous one (greet=1, then +2 per chat_turn).
    """
    raw = (a.metadata or {}).get("length") if isinstance(a.metadata, dict) else None
    return raw if isinstance(raw, int) else 0


def _normalize_history(raw: Any) -> list[dict[str, str]]:
    """Normalize a loaded history artifact value to messages-format dicts."""
    if not raw:
        return []
    out: list[dict[str, str]] = []
    for m in raw:
        if isinstance(m, dict):
            out.append({"role": str(m["role"]), "content": str(m["content"])})
        else:
            out.append({"role": str(m.role), "content": str(m.content)})
    return out


def _load_history(
    *,
    exec_id: str | None = None,
    arts: list[ArtifactRef] | None = None,
    retries: int = 1,
    retry_sleep: float = 0.5,
) -> list[dict[str, str]]:
    """Load the latest ``history`` artifact given an ``exec_id`` or pre-fetched arts.

    Pass ``arts`` when the caller already has a hydrated Execution (saves a
    ~1.5s artifacts.list roundtrip). Otherwise pass ``exec_id`` and we fetch.
    Use ``retries`` > 1 right after a flow first transitions to WAITING — the
    artifact list can briefly trail the status update.
    """
    for attempt in range(retries):
        if arts is None:
            try:
                arts = client.artifacts.list(exec_id, name="history") if exec_id else []
            except Exception:
                arts = []
        history_arts = [a for a in arts if a.name == "history"]
        if history_arts:
            latest = max(history_arts, key=_history_length)
            return _normalize_history(latest.load())
        arts = None  # force re-fetch on next attempt
        if attempt + 1 < retries:
            time.sleep(retry_sleep)
    return []


# ---------------------------------------------------------------------------
# Session sidebar
# ---------------------------------------------------------------------------

# Preview cache: once a session has a user message, its first-user-message
# preview is fixed forever. We cache aggressively and only re-fetch when the
# cached value is the empty-state placeholder.
_preview_cache: dict[str, str] = {}


def _preview_from_history(history: list[dict[str, str]]) -> str:
    """First user message (else greeting), collapsed and truncated."""
    if not history:
        return _NO_MESSAGES_PREVIEW
    raw = next(
        (m["content"] for m in history if m["role"] == "user"),
        history[0]["content"],
    )
    raw = " ".join(raw.split())
    return raw[:_PREVIEW_MAX] + ("…" if len(raw) > _PREVIEW_MAX else "")


def _session_preview(
    exec_id: str, *, history: list[dict[str, str]] | None = None
) -> str:
    """Cached one-line preview; pass ``history`` if you already have it."""
    cached = _preview_cache.get(exec_id)
    if cached is not None and cached != _NO_MESSAGES_PREVIEW:
        return cached
    if history is None:
        history = _load_history(exec_id=exec_id)
    snippet = _preview_from_history(history)
    _preview_cache[exec_id] = snippet
    return snippet


def _session_label(ex: Execution, preview: str) -> str:
    when = ex.started_at.strftime("%m-%d %H:%M") if ex.started_at else ""
    marker = "●" if ex.status == ExecutionStatus.WAITING else "○"
    return f"{marker} {preview}  ·  {when}"


_VISIBLE_STATUSES = {
    ExecutionStatus.WAITING,
    ExecutionStatus.COMPLETED,
    ExecutionStatus.RUNNING,
}


def _sessions_from(execs: list[Execution]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for ex in execs:
        if ex.status not in _VISIBLE_STATUSES:
            continue
        out.append((_session_label(ex, _session_preview(ex.exec_id)), ex.exec_id))
        if len(out) >= SESSION_LIMIT:
            break
    return out


def _list_sessions() -> list[tuple[str, str]]:
    """Recent live + completed chats, newest first, with a content preview."""
    return _sessions_from(
        client.executions.list(flow=FLOW_NAME, limit=SESSION_LIMIT * 2)
    )


# ---------------------------------------------------------------------------
# Gradio callbacks
# ---------------------------------------------------------------------------

_PLACEHOLDER_GREETING = [
    {
        "role": "assistant",
        "content": "Hey 👋 — getting set up. I'll be with you in a moment.",
    }
]


def refresh_sessions() -> gr.Dropdown:
    return gr.Dropdown(choices=_list_sessions())


def new_chat():
    """Invoke the deployed chatbot flow; placeholder greeting while it boots."""
    # First yield must be cheap — no server roundtrips. `gr.skip()` leaves the
    # existing dropdown untouched; we refresh it after the flow is ready.
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

    ex = _poll_until_ready(handle.exec_id)
    if ex is None:
        yield (
            [],
            {},
            gr.Dropdown(choices=_list_sessions()),
            gr.Textbox(interactive=False),
            "Timed out waiting for the deployment to start. Try again.",
        )
        return

    history = _load_history(arts=ex.artifacts, retries=10, retry_sleep=0.5)
    wait_id = ex.pending_wait.wait_id if ex.pending_wait else None
    # NOTE: refresh sidebar choices but DON'T set `value=ex.exec_id` here — that
    # would fire `sessions.change → load_session`, racing with the state we just
    # produced.
    yield (
        history,
        {"exec_id": ex.exec_id, "wait_id": wait_id},
        gr.Dropdown(choices=_list_sessions()),
        gr.Textbox(interactive=wait_id is not None),
        f"New chat · {_short(ex.exec_id)}",
    )


def load_session(
    exec_id: str | None,
) -> tuple[list[dict], dict, gr.Textbox, str]:
    if not exec_id:
        return [], {}, gr.Textbox(interactive=False), ""
    ex = _try_get(exec_id)
    if ex is None:
        return [], {}, gr.Textbox(interactive=False), "Could not load session."
    history = _load_history(arts=ex.artifacts, retries=5, retry_sleep=0.4)
    wait_id = ex.pending_wait.wait_id if ex.pending_wait else None
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

    if live is None:
        return (
            [],
            {},
            gr.Dropdown(choices=_sessions_from(execs)),
            gr.Textbox(
                interactive=False,
                placeholder="No live chats — click + New chat to start one.",
            ),
            "Welcome — no active chats yet.",
        )

    # Load history once and prime the preview cache before the sidebar renders.
    history = _load_history(exec_id=live.exec_id, retries=3, retry_sleep=0.3)
    _session_preview(live.exec_id, history=history)

    assert live.pending_wait is not None
    return (
        history,
        {"exec_id": live.exec_id, "wait_id": live.pending_wait.wait_id},
        gr.Dropdown(choices=_sessions_from(execs), value=live.exec_id),
        gr.Textbox(interactive=True),
        f"Resumed · {_short(live.exec_id)}",
    )


def respond(message: str, history: list[dict], state: dict):
    """Stream the chat update so the user's message appears immediately."""
    exec_id: str | None = state.get("exec_id")
    wait_id: str | None = state.get("wait_id")

    if not exec_id or not wait_id or not message.strip():
        yield history, state, gr.Textbox(value=message), ""
        return

    pending = [*history, {"role": "user", "content": message}]
    yield (
        [
            *pending,
            {"role": "assistant", "content": "…", "metadata": {"title": "Thinking"}},
        ],
        state,
        gr.Textbox(value="", interactive=False),
        "Thinking…",
    )

    # `input` writes the value into the server-side wait; the deployment
    # runtime picks it up and runs the next chat_turn. The UI never resumes.
    client.executions.input(exec_id, wait=wait_id, value=message)
    ex = _poll_until_ready(exec_id)

    if ex is None:
        new_history, next_wait_id = pending, None
    else:
        next_wait_id = ex.pending_wait.wait_id if ex.pending_wait else None
        new_history = _load_history(arts=ex.artifacts) or pending

    yield (
        new_history,
        {"exec_id": exec_id, "wait_id": next_wait_id},
        gr.Textbox(value="", interactive=next_wait_id is not None),
        "" if next_wait_id else "Conversation ended.",
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

    new_btn.click(new_chat, outputs=[chatbot_ui, state, sessions, msg, status_bar])
    refresh_btn.click(refresh_sessions, outputs=[sessions])
    sessions.change(
        load_session,
        inputs=[sessions],
        outputs=[chatbot_ui, state, msg, status_bar],
    )
    msg.submit(respond, [msg, chatbot_ui, state], [chatbot_ui, state, msg, status_bar])
    demo.load(
        initial_load,
        outputs=[chatbot_ui, state, sessions, msg, status_bar],
    )


if __name__ == "__main__":
    demo.launch(css=CSS)
