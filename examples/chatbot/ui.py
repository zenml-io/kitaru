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
    uv run python examples/chatbot/ui.py
"""

import sys
import time
from pathlib import Path

import gradio as gr

from kitaru.client import ArtifactRef, Execution, ExecutionStatus, KitaruClient

_REPO_ROOT = str(Path(__file__).resolve().parents[2])
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from examples.chatbot.history_artifacts import (  # noqa: E402
    HISTORY_ARTIFACT_NAME,
    load_longest_usable_history,
)

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
# History (single source of truth: the best usable 'history' artifact)
# ---------------------------------------------------------------------------


def _load_history(
    *,
    exec_id: str | None = None,
    arts: list[ArtifactRef] | None = None,
    retries: int = 1,
    retry_sleep: float = 0.5,
    min_length: int = 0,
) -> list[dict[str, str]]:
    """Load the best usable ``history`` artifact for an execution.

    Pass ``arts`` when the caller already has a hydrated Execution (saves a
    ~1.5s artifacts.list roundtrip). Otherwise pass ``exec_id`` and we fetch.
    Use ``retries`` > 1 right after a flow first transitions to WAITING — the
    artifact list can briefly trail the status update. Pass ``min_length`` when
    the caller already knows the shortest acceptable transcript length; this
    prevents a stale non-empty artifact from replacing a locally pending turn.
    """
    current_arts = arts
    best_short_history: list[dict[str, str]] = []
    for attempt in range(retries):
        if current_arts is None:
            try:
                current_arts = (
                    client.artifacts.list(exec_id, name=HISTORY_ARTIFACT_NAME)
                    if exec_id
                    else []
                )
            except Exception:
                current_arts = []

        history_arts = [a for a in current_arts if a.name == HISTORY_ARTIFACT_NAME]
        if history_arts:
            history = load_longest_usable_history(history_arts)
            if len(history) >= min_length and history:
                return history
            if len(history) > len(best_short_history):
                best_short_history = history

        # If the hydrated Execution had a stale artifact list, fetch a fresh
        # list on the next attempt. If the caller only supplied artifact refs,
        # retrying can still help when ``ArtifactRef.load()`` was transiently
        # unavailable.
        current_arts = None if exec_id else current_arts
        if attempt + 1 < retries:
            time.sleep(retry_sleep)
    return [] if min_length else best_short_history


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

    history = _load_history(
        exec_id=ex.exec_id,
        arts=ex.artifacts,
        retries=10,
        retry_sleep=0.5,
    )
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
    history = _load_history(
        exec_id=ex.exec_id,
        arts=ex.artifacts,
        retries=5,
        retry_sleep=0.4,
    )
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
        min_history_length = (
            len(pending) + 1 if next_wait_id is not None else len(pending)
        )
        new_history = (
            _load_history(
                exec_id=ex.exec_id,
                arts=ex.artifacts,
                retries=3,
                retry_sleep=0.3,
                min_length=min_history_length,
            )
            or pending
        )

    yield (
        new_history,
        {"exec_id": exec_id, "wait_id": next_wait_id},
        gr.Textbox(value="", interactive=next_wait_id is not None),
        "" if next_wait_id else "Conversation ended.",
    )


# ---------------------------------------------------------------------------
# Theme + layout
# ---------------------------------------------------------------------------

# Kitaru palette — exact OKLCH → sRGB conversion of the `[data-app="kitaru"]`
# block from zenml-frontend-monorepo/shared/hashi.
_KITARU = {
    "background": "#faf8f4",
    "card": "#fffefb",
    "foreground": "#2e2016",
    "muted": "#f2e9e2",
    "muted_fg": "#988573",
    "border": "#e8e0d4",
    "sidebar": "#f9f3eb",
    "primary": "#e28c46",
    "primary_hover": "#cf7a32",
    "primary_fg": "#ffffff",
    "ring": "#ce8e58",
}

KITARU_THEME = gr.themes.Default(
    primary_hue=gr.themes.Color(
        c50="#ffde98",
        c100="#ffd08b",
        c200="#ffba74",
        c300="#f8a05b",
        c400="#ea934e",
        c500=_KITARU["primary"],
        c600=_KITARU["primary_hover"],
        c700="#b46210",
        c800="#934400",
        c900="#6d2000",
        c950="#3c0000",
    ),
    neutral_hue=gr.themes.Color(
        c50=_KITARU["card"],
        c100=_KITARU["sidebar"],
        c200=_KITARU["border"],
        c300="#d4c6b1",
        c400=_KITARU["muted_fg"],
        c500="#7a6c5d",
        c600="#5b4f44",
        c700="#3f372f",
        c800=_KITARU["foreground"],
        c900="#1e1814",
        c950="#100c0a",
    ),
    radius_size=gr.themes.sizes.radius_md,
    font=[gr.themes.GoogleFont("Inter"), "ui-sans-serif", "system-ui", "sans-serif"],
    font_mono=[gr.themes.GoogleFont("JetBrains Mono"), "ui-monospace", "monospace"],
).set(
    body_background_fill=_KITARU["background"],
    body_text_color=_KITARU["foreground"],
    body_text_color_subdued=_KITARU["muted_fg"],
    background_fill_primary=_KITARU["card"],
    background_fill_secondary=_KITARU["sidebar"],
    block_background_fill=_KITARU["card"],
    block_label_background_fill=_KITARU["sidebar"],
    block_border_color=_KITARU["border"],
    border_color_primary=_KITARU["border"],
    border_color_accent=_KITARU["primary"],
    color_accent=_KITARU["primary"],
    color_accent_soft=_KITARU["muted"],
    input_background_fill=_KITARU["card"],
    input_border_color=_KITARU["border"],
    input_border_color_focus=_KITARU["ring"],
    button_primary_background_fill=_KITARU["primary"],
    button_primary_background_fill_hover=_KITARU["primary_hover"],
    button_primary_text_color=_KITARU["primary_fg"],
    button_primary_border_color=_KITARU["primary"],
    button_secondary_background_fill=_KITARU["card"],
    button_secondary_background_fill_hover=_KITARU["muted"],
    button_secondary_text_color=_KITARU["foreground"],
    button_secondary_border_color=_KITARU["border"],
    code_background_fill=_KITARU["muted"],
)

# Polished slab layout (no viewport-lock — that broke things).
CSS = """
.gradio-container { padding: 0 !important; }

#chat-shell { padding: 24px 28px 28px 28px; }
#side-rail {
    background: #f9f3eb;
    border-right: 1px solid #e8e0d4;
    padding: 24px 18px;
}
#side-rail .form, #side-rail .block, #side-rail label {
    background: transparent !important;
}

#brand { margin: 0 0 24px 0; line-height: 1.1; }
#brand .wordmark {
    font-family: 'Inter', sans-serif;
    font-weight: 600;
    font-size: 1.05rem;
    letter-spacing: -0.02em;
    color: #2e2016;
}
#brand .wordmark .accent { color: #e28c46; }
#brand .subtitle {
    font-family: 'JetBrains Mono', ui-monospace, monospace;
    font-size: 0.72rem;
    color: #988573;
    text-transform: uppercase;
    letter-spacing: 0.12em;
    margin-top: 4px;
}

#side-rail .rail-label {
    font-family: 'JetBrains Mono', ui-monospace, monospace;
    font-size: 0.68rem;
    text-transform: uppercase;
    letter-spacing: 0.14em;
    color: #988573;
    margin: 20px 0 8px 0;
}
#side-rail button { width: 100%; justify-content: flex-start; }

#chat-col {
    width: 100%;
    display: flex;
    flex-direction: column;
    gap: 14px;
}
#chat-col .chatbot, #chat-col .block {
    border: none !important;
    box-shadow: none !important;
}

#composer textarea {
    font-size: 1rem;
    line-height: 1.45;
    padding: 14px 16px !important;
    border-radius: 14px !important;
    background: #fffefb !important;
    border: 1px solid #e8e0d4 !important;
}
#composer textarea:focus {
    outline: none !important;
    border-color: #ce8e58 !important;
    box-shadow: 0 0 0 4px rgba(226, 140, 70, 0.12) !important;
}
#composer textarea::placeholder { color: #988573; }

#status-bar {
    font-family: 'JetBrains Mono', ui-monospace, monospace;
    font-size: 0.72rem;
    color: #988573;
    letter-spacing: 0.04em;
    min-height: 1.2rem;
}
"""

with gr.Blocks(title="Kitaru Chatbot") as demo:
    state = gr.State({})

    with gr.Row(equal_height=False):
        with gr.Column(scale=1, min_width=240, elem_id="side-rail"):
            gr.HTML(
                '<div id="brand">'
                '<div class="wordmark">kitaru<span class="accent">.</span></div>'
                '<div class="subtitle">Durable chatbot</div>'
                "</div>"
            )
            new_btn = gr.Button("Start a new chat", variant="primary")
            gr.HTML('<div class="rail-label">Recent sessions</div>')
            sessions = gr.Dropdown(
                choices=_list_sessions(),
                show_label=False,
                interactive=True,
                container=False,
            )
            refresh_btn = gr.Button("Refresh", variant="secondary", size="sm")

        with (
            gr.Column(scale=4, elem_id="chat-shell"),
            gr.Column(elem_id="chat-col"),
        ):
            chatbot_ui = gr.Chatbot(height=620, show_label=False, container=False)
            msg = gr.Textbox(
                placeholder="Message…",
                show_label=False,
                interactive=False,
                autofocus=True,
                elem_id="composer",
                container=False,
                lines=1,
                max_lines=5,
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
    demo.launch(css=CSS, theme=KITARU_THEME)
