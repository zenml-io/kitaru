"""Gradio chat UI for the durable Kitaru chatbot.

On page load a new chatbot flow starts automatically and the bot's greeting
appears in the chat. Each Gradio turn maps to one Kitaru wait/resume cycle.

Returning users can paste an execution ID into the "Resume session" box to
reload a previous conversation — history is reconstructed from the
``save_history`` checkpoint artifacts stored by the flow.

Install Gradio, then run:
    uv add --dev gradio
    export OPENAI_API_KEY=sk-...
    uv run examples/chatbot/ui.py
"""

import re
import sys
import time
from pathlib import Path

import gradio as gr

from kitaru.client import ExecutionStatus, KitaruClient

_REPO_ROOT = str(Path(__file__).resolve().parents[2])
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from examples.chatbot.chatbot import Message, chatbot  # noqa: E402

POLL_INTERVAL = 0.5
POLL_TIMEOUT = 120

client = KitaruClient()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _poll(exec_id: str) -> tuple[str, str | None]:
    """Poll until WAITING or finished. Returns (question_or_reply, wait_id | None)."""
    deadline = time.time() + POLL_TIMEOUT
    while time.time() < deadline:
        ex = client.executions.get(exec_id)
        if ex.status == ExecutionStatus.WAITING and ex.pending_wait is not None:
            pw = ex.pending_wait
            return pw.question or "", pw.wait_id
        if ex.status.is_finished:
            arts = client.artifacts.list(exec_id)
            return str(arts[-1].load()) if arts else "", None
        time.sleep(POLL_INTERVAL)
    return "(timeout)", None


def _history_from_artifacts(exec_id: str) -> list[Message]:
    """Load the most recent 'history' artifact saved by chat_turn checkpoints."""
    arts = client.artifacts.list(exec_id)
    history_arts = [a for a in arts if a.name == "history"]
    if not history_arts:
        return []

    def _sort_key(a: object) -> int:
        m = re.search(r"(\d+)$", getattr(a, "producing_call", "") or "")
        return int(m.group(1)) if m else 0

    raw = sorted(history_arts, key=_sort_key)[-1].load()
    # Deserialize: each item may be a dict or a Message
    return [Message(**m) if isinstance(m, dict) else m for m in raw]


def _greeting_from_artifacts(exec_id: str) -> str:
    """Load the bot's greeting from the first model-request checkpoint artifact."""
    arts = client.artifacts.list(exec_id)
    model_arts = [a for a in arts if "model_request" in str(a.producing_call or "")]
    if not model_arts:
        return ""
    mr = model_arts[0].load()
    return mr.parts[0].content if getattr(mr, "parts", None) else str(mr)


def _to_gradio_history(
    greeting: str,
    messages: list[Message],
) -> list[tuple[str | None, str | None]]:
    """Convert a greeting + message list into Gradio (user, bot) pairs."""
    chat: list[tuple[str | None, str | None]] = [(None, greeting)] if greeting else []
    for i in range(0, len(messages) - 1, 2):
        chat.append((messages[i].content, messages[i + 1].content))
    return chat


# ---------------------------------------------------------------------------
# Gradio callbacks
# ---------------------------------------------------------------------------


def initialize() -> tuple[list[tuple], dict]:
    """Start a new chatbot flow and return the greeting as the first chat entry."""
    handle = chatbot.run()
    exec_id = handle.exec_id
    greeting, wait_id = _poll(exec_id)
    chat = [(None, greeting)] if greeting else []
    state = {"exec_id": exec_id, "wait_id": wait_id}
    return chat, state


def respond(
    message: str,
    history: list[tuple],
    state: dict,
) -> tuple[str, list[tuple], dict]:
    exec_id: str | None = state.get("exec_id")
    wait_id: str | None = state.get("wait_id")

    if not exec_id or not wait_id:
        return "", history, state  # not ready; shouldn't happen after initialize()

    client.executions.input(exec_id, wait=wait_id, value=message)
    client.executions.resume(exec_id)

    bot_reply, next_wait_id = _poll(exec_id)
    new_history = [*history, (message, bot_reply or "(done)")]

    if next_wait_id is None:
        new_state: dict = {}  # flow ended — next message starts a fresh run
    else:
        new_state = {"exec_id": exec_id, "wait_id": next_wait_id}

    return "", new_history, new_state


def resume_session(exec_id_input: str, state: dict) -> tuple[list[tuple], dict, str]:
    """Reload a previous conversation from its execution ID."""
    exec_id = exec_id_input.strip()
    if not exec_id:
        return [], state, "Enter an execution ID above."

    try:
        ex = client.executions.get(exec_id)
    except Exception as exc:
        return [], state, f"Could not load execution: {exc}"

    greeting = _greeting_from_artifacts(exec_id)
    messages = _history_from_artifacts(exec_id)
    chat = _to_gradio_history(greeting, messages)

    wait_id = None
    if ex.status == ExecutionStatus.WAITING and ex.pending_wait is not None:
        wait_id = ex.pending_wait.wait_id

    new_state = {"exec_id": exec_id, "wait_id": wait_id} if wait_id else {}
    status = f"Resumed {'(still active)' if wait_id else '(conversation ended)'}."
    return chat, new_state, status


# ---------------------------------------------------------------------------
# Layout
# ---------------------------------------------------------------------------

with gr.Blocks(title="Kitaru Chatbot") as demo:
    state = gr.State({})

    gr.Markdown("## Kitaru Chatbot")

    chatbot_ui = gr.Chatbot(height=500, show_label=False)
    msg = gr.Textbox(
        placeholder='Message — press Enter to send. Type "exit" to end.',
        show_label=False,
        autofocus=True,
    )

    with gr.Accordion("Resume a previous session", open=False):
        exec_id_input = gr.Textbox(
            label="Execution ID", placeholder="paste exec ID here"
        )
        resume_btn = gr.Button("Load session")
        resume_status = gr.Textbox(label="Status", interactive=False)
        resume_btn.click(
            resume_session,
            [exec_id_input, state],
            [chatbot_ui, state, resume_status],
        )

    msg.submit(respond, [msg, chatbot_ui, state], [msg, chatbot_ui, state])
    demo.load(initialize, outputs=[chatbot_ui, state])

if __name__ == "__main__":
    demo.launch()
