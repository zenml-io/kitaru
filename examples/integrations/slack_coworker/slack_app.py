"""Slack front door for the durable ``coworker`` flow.

This is the bridge between Slack and Kitaru — the equivalent of the chatbot
example's ``ui.py``, but for Slack instead of a web UI. It runs in **Socket
Mode**, so you don't need a public URL or a tunnel: the app opens an outbound
WebSocket to Slack.

The bridge is deliberately thin:

* ``/coworker <goal>``  → ``deployments.invoke`` (acks within Slack's 3s window,
  then the flow runs for as long as the goal needs).
* a background poller watches the execution; when it suspends at an approval
  wait it posts Approve / Revise / Reject buttons into the thread.
* a button (or the revise modal) → ``executions.input``, which resumes the run.
* when the run finishes, the poller posts the outcome plus the CLI commands to
  inspect and replay it.

The pure helpers below are import-safe without ``slack_bolt`` installed so they
can be unit-tested; the Slack wiring is built lazily in ``build_app``.
"""

import contextlib
import json
import os
import re
import threading
import time
from typing import Any

from kitaru.client import ExecutionStatus, KitaruClient, PendingWait

# The slash command to listen for and the deployment it triggers. Defaults match
# the coworker flow; override via env to point at an existing deployment.
SLASH_COMMAND = os.environ.get("KITARU_COWORKER_COMMAND", "coworker")
FLOW_NAME = os.environ.get("KITARU_COWORKER_FLOW", "coworker")
DEPLOYMENT_TAG = os.environ.get("KITARU_COWORKER_TAG", "prod")
INPUT_KEY = os.environ.get("KITARU_COWORKER_INPUT_KEY", "request")

DECISION_ACTION_ID = "coworker_decision"
REVISE_CALLBACK_ID = "coworker_revise"
NOTES_BLOCK_ID = "notes_block"
NOTES_ACTION_ID = "notes_input"

POLL_INTERVAL = 2.0
# Slack section text has a hard 3000-character ceiling; stay safely under it.
_SLACK_TEXT_LIMIT = 2900


def _decision_value(exec_id: str, wait_id: str, decision: str) -> str:
    """Encode the routing info a button needs to resolve a wait."""
    return json.dumps({"exec_id": exec_id, "wait_id": wait_id, "decision": decision})


def _parse_decision_value(raw: str) -> dict[str, str]:
    """Decode a button/modal payload back into routing info."""
    data = json.loads(raw)
    return {
        "exec_id": str(data["exec_id"]),
        "wait_id": str(data["wait_id"]),
        "decision": str(data.get("decision", "")),
    }


def _truncate(text: str) -> str:
    """Trim text to Slack's section-block limit."""
    if len(text) <= _SLACK_TEXT_LIMIT:
        return text
    return text[: _SLACK_TEXT_LIMIT - 1] + "…"


def _to_slack_mrkdwn(text: str) -> str:
    """Convert common GitHub-style markdown to Slack mrkdwn.

    LLMs emit ``**bold**``, ``## headers`` and ``- bullets``; Slack renders
    ``*bold*``, has no headers, and uses ``•`` bullets.
    """
    text = re.sub(r"\*\*(.+?)\*\*", r"*\1*", text)  # **bold** -> *bold*
    text = re.sub(r"__(.+?)__", r"*\1*", text)  # __bold__ -> *bold*
    # Headers/bullets use [ \t] (not \s) so trailing newlines survive — \s would
    # swallow the blank lines between sections and merge them together.
    text = re.sub(r"(?m)^[ \t]{0,3}#{1,6}[ \t]*(.+?)[ \t]*$", r"*\1*", text)
    text = re.sub(r"(?m)^([ \t]*)[-*][ \t]+", r"\1• ", text)
    return text


_DECISION_LABEL = {
    "approve": "✅ Approved — resuming the run…",
    "reject": "🚫 Rejected",
    "revise": "✏️ Requested changes",
}


def resolved_blocks(
    blocks: list[dict[str, Any]], decision: str
) -> list[dict[str, Any]]:
    """Rebuild a card after a decision: keep the draft, drop the buttons.

    Replaces the header with the decision and keeps the section block(s) so the
    reviewer can still see what they acted on.
    """
    label = _DECISION_LABEL.get(decision, "Decision recorded")
    out: list[dict[str, Any]] = [
        {"type": "header", "text": {"type": "plain_text", "text": label[:150]}}
    ]
    out.extend(b for b in blocks if b.get("type") == "section")
    return out


def build_approval_blocks(exec_id: str, wait: PendingWait) -> list[dict[str, Any]]:
    """Build the Block Kit message that asks a human to decide on a draft."""
    question = _to_slack_mrkdwn(wait.question or "Approve this deliverable?")
    return [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "⏸️ Your coworker needs a decision"},
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": (
                        f"The run is *suspended* — zero compute while it waits on "
                        f"you. Resumes the instant you click. · `{exec_id}`"
                    ),
                }
            ],
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": _truncate(question)},
        },
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "style": "primary",
                    "text": {"type": "plain_text", "text": "Approve"},
                    # Slack requires a unique action_id per element in a message.
                    "action_id": f"{DECISION_ACTION_ID}_approve",
                    "value": _decision_value(exec_id, wait.wait_id, "approve"),
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Revise"},
                    "action_id": f"{DECISION_ACTION_ID}_revise",
                    "value": _decision_value(exec_id, wait.wait_id, "revise"),
                },
                {
                    "type": "button",
                    "style": "danger",
                    "text": {"type": "plain_text", "text": "Reject"},
                    "action_id": f"{DECISION_ACTION_ID}_reject",
                    "value": _decision_value(exec_id, wait.wait_id, "reject"),
                },
            ],
        },
    ]


def build_revise_modal(exec_id: str, wait_id: str) -> dict[str, Any]:
    """Build the modal that collects revision notes for a ``revise`` decision."""
    return {
        "type": "modal",
        "callback_id": REVISE_CALLBACK_ID,
        "private_metadata": json.dumps({"exec_id": exec_id, "wait_id": wait_id}),
        "title": {"type": "plain_text", "text": "Request changes"},
        "submit": {"type": "plain_text", "text": "Send notes"},
        "blocks": [
            {
                "type": "input",
                "block_id": NOTES_BLOCK_ID,
                "label": {"type": "plain_text", "text": "What should change?"},
                "element": {
                    "type": "plain_text_input",
                    "multiline": True,
                    "action_id": NOTES_ACTION_ID,
                },
            }
        ],
    }


def extract_modal_notes(view: dict[str, Any]) -> tuple[dict[str, str], str]:
    """Pull routing info and notes text out of a submitted revise modal."""
    meta = json.loads(view["private_metadata"])
    notes = view["state"]["values"][NOTES_BLOCK_ID][NOTES_ACTION_ID]["value"] or ""
    return (
        {"exec_id": str(meta["exec_id"]), "wait_id": str(meta["wait_id"])},
        notes,
    )


def format_outcome(outcome: str, exec_id: str) -> str:
    """Render the final outcome with CLI hints for inspecting and replaying."""
    return (
        f"{_truncate(_to_slack_mrkdwn(outcome))}\n\n"
        f"*Inspect & rewind* (`{exec_id}`):\n"
        f"• `kitaru executions get {exec_id}`  (lists the checkpoints)\n"
        f"• `kitaru executions logs {exec_id}`\n"
        f"• `kitaru executions replay {exec_id} --from <draft-checkpoint> "
        f'--args \'{{"model": "openai:gpt-4o"}}\'`'
    )


def _load_outcome(client: KitaruClient, exec_id: str) -> str:
    """Load the ``outcome`` artifact the flow saves on every terminal branch."""
    artifacts = client.artifacts.list(exec_id, name="outcome", limit=1)
    if artifacts:
        return str(artifacts[0].load())
    return "Run finished (no outcome artifact found)."


def build_app() -> tuple[Any, Any]:
    """Construct the Bolt app, register handlers, and start the poller.

    Imports of ``slack_bolt`` and the ``KitaruClient`` happen here (not at module
    import) so the helpers above stay usable without those dependencies.
    """
    from slack_bolt import App
    from slack_bolt.adapter.socket_mode import SocketModeHandler

    kitaru_client = KitaruClient()
    app = App(token=os.environ["SLACK_BOT_TOKEN"])

    # exec_id -> {"channel": str, "thread_ts": str, "posted": set[str]}
    tracked: dict[str, dict[str, Any]] = {}
    lock = threading.Lock()

    @app.command(f"/{SLASH_COMMAND}")
    def handle_command(ack: Any, command: dict[str, Any], client: Any) -> None:
        goal = command.get("text", "").strip()
        ack(f":hourglass_flowing_sand: On it — _{goal or 'no goal given'}_")
        if not goal:
            return
        # With an input key, pass the Slack text as that input; without one,
        # fall back to the deployment's baked-in default inputs.
        handle = kitaru_client.deployments.invoke(
            flow=FLOW_NAME,
            tag=DEPLOYMENT_TAG,
            inputs={INPUT_KEY: goal} if INPUT_KEY else None,
        )
        # Best-effort auto-join (needs the channels:join scope); harmless if the
        # bot is already a member or the scope is absent — otherwise invite it.
        with contextlib.suppress(Exception):
            client.conversations_join(channel=command["channel_id"])
        posted = client.chat_postMessage(
            channel=command["channel_id"],
            text=(
                f":rocket: Launched on Kitaru — execution `{handle.exec_id}` "
                f"running on the remote stack. I'll post here when it needs you."
            ),
        )
        with lock:
            tracked[handle.exec_id] = {
                "channel": command["channel_id"],
                "thread_ts": posted["ts"],
                "posted": set(),
            }

    @app.action(re.compile(rf"^{DECISION_ACTION_ID}_(approve|revise|reject)$"))
    def handle_decision(
        ack: Any, body: dict[str, Any], client: Any, respond: Any
    ) -> None:
        ack()
        route = _parse_decision_value(body["actions"][0]["value"])
        # Rebuild the card without buttons (keeps the draft visible) so the click
        # registers immediately and can't be double-submitted.
        original_blocks = body.get("message", {}).get("blocks", [])
        respond(
            replace_original=True,
            blocks=resolved_blocks(original_blocks, route["decision"]),
            text=_DECISION_LABEL.get(route["decision"], "Decision recorded"),
        )
        if route["decision"] == "revise":
            client.views_open(
                trigger_id=body["trigger_id"],
                view=build_revise_modal(route["exec_id"], route["wait_id"]),
            )
            return
        # Resume the run; suppress double-click races (wait already resolved).
        with contextlib.suppress(Exception):
            kitaru_client.executions.input(
                route["exec_id"],
                wait=route["wait_id"],
                value={"decision": route["decision"], "notes": ""},
            )

    @app.view(REVISE_CALLBACK_ID)
    def handle_revise(ack: Any, view: dict[str, Any]) -> None:
        ack()
        route, notes = extract_modal_notes(view)
        with contextlib.suppress(Exception):
            kitaru_client.executions.input(
                route["exec_id"],
                wait=route["wait_id"],
                value={"decision": "revise", "notes": notes},
            )

    def _poll_forever() -> None:
        while True:
            with lock:
                items = list(tracked.items())
            for exec_id, ctx in items:
                with contextlib.suppress(Exception):
                    # A transient poll error shouldn't kill the loop.
                    _sync_one(kitaru_client, app.client, exec_id, ctx, tracked, lock)
            time.sleep(POLL_INTERVAL)

    threading.Thread(target=_poll_forever, daemon=True).start()
    return app, SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"])


def _sync_one(
    kitaru_client: KitaruClient,
    slack: Any,
    exec_id: str,
    ctx: dict[str, Any],
    tracked: dict[str, dict[str, Any]],
    lock: threading.Lock,
) -> None:
    """Reconcile one tracked execution with Slack: post waits and the outcome."""
    ex = kitaru_client.executions.get(exec_id)

    if ex.status == ExecutionStatus.WAITING and ex.pending_wait is not None:
        wait = ex.pending_wait
        if wait.wait_id not in ctx["posted"]:
            slack.chat_postMessage(
                channel=ctx["channel"],
                thread_ts=ctx["thread_ts"],
                blocks=build_approval_blocks(exec_id, wait),
                text="A deliverable is ready for your review.",
            )
            ctx["posted"].add(wait.wait_id)
        return

    if ex.status.is_finished:
        slack.chat_postMessage(
            channel=ctx["channel"],
            thread_ts=ctx["thread_ts"],
            text=format_outcome(_load_outcome(kitaru_client, exec_id), exec_id),
        )
        with lock:
            tracked.pop(exec_id, None)


def main() -> None:
    """Run the Slack app (blocks on the Socket Mode connection)."""
    _, handler = build_app()
    handler.start()


if __name__ == "__main__":
    main()
