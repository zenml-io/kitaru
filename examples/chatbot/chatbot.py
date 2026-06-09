"""Durable chatbot using Kitaru + PydanticAI.

The chatbot is one agent with a single ``say_and_wait`` tool. The LLM drives
the whole conversation: every time it wants to talk to the user it calls
``say_and_wait(message=...)``, which suspends the run via ``kitaru.wait`` and
returns whatever the user typed back. The agent stops calling the tool when
the conversation is over.

No turn loop, no manual per-turn bookkeeping — the KitaruAgent adapter
wraps model calls in checkpoints for replay. The ``say_and_wait`` tool itself
stays at flow scope because waits must be created outside checkpoints, and a
small explicit checkpoint helper saves the running ``history`` artifact so any
UI can rehydrate a session by loading the latest one.

Recommended workflow:

    # one-time deploy (rerun after editing this file)
    kitaru deploy chatbot.py:chatbot --tag prod --stack <remote-stack> --exclusive

    # invoke from anywhere
    from kitaru.client import KitaruClient
    KitaruClient().deployments.invoke(flow="chatbot", tag="prod")

For quick local interactive terminal testing without deploying,
``python chatbot.py`` runs the flow against the active stack. This direct mode
may block until the conversation finishes. For local non-interactive automation,
use ``drive_local.py`` so one actor runs the flow while another actor answers
pending waits.
"""

from dataclasses import dataclass, field
from uuid import uuid4

from pydantic_ai import Agent, RunContext

import kitaru
from kitaru import ImageSettings, flow
from kitaru.adapters.pydantic_ai import KitaruAgent, wait_for_input

try:
    from .history_artifacts import HISTORY_ARTIFACT_NAME
except ImportError:  # pragma: no cover - direct ``python chatbot.py`` execution
    from history_artifacts import HISTORY_ARTIFACT_NAME

CHATBOT_IMAGE = ImageSettings(
    requirements=["pydantic-ai", "openai"],
    # Injects the secret's keys (here: ``OPENAI_API_KEY``) into the runtime
    # environment of every checkpoint pod.
    secret_environment_from=["openai-creds"],
)

MODEL = "openai:gpt-4o-mini"
SYSTEM_PROMPT = (
    "You are a helpful, concise assistant. Talk to the user via the "
    "`say_and_wait` tool — pass your reply as the `message` argument and "
    "the user's next message will come back as the tool result. "
    "Open the conversation by greeting them warmly with `say_and_wait`. "
    "End the conversation gracefully when the user says bye/quit/exit: "
    "send one final `say_and_wait` goodbye, then stop calling the tool."
)

Message = dict[str, str]  # {"role": "user" | "assistant", "content": ...}
CHATBOT_SESSION_LABEL_METADATA_KEY = "chatbot_session_label"
CHATBOT_TURN_METADATA_KEY = "chatbot_turn"


@kitaru.checkpoint(cache=False)
def persist_history(history: list[Message]) -> None:
    """Save a snapshot of the conversation history as a versioned artifact."""
    kitaru.save(HISTORY_ARTIFACT_NAME, history)


@dataclass
class Conversation:
    """Per-run state threaded through the agent via PydanticAI deps."""

    history: list[Message] = field(default_factory=list)
    turn: int = 0


def chatbot_wait_metadata(*, session_label: str, turn: int) -> dict[str, str | int]:
    """Return metadata that lets local drivers find this session's pending wait."""
    return {
        CHATBOT_SESSION_LABEL_METADATA_KEY: session_label,
        CHATBOT_TURN_METADATA_KEY: turn,
    }


@flow(image=CHATBOT_IMAGE)
def chatbot(session_label: str | None = None) -> str:
    """Durable chatbot: the agent runs until it stops calling ``say_and_wait``."""
    session_label = session_label or f"chatbot-{uuid4().hex}"
    agent: Agent[Conversation, str] = Agent(
        MODEL,
        name="chatbot",
        system_prompt=SYSTEM_PROMPT,
        deps_type=Conversation,
        output_type=str,
    )

    @agent.tool
    def say_and_wait(ctx: RunContext[Conversation], message: str) -> str:
        """Send MESSAGE to the user and return whatever they reply.

        Call this every time you want to speak to the user; the tool result
        is the user's next message.
        """
        conv = ctx.deps
        conv.history.append({"role": "assistant", "content": message})
        persist_history(list(conv.history))

        user_reply = wait_for_input(
            schema=str,
            question=message,
            name=f"user_turn_{conv.turn}",
            timeout=3600,
            metadata=chatbot_wait_metadata(
                session_label=session_label,
                turn=conv.turn,
            ),
        )
        conv.turn += 1
        conv.history.append({"role": "user", "content": user_reply})
        persist_history(list(conv.history))
        return user_reply

    # ``say_and_wait`` opts out of the adapter's synthetic tool checkpoint so
    # the body can call ``wait_for_input`` (which must run at flow scope, not
    # inside a checkpoint). ``allow_sync_tool_body_waits=True`` keeps the
    # tool on the workflow thread so the wait is allowed.
    kitaru_agent = KitaruAgent(
        agent,
        tool_checkpoint_config_by_name={"say_and_wait": False},
        allow_sync_tool_body_waits=True,
    )

    conv = Conversation()
    return kitaru_agent.run_sync(
        "Begin the conversation by greeting the user.",
        deps=conv,
    ).output


def main() -> None:
    print(
        "\nStarting chatbot.py in direct interactive mode. "
        "This process waits until the conversation finishes.\n"
        "For local non-interactive automation, run drive_local.py instead; "
        "that script starts the flow in the background and submits input "
        "from the foreground process.\n"
    )
    handle = chatbot.run()
    handle.wait()
    print("\nConversation ended.")


if __name__ == "__main__":
    main()
