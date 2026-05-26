"""Simple durable chatbot using Kitaru + PydanticAI.

The flow greets the user first, then suspends waiting for input. Each turn:
wait for input → chat_turn checkpoint (LLM reply + history saved as artifact) → wait.
Close your terminal and resume any time — conversation is fully durable.

Run:
    export OPENAI_API_KEY=sk-...
    uv run examples/chatbot/chatbot.py

To continue from another terminal:
    kitaru executions input <exec_id> --value '"your message"'
    kitaru executions resume <exec_id>

Type "exit", "quit", or "bye" to end the conversation.
"""

from typing import Any

from pydantic import BaseModel
from pydantic_ai import Agent
from pydantic_ai.messages import (
    ModelRequest,
    ModelResponse,
    TextPart,
    UserPromptPart,
)

import kitaru
from kitaru import checkpoint, flow
from kitaru.adapters.pydantic_ai import KitaruAgent

SYSTEM_PROMPT = "You are a helpful, concise assistant."
MODEL = "openai:gpt-4o-mini"
MAX_TURNS = 20
STOP_WORDS = {"exit", "quit", "bye", "/done", "done"}
GREETING_PROMPT = "Greet the user warmly but briefly. Ask how you can help."

_raw: Agent[None, str] | None = None
_agent: KitaruAgent | None = None


def _get_agents() -> tuple[Agent[None, str], KitaruAgent]:
    """Lazy-initialize agents so module import doesn't require OPENAI_API_KEY."""
    global _raw, _agent
    if _raw is None:
        _raw = Agent(
            MODEL, name="chatbot", system_prompt=SYSTEM_PROMPT, output_type=str
        )
        _agent = KitaruAgent(_raw)
    assert _agent is not None
    return _raw, _agent


class Message(BaseModel):
    role: str
    content: str


def _to_pydantic_history(messages: list[Message]) -> list[ModelRequest | ModelResponse]:
    history = []
    for m in messages:
        if m.role == "user":
            history.append(ModelRequest(parts=[UserPromptPart(content=m.content)]))
        else:
            history.append(ModelResponse(parts=[TextPart(m.content)]))
    return history


def _load(handle: Any) -> Any:
    """Materialize a @checkpoint output handle when used directly in a flow body."""
    load_fn = getattr(handle, "load", None)
    return load_fn() if callable(load_fn) else handle


@checkpoint
def chat_turn(user_message: str, history: list[Message]) -> str:
    """Generate a reply and save the updated conversation history as an artifact."""
    raw, _ = _get_agents()
    result = raw.run_sync(user_message, message_history=_to_pydantic_history(history))
    assistant_reply: str = result.output
    updated = [
        *history,
        Message(role="user", content=user_message),
        Message(role="assistant", content=assistant_reply),
    ]
    kitaru.save("history", updated)
    return assistant_reply


@flow
def chatbot(max_turns: int = MAX_TURNS) -> str:
    """Durable chatbot: greets the user, then waits for input each turn."""
    history: list[Message] = []

    # Bot greets first; KitaruAgent auto-opens a turn checkpoint
    _, agent = _get_agents()
    assistant_reply: str = agent.run_sync(GREETING_PROMPT).output

    for turn in range(max_turns):
        user_message: str = kitaru.wait(
            name=f"user_turn_{turn}",
            schema=str,
            question=assistant_reply,
            timeout=3600,
        )

        if user_message.strip().lower() in STOP_WORDS:
            break

        assistant_reply = _load(chat_turn(user_message, history))
        history = [
            *history,
            Message(role="user", content=user_message),
            Message(role="assistant", content=assistant_reply),
        ]

    return assistant_reply


def main() -> None:
    handle = chatbot.run()
    handle.wait()
    print("\nConversation ended.")


if __name__ == "__main__":
    main()
