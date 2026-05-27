"""Durable chatbot using Kitaru + PydanticAI.

The flow opens with a ``greet`` checkpoint that produces the bot's first
message and seeds the conversation history. Each subsequent turn is
``wait(user input) → chat_turn(reply) → wait(...)``. Both checkpoints save
the **full** conversation as a single ``history`` artifact, so any UI can
rehydrate a session by loading the latest ``history`` artifact.

History is stored as plain ``dict[str, str]`` (``role``/``content``) so the
artifact can be deserialized from any import context (script, Gradio UI,
CLI) without depending on a project-specific class.

Recommended workflow — deploy the flow once, then invoke it from a UI or
CLI without holding a Python process open:

    # one-time deploy (rerun after editing this file)
    kitaru deploy chatbot.py:chatbot --tag prod --stack <remote-stack> --exclusive

    # invoke from anywhere via the Python client
    from kitaru.client import KitaruClient
    handle = KitaruClient().deployments.invoke(flow="chatbot", tag="prod")

    # … or interactively from the CLI
    kitaru invoke chatbot --tag prod

For quick local testing without deploying, ``python chatbot.py`` runs the
flow on the active stack. To continue a paused execution from a separate
terminal:

    kitaru executions input <exec_id> --value '"your message"'

Type "exit", "quit", or "bye" to end the conversation.
"""

from typing import Any

from pydantic_ai import Agent
from pydantic_ai.messages import (
    ModelRequest,
    ModelResponse,
    TextPart,
    UserPromptPart,
)

import kitaru
from kitaru import ImageSettings, checkpoint, flow

CHATBOT_IMAGE = ImageSettings(
    requirements=["pydantic-ai", "openai"],
    # Injects the secret's keys (here: ``OPENAI_API_KEY``) into the runtime
    # environment of every checkpoint pod.
    secret_environment_from=["openai-creds"],
)

SYSTEM_PROMPT = "You are a helpful, concise assistant."
MODEL = "openai:gpt-4o-mini"
MAX_TURNS = 50
STOP_WORDS = {"exit", "quit", "bye", "/done", "done"}
GREETING_PROMPT = "Greet the user warmly but briefly. Ask how you can help."

Message = dict[str, str]  # {"role": "user" | "assistant", "content": ...}


def _to_pydantic_history(messages: list[Message]) -> list[ModelRequest | ModelResponse]:
    history: list[ModelRequest | ModelResponse] = []
    for m in messages:
        if m["role"] == "user":
            history.append(ModelRequest(parts=[UserPromptPart(content=m["content"])]))
        else:
            history.append(ModelResponse(parts=[TextPart(m["content"])]))
    return history


def _load(handle: Any) -> Any:
    """Materialize a @checkpoint output handle when used directly in a flow body."""
    load_fn = getattr(handle, "load", None)
    return load_fn() if callable(load_fn) else handle


def _agent() -> Agent[None, str]:
    return Agent(MODEL, name="chatbot", system_prompt=SYSTEM_PROMPT, output_type=str)


@checkpoint(cache=False)
def greet() -> list[Message]:
    """Produce the opening assistant message and seed the conversation history.

    Caching is disabled because ``greet`` takes no inputs — without ``cache=False``,
    every new chat would reuse the very first greeting ever generated.
    """
    reply: str = _agent().run_sync(GREETING_PROMPT).output
    history: list[Message] = [{"role": "assistant", "content": reply}]
    kitaru.save("history", history)
    return history


@checkpoint
def chat_turn(user_message: str, history: list[Message]) -> list[Message]:
    """Reply to one user message and save the updated conversation history."""
    reply: str = (
        _agent()
        .run_sync(user_message, message_history=_to_pydantic_history(history))
        .output
    )
    updated: list[Message] = [
        *history,
        {"role": "user", "content": user_message},
        {"role": "assistant", "content": reply},
    ]
    kitaru.save("history", updated)
    return updated


@flow(image=CHATBOT_IMAGE)
def chatbot(max_turns: int = MAX_TURNS) -> str:
    """Durable chatbot: greets the user, then waits for input each turn."""
    history: list[Message] = _load(greet())

    for turn in range(max_turns):
        user_message: str = kitaru.wait(
            name=f"user_turn_{turn}",
            schema=str,
            question=history[-1]["content"],
            timeout=3600,
        )

        if user_message.strip().lower() in STOP_WORDS:
            break

        history = _load(chat_turn(user_message, history))

    return history[-1]["content"]


def main() -> None:
    handle = chatbot.run()
    handle.wait()
    print("\nConversation ended.")


if __name__ == "__main__":
    main()
