"""Durable chatbot using Kitaru + PydanticAI.

The chatbot is one agent with a single ``say_and_wait`` tool. The LLM drives
the whole conversation: every time it wants to talk to the user it calls
``say_and_wait(message=...)``, which suspends the run via ``kitaru.wait`` and
returns whatever the user typed back. The agent stops calling the tool when
the conversation is over.

No turn loop, no manual checkpoint boundaries, no per-turn bookkeeping — the
KitaruAgent adapter wraps each model + tool call in a synthetic checkpoint
for replay, and the tool body saves the running ``history`` artifact so any
UI can rehydrate a session by loading the latest one.

Recommended workflow:

    # one-time deploy (rerun after editing this file)
    kitaru deploy chatbot.py:chatbot --tag prod --stack <remote-stack> --exclusive

    # invoke from anywhere
    from kitaru.client import KitaruClient
    KitaruClient().deployments.invoke(flow="chatbot", tag="prod")

For quick local testing without deploying, ``python chatbot.py`` runs the
flow against the active stack.
"""

from dataclasses import dataclass, field

from pydantic_ai import Agent, RunContext

from kitaru import ImageSettings, flow
from kitaru.adapters.pydantic_ai import KitaruAgent, wait_for_input

# Faked, deterministic support tools are defined inline so the flow module is
# fully self-contained — it must import cleanly inside the built deployment image
# with no sibling files. The Replay Lab cohort has its own copy in
# ``support_tools.py`` for local use.
_STOCK = {
    "cordless drill": "in stock at the Riverside store (aisle 12)",
    "exterior wood stain": "in stock at the Riverside store (aisle 7)",
    "patio heater": "out of stock; available for delivery in 5-7 days",
}
_ORDERS = {
    "A1001": "shipped, arriving tomorrow",
    "A1002": "processing, not yet shipped",
    "A1003": "delivered on 2026-06-02",
}


def support_check_stock(product: str) -> str:
    """Deterministic stock status for a product (faked, read-only)."""
    key = product.strip().lower()
    return _STOCK.get(key, f"'{product}' is not in our catalog; please check the name")


def support_lookup_order(order_id: str) -> str:
    """Deterministic status for an order id (faked, read-only)."""
    return _ORDERS.get(order_id.strip().upper(), f"no order found with id '{order_id}'")


def support_issue_refund(order_id: str, amount: str) -> str:
    """Guarded refund (faked): always requires human verification."""
    return (
        "REFUND NOT PROCESSED: refunds require identity verification and a "
        "supervisor approval that this agent cannot perform. Escalate to a "
        "human support specialist; do not tell the customer the refund is done."
    )

CHATBOT_IMAGE = ImageSettings(
    # Pin to the pydantic-ai version Kitaru's adapter is built against; an
    # unpinned install pulls a version missing symbols the adapter imports.
    requirements=["pydantic-ai-slim==1.96.0", "openai"],
    # Injects the secret's keys (here: ``OPENAI_API_KEY``) into the runtime
    # environment of every checkpoint pod.
    secret_environment_from=["kami-openai"],
)

MODEL = "openai:gpt-4o-mini"
SYSTEM_PROMPT = (
    "You are a helpful, concise retail customer-support assistant for a "
    "home-improvement store. Talk to the user via the "
    "`say_and_wait` tool — pass your reply as the `message` argument and "
    "the user's next message will come back as the tool result. "
    "Use `check_stock` to answer product availability questions and "
    "`lookup_order` to answer order-status questions before replying. "
    "If a customer asks for a refund, call `issue_refund`; if it reports that "
    "the refund needs verification, do NOT tell the customer it is done — "
    "explain you are escalating to a human specialist. "
    "Open the conversation by greeting them warmly with `say_and_wait`. "
    "End the conversation gracefully when the user says bye/quit/exit: "
    "send one final `say_and_wait` goodbye, then stop calling the tool."
)

Message = dict[str, str]  # {"role": "user" | "assistant", "content": ...}


@dataclass
class Conversation:
    """Per-run state threaded through the agent via PydanticAI deps."""

    history: list[Message] = field(default_factory=list)
    turn: int = 0


@flow(image=CHATBOT_IMAGE)
def chatbot() -> str:
    """Durable chatbot: the agent runs until it stops calling ``say_and_wait``."""
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

        user_reply = wait_for_input(
            schema=str,
            question=message,
            name=f"user_turn_{conv.turn}",
            timeout=3600,
        )
        conv.turn += 1
        conv.history.append({"role": "user", "content": user_reply})
        return user_reply

    @agent.tool_plain
    def check_stock(product: str) -> str:
        """Look up deterministic stock availability for a product."""
        return support_check_stock(product)

    @agent.tool_plain
    def lookup_order(order_id: str) -> str:
        """Look up deterministic status for an order id."""
        return support_lookup_order(order_id)

    @agent.tool_plain
    def issue_refund(order_id: str, amount: str) -> str:
        """Attempt a refund. Returns a verification-required result (guarded)."""
        return support_issue_refund(order_id, amount)

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
    handle = chatbot.run()
    handle.wait()
    print("\nConversation ended.")


if __name__ == "__main__":
    main()
