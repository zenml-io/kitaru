"""Stream PydanticAI live events through Kitaru.

Story:
- A customer asks about order ORD-1007.
- The agent calls local tools, then writes the final support answer.
- Kitaru forwards useful PydanticAI stream updates while the agent-turn
  checkpoint is active, then saves the final answer as the durable record.

Run:
    uv sync --extra local --extra pydantic-ai --extra openai
    uv run kitaru init
    export OPENAI_API_KEY=sk-...
    uv run python examples/integrations/pydantic_ai_agent/pydantic_ai_streaming.py
"""

import os
import threading
from typing import Any

from pydantic_ai import Agent, RunContext

from kitaru import flow
from kitaru.adapters.pydantic_ai import (
    PYDANTIC_AI_STREAM_EVENT_KINDS,
    PYDANTIC_AI_STREAM_TERMINAL_EVENT_KINDS,
    KitaruAgent,
)
from kitaru.client import KitaruClient
from kitaru.errors import KitaruBackendError, KitaruFeatureNotAvailableError

ORDERS: dict[str, dict[str, str]] = {
    "ORD-1007": {
        "status": "delayed_weather_hub",
        "eta": "2026-05-08",
        "last_scan": "Rotterdam Sort Center",
        "carrier": "PostNL",
    },
    "ORD-1042": {
        "status": "delivered",
        "eta": "2026-05-02",
        "last_scan": "Customer mailbox",
        "carrier": "PostNL",
    },
}


def _require_openai_api_key() -> None:
    if os.getenv("OPENAI_API_KEY"):
        return
    raise SystemExit(
        "Missing OPENAI_API_KEY.\n"
        "Set it first, then rerun:\n"
        "  export OPENAI_API_KEY='sk-...'"
    )


def _build_agent() -> Agent[None, str]:
    model = os.getenv("PYDANTIC_AI_MODEL", "openai:gpt-5-nano")
    agent = Agent(
        model,
        name="streaming_customer_support_agent",
        output_type=str,
        instructions=(
            "You are a careful customer support assistant. "
            "Always call lookup_order first when an order id is present. "
            "Then call shipping_policy using the order status before giving an "
            "answer. In the final response, include: order status, ETA, "
            "policy summary, and next step."
        ),
    )

    @agent.tool_plain
    def lookup_order(order_id: str) -> str:
        """Return order status details for a known order id."""
        order = ORDERS.get(order_id)
        if order is None:
            return (
                f"Order {order_id} was not found. "
                "Ask for a valid order id in ORD-xxxx format."
            )
        return (
            f"Order {order_id}: status={order['status']}, eta={order['eta']}, "
            f"last_scan={order['last_scan']}, carrier={order['carrier']}"
        )

    @agent.tool_plain
    def shipping_policy(status_or_issue: str) -> str:
        """Return support policy guidance for shipment statuses/issues."""
        topic = status_or_issue.strip().lower()
        policies = {
            "delayed_weather_hub": (
                "Weather delay policy: wait 48 hours after ETA before replacement. "
                "If still not delivered, offer free replacement or full refund."
            ),
            "lost": (
                "Lost parcel policy: if no scan for 7 days, open claim and offer "
                "immediate replacement or refund."
            ),
            "delivered": (
                "Delivered policy: confirm address and drop-off photo; if customer "
                "reports non-receipt, open theft/misdelivery claim."
            ),
        }
        return policies.get(
            topic,
            "General shipping policy: verify status, share ETA, and escalate to a "
            "human agent for account-specific exceptions.",
        )

    return agent


async def _drain_stream(_ctx: RunContext[None], stream: Any) -> None:
    """Consume upstream events so Kitaru can publish normalized live events."""
    async for _event in stream:
        pass


def _event_data(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data")
    return data if isinstance(data, dict) else {}


def _watch_pydantic_ai_stream(exec_id: str, stop_event: threading.Event) -> None:
    print("\n=== live PydanticAI stream events ===")
    try:
        for event in KitaruClient().executions.events(
            exec_id,
            kinds=list(PYDANTIC_AI_STREAM_EVENT_KINDS),
        ):
            if stop_event.is_set():
                return
            data = _event_data(event.payload)
            display = data.get("display") or event.kind
            category = data.get("category")
            prefix = f"[{category}] " if isinstance(category, str) else ""
            print(f"- {prefix}{display}")
            if event.kind in PYDANTIC_AI_STREAM_TERMINAL_EVENT_KINDS:
                return
    except (KitaruBackendError, KitaruFeatureNotAvailableError) as error:
        print("\nLive event watching is unavailable on this backend.")
        print(f"The durable result will still be read with .wait(): {error}")


def main() -> None:
    _require_openai_api_key()
    model_label = os.getenv("PYDANTIC_AI_MODEL", "openai:gpt-5-nano")
    print(f"Using model: {model_label}")

    support_agent = KitaruAgent(
        _build_agent(),
        event_stream_handler=_drain_stream,
        checkpoint_strategy="turn",
    )

    @flow
    def support_flow(customer_message: str) -> str:
        return support_agent.run_sync(customer_message).output

    handle = support_flow.run(
        "Hi, where is order ORD-1007? I need this for a birthday gift. "
        "Please check the actual order status and your shipping delay policy "
        "before answering, then tell me what happens next.",
        cache=False,
    )
    exec_id = handle.exec_id
    print(f"Submitted execution: {exec_id}")

    stop_watching = threading.Event()
    watcher = threading.Thread(
        target=_watch_pydantic_ai_stream,
        args=(exec_id, stop_watching),
        daemon=True,
    )
    watcher.start()

    final_answer = handle.wait()
    stop_watching.set()
    watcher.join(timeout=1.0)
    if watcher.is_alive():
        print("\nLive watcher is still open; showing the durable result now.")

    print("\n=== durable final answer ===")
    print(final_answer)


if __name__ == "__main__":
    main()
