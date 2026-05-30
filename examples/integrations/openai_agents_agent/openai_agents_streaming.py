"""Stream OpenAI Agents SDK live events through Kitaru.

Story:
- A customer asks about order ORD-1007.
- The agent calls local tools, then writes the final support answer.
- Kitaru forwards useful OpenAI stream updates while the runner-call checkpoint
  is active, then saves the final OpenAIRunResult as the durable record.

Run:
    uv sync --extra local --extra openai-agents
    uv run kitaru init
    export OPENAI_API_KEY=sk-...
    uv run examples/integrations/openai_agents_agent/openai_agents_streaming.py
"""

import os
import threading
from typing import Any

from agents import Agent, RunConfig, function_tool

from kitaru import flow
from kitaru.adapters.openai_agents import (
    OPENAI_STREAM_EVENT_KINDS,
    OPENAI_STREAM_TERMINAL_EVENT_KINDS,
    KitaruRunner,
    OpenAIRunRequest,
    OpenAIRunResult,
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


@function_tool
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


@function_tool
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


def _require_openai_api_key() -> None:
    if os.getenv("OPENAI_API_KEY"):
        return
    raise SystemExit(
        "Missing OPENAI_API_KEY.\n"
        "Set it first, then rerun:\n"
        "  export OPENAI_API_KEY='sk-...'"
    )


def _build_agent() -> Agent:
    model = os.getenv("OPENAI_AGENTS_MODEL", "gpt-5-nano")
    return Agent(
        name="streaming_customer_support_agent",
        instructions=(
            "You are a careful customer support assistant. "
            "Always call lookup_order first when an order id is present. "
            "Then call shipping_policy using the order status before giving an "
            "answer. In the final response, include: order status, ETA, "
            "policy summary, and next step."
        ),
        model=model,
        tools=[lookup_order, shipping_policy],
    )


def _tracing_disabled() -> bool:
    return os.getenv("OPENAI_AGENTS_ENABLE_TRACING", "").lower() not in {
        "1",
        "true",
        "yes",
    }


def _event_data(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data")
    return data if isinstance(data, dict) else {}


def _watch_openai_stream(exec_id: str, stop_event: threading.Event) -> None:
    print("\n=== live OpenAI stream events ===")
    try:
        for event in KitaruClient().executions.events(
            exec_id,
            kinds=list(OPENAI_STREAM_EVENT_KINDS),
        ):
            if stop_event.is_set():
                return
            data = _event_data(event.payload)
            display = data.get("display") or event.kind
            category = data.get("category")
            prefix = f"[{category}] " if isinstance(category, str) else ""
            print(f"- {prefix}{display}")
            if event.kind in OPENAI_STREAM_TERMINAL_EVENT_KINDS:
                return
    except (KitaruBackendError, KitaruFeatureNotAvailableError) as error:
        print("\nLive event watching is unavailable on this backend.")
        print(f"The durable result will still be read with .wait(): {error}")


def _print_final_result(result: OpenAIRunResult) -> None:
    print("\n=== durable OpenAIRunResult ===")
    print(f"status: {result.status}")
    if result.usage:
        print(f"usage: {result.usage}")
    if result.status == "interrupted":
        print(f"interruptions: {len(result.interruptions)}")
        return
    print("final output:")
    print(result.final_output)


def main() -> None:
    _require_openai_api_key()
    model_label = os.getenv("OPENAI_AGENTS_MODEL", "gpt-5-nano")
    print(f"Using model: {model_label}")

    runner = KitaruRunner(
        _build_agent(),
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: RunConfig(tracing_disabled=_tracing_disabled()),
    )

    @flow
    def support_flow(customer_message: str) -> OpenAIRunResult:
        # The events are live progress. This final OpenAIRunResult is the value
        # Kitaru saves durably after the OpenAI stream finishes.
        return runner.run_stream_sync(OpenAIRunRequest.start(customer_message))

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
        target=_watch_openai_stream,
        args=(exec_id, stop_watching),
        daemon=True,
    )
    watcher.start()

    wait_value = handle.wait()
    stop_watching.set()
    watcher.join(timeout=1.0)
    if watcher.is_alive():
        print("\nLive watcher is still open; showing the durable result now.")

    if isinstance(wait_value, OpenAIRunResult):
        result = wait_value
    else:
        model_dump = getattr(wait_value, "model_dump", None)
        result = OpenAIRunResult.model_validate(
            model_dump(mode="python") if callable(model_dump) else wait_value
        )
    _print_final_result(result)


if __name__ == "__main__":
    main()
