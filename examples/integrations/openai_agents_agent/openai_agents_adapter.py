"""Real OpenAI Agents SDK + Kitaru adapter example.

Story:
- A customer asks about order ORD-1007.
- The agent must call local tools to look up order status and shipping policy.
- Kitaru records durable checkpoints for model + tool calls.

Run:
    uv sync --extra local --extra openai-agents
    uv run kitaru init
    export OPENAI_API_KEY=sk-...
    uv run examples/integrations/openai_agents_agent/openai_agents_adapter.py
"""

import ast
import os
import re
from typing import Any

from agents import Agent, RunConfig, function_tool
from agents.items import ModelResponse

from kitaru import flow
from kitaru.adapters.openai_agents import (
    KitaruRunner,
    OpenAIRunRequest,
    OpenAIRunResult,
)
from kitaru.errors import KitaruAmbiguousFlowResultError

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
            "Lost parcel policy: if no scan for 7 days, open claim and offer immediate "
            "replacement or refund."
        ),
        "delivered": (
            "Delivered policy: confirm address and drop-off photo; if customer reports "
            "non-receipt, open theft/misdelivery claim."
        ),
    }
    return policies.get(
        topic,
        "General shipping policy: verify status, share ETA, and escalate to a human "
        "agent for account-specific exceptions.",
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
        name="customer_support_agent",
        instructions=(
            "You are a careful customer support assistant. "
            "Always call lookup_order first when an order id is present. "
            "Then call shipping_policy using the order status before giving an answer. "
            "In the final response, include: order status, ETA, "
            "policy summary, and next step."
        ),
        model=model,
        tools=[lookup_order, shipping_policy],
    )


def _extract_model_response_text(response: ModelResponse) -> str:
    for item in response.output:
        content = getattr(item, "content", None)
        if not isinstance(content, list):
            continue
        for part in content:
            text = getattr(part, "text", None)
            if isinstance(text, str) and text.strip():
                return text
    return str(response)


def _extract_final_output_from_envelope_text(text: str) -> str:
    match = re.search(r"final_output=(.+?)(?:\s\w+=|$)", text)
    if not match:
        return text
    raw = match.group(1).strip()
    try:
        parsed = ast.literal_eval(raw)
    except (ValueError, SyntaxError):
        return raw or text
    return str(parsed)


def _normalize_wait_output(value: Any) -> str:
    if isinstance(value, OpenAIRunResult):
        if value.status != "completed":
            raise RuntimeError(f"Expected completed run, got status={value.status!r}.")
        return str(value.final_output)
    if isinstance(value, ModelResponse):
        return _extract_model_response_text(value)
    text = str(value)
    return _extract_final_output_from_envelope_text(text)


def _run_once(checkpoint_strategy: str) -> str:
    agent = _build_agent()
    runner = KitaruRunner(
        agent,
        checkpoint_strategy=checkpoint_strategy,
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )

    @flow
    def support_flow(customer_message: str) -> str:
        result = runner.run_sync(OpenAIRunRequest.start(customer_message))
        if result.status != "completed":
            raise RuntimeError(f"Expected completed run, got status={result.status!r}.")
        return str(result.final_output)

    raw_output = support_flow.run(
        "Hi, where is order ORD-1007? I need this for a birthday gift. "
        "Please check the actual order status and your shipping delay policy "
        "before answering, then tell me what happens next."
    ).wait()
    return _normalize_wait_output(raw_output)


def main() -> None:
    _require_openai_api_key()

    model_label = os.getenv("OPENAI_AGENTS_MODEL", "gpt-5-nano")
    print(f"Using model: {model_label}")

    runner_call_output = _run_once("runner_call")
    print("\n=== runner_call strategy output ===")
    print(runner_call_output)

    if os.getenv("OPENAI_AGENTS_COMPARE_CALLS", "").lower() in {
        "1",
        "true",
        "yes",
    }:
        # `calls` strategy creates per-tool/per-model peer checkpoints with no
        # single sink, so `.wait()` raises `KitaruAmbiguousFlowResultError`
        # when it can't pick a single terminal artifact. The raised message
        # points at the per-checkpoint artifacts in the Kitaru UI /
        # `KitaruClient`, which is the right surface for `calls` flows.
        # Real execution failures (model error, tool failure, etc.) will not
        # match this specific subclass and will propagate normally.
        try:
            calls_output = _run_once("calls")
        except KitaruAmbiguousFlowResultError as error:
            print("\n=== calls strategy output ===")
            print(f"(per-checkpoint artifacts only; .wait() raised: {error})")
        else:
            print("\n=== calls strategy output ===")
            print(calls_output)


if __name__ == "__main__":
    main()
