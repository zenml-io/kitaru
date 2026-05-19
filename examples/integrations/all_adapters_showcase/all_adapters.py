"""Run one customer-support scenario through every Kitaru framework adapter.

The point of this example is to show that Kitaru's durable-execution layer
(``@flow``, ``@checkpoint``, runner adapters, artifact tracking) is the *same*
regardless of which agent framework a team picks. The agent inside changes;
the orchestration boundary does not.

Scenario: a customer asks about a delayed order. The agent must look up the
order and apply the relevant shipping policy before answering.

Adapters exercised:
  1. PydanticAI         (kitaru.adapters.pydantic_ai.KitaruAgent)
  2. OpenAI Agents SDK  (kitaru.adapters.openai_agents.KitaruRunner)
  3. LangGraph          (kitaru.adapters.langgraph.KitaruGraphRunner, calls mode)
  4. Claude Agent SDK   (kitaru.adapters.claude_agent_sdk.KitaruClaudeRunner)

Run:
    uv sync --extra local \\
            --extra pydantic-ai \\
            --extra openai-agents \\
            --extra langgraph-openai \\
            --extra claude-agent-sdk
    uv run kitaru init
    export OPENAI_API_KEY=sk-...
    export ANTHROPIC_API_KEY=sk-ant-...
    uv run python examples/integrations/all_adapters_showcase/all_adapters.py
"""

import os
import time
from dataclasses import dataclass
from typing import Any, cast

import kitaru
from kitaru import flow

# ---------------------------------------------------------------------------
# Shared scenario
# ---------------------------------------------------------------------------

CUSTOMER_QUESTION = (
    "Hi, where is order ORD-1007? Please look up the actual order status, "
    "apply the relevant shipping policy, and tell me what happens next."
)

ORDERS: dict[str, dict[str, str]] = {
    "ORD-1007": {
        "status": "delayed_weather_hub",
        "eta": "2026-05-08",
        "last_scan": "Rotterdam Sort Center",
        "carrier": "PostNL",
    },
}

POLICIES: dict[str, str] = {
    "delayed_weather_hub": (
        "Weather delay policy: wait 48 hours after ETA before replacement. "
        "If still not delivered, offer free replacement or full refund."
    ),
    "default": (
        "General shipping policy: verify status, share ETA, "
        "and escalate to a human agent."
    ),
}


def _lookup_order(order_id: str) -> str:
    order = ORDERS.get(order_id)
    if order is None:
        return f"Order {order_id} not found."
    return (
        f"Order {order_id}: status={order['status']}, eta={order['eta']}, "
        f"last_scan={order['last_scan']}, carrier={order['carrier']}"
    )


def _shipping_policy(status: str) -> str:
    return POLICIES.get(status.strip().lower(), POLICIES["default"])


# ---------------------------------------------------------------------------
# Result type — one row in the final comparison table
# ---------------------------------------------------------------------------


@dataclass
class AdapterResult:
    adapter: str
    framework: str
    exec_id: str
    status: str
    checkpoints: int
    duration_s: float
    final_output: str
    note: str = ""


def _summarize_execution(exec_id: str) -> tuple[str, int]:
    """Return (status, checkpoint count) for the given execution id."""
    client = kitaru.KitaruClient()
    execution = client.executions.get(exec_id)
    return execution.status.value, len(execution.list_checkpoints())


def _truncate(text: str, limit: int = 220) -> str:
    text = " ".join(str(text).split())
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "…"


# ---------------------------------------------------------------------------
# 1. PydanticAI
# ---------------------------------------------------------------------------


def run_pydantic_ai() -> AdapterResult:
    from pydantic_ai import Agent

    from kitaru.adapters.pydantic_ai import KitaruAgent

    model = os.getenv("PYDANTIC_AI_MODEL", "openai:gpt-5-nano")

    agent: Agent[None, str] = Agent(
        model,
        name="pydantic_support_agent",
        output_type=str,
        instructions=(
            "You are a careful customer support assistant. "
            "Always call lookup_order first when an order id is present, "
            "then call shipping_policy using the returned order status, "
            "and only then write the final answer."
        ),
    )

    @agent.tool_plain
    def lookup_order(order_id: str) -> str:
        """Return the order's status, ETA, last scan, and carrier."""
        return _lookup_order(order_id)

    @agent.tool_plain
    def shipping_policy(status: str) -> str:
        """Return the support policy for a given order status."""
        return _shipping_policy(status)

    # Granular mode: KitaruAgent emits one Kitaru checkpoint per model
    # request and per tool call. The flow body returns the answer string
    # directly; `.wait()` finds the Kitaru-saved flow return value rather
    # than getting confused by the adapter's sibling checkpoints.
    wrapped = KitaruAgent(agent, granular_checkpoints=True)

    @flow
    def pydantic_ai_flow(question: str) -> str:
        return wrapped.run_sync(question).output

    started = time.monotonic()
    handle = pydantic_ai_flow.run(CUSTOMER_QUESTION)
    output = cast(str, handle.wait())
    duration = time.monotonic() - started

    status, checkpoints = _summarize_execution(handle.exec_id)
    return AdapterResult(
        adapter="PydanticAI",
        framework="pydantic-ai",
        exec_id=handle.exec_id,
        status=status,
        checkpoints=checkpoints,
        duration_s=duration,
        final_output=output,
    )


# ---------------------------------------------------------------------------
# 2. OpenAI Agents SDK
# ---------------------------------------------------------------------------


def run_openai_agents() -> AdapterResult:
    from agents import Agent, RunConfig, function_tool

    from kitaru.adapters.openai_agents import KitaruRunner, OpenAIRunRequest

    @function_tool
    def lookup_order(order_id: str) -> str:
        """Return the order's status, ETA, last scan, and carrier."""
        return _lookup_order(order_id)

    @function_tool
    def shipping_policy(status: str) -> str:
        """Return the support policy for a given order status."""
        return _shipping_policy(status)

    agent = Agent(
        name="openai_support_agent",
        instructions=(
            "You are a careful customer support assistant. "
            "Always call lookup_order first, then shipping_policy with the "
            "returned status, then write the final answer."
        ),
        model=os.getenv("OPENAI_AGENTS_MODEL", "gpt-5-nano"),
        tools=[lookup_order, shipping_policy],
    )

    runner = KitaruRunner(
        agent,
        checkpoint_strategy="calls",
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )

    @flow
    def openai_agents_flow(question: str) -> str:
        # `calls` strategy: one Kitaru checkpoint per model + tool call.
        # The flow body returns the answer string directly; `.wait()` picks
        # up the Kitaru-saved return value rather than tripping over the
        # adapter's sibling per-call checkpoints.
        result = runner.run_sync(OpenAIRunRequest.start(question))
        status = getattr(result, "status", None)
        if status != "completed":
            raise RuntimeError(
                f"OpenAI Agents run did not complete (status={status!r})."
            )
        return str(result.final_output)

    started = time.monotonic()
    handle = openai_agents_flow.run(CUSTOMER_QUESTION)
    output = cast(str, handle.wait())
    duration = time.monotonic() - started

    status, checkpoints = _summarize_execution(handle.exec_id)
    return AdapterResult(
        adapter="OpenAI Agents",
        framework="openai-agents",
        exec_id=handle.exec_id,
        status=status,
        checkpoints=checkpoints,
        duration_s=duration,
        final_output=output,
    )


# ---------------------------------------------------------------------------
# 3. LangGraph (calls strategy with a LangChain agent)
# ---------------------------------------------------------------------------


def run_langgraph() -> AdapterResult:
    from langchain.agents import create_agent
    from langchain_openai import ChatOpenAI
    from langgraph.checkpoint.memory import InMemorySaver

    from kitaru.adapters.langgraph import KitaruGraphRunner, LangGraphRunRequest
    from kitaru.adapters.langgraph.langchain import KitaruLangGraphMiddleware

    def lookup_order_tool(order_id: str) -> str:
        """Return the order's status, ETA, last scan, and carrier."""
        return _lookup_order(order_id)

    def shipping_policy_tool(status: str) -> str:
        """Return the support policy for a given order status."""
        return _shipping_policy(status)

    runner_name = "langgraph_support_agent"
    model = ChatOpenAI(model=os.getenv("LANGGRAPH_AGENT_MODEL", "gpt-5-nano"))
    lc_agent = create_agent(
        model=model,
        tools=[lookup_order_tool, shipping_policy_tool],
        middleware=[KitaruLangGraphMiddleware(graph_name=runner_name)],
        checkpointer=InMemorySaver(),
        name=runner_name,
        system_prompt=(
            "You are a careful customer support assistant. "
            "Always call lookup_order_tool first, then shipping_policy_tool with "
            "the returned status, then write the final answer including order "
            "status, ETA, policy summary, and the next step."
        ),
    )

    runner = KitaruGraphRunner(
        lc_agent,
        name=runner_name,
        checkpoint_strategy="calls",
    )

    @flow
    def langgraph_flow(question: str) -> str:
        # `calls` strategy: one Kitaru checkpoint per LangChain model + tool
        # call, plus a final consolidating `langgraph_summary` checkpoint.
        result = runner.invoke(
            LangGraphRunRequest.start(
                {"messages": [{"role": "user", "content": question}]},
                thread_id=f"showcase-{int(time.time())}",
            )
        )
        status = getattr(result, "status", None)
        if status != "completed":
            raise RuntimeError(f"LangGraph run did not complete (status={status!r}).")
        output = cast(dict[str, Any], result.output)
        messages = output.get("messages") or []
        final = messages[-1] if messages else None
        return str(getattr(final, "content", final))

    started = time.monotonic()
    handle = langgraph_flow.run(CUSTOMER_QUESTION)
    output = cast(str, handle.wait())
    duration = time.monotonic() - started

    status, checkpoints = _summarize_execution(handle.exec_id)
    return AdapterResult(
        adapter="LangGraph",
        framework="langgraph",
        exec_id=handle.exec_id,
        status=status,
        checkpoints=checkpoints,
        duration_s=duration,
        final_output=output,
        note="checkpoint_strategy='calls'",
    )


# ---------------------------------------------------------------------------
# 4. Claude Agent SDK
# ---------------------------------------------------------------------------


def run_claude_sdk() -> AdapterResult:
    from claude_agent_sdk import ClaudeAgentOptions

    from kitaru.adapters.claude_agent_sdk import ClaudeRunRequest, KitaruClaudeRunner

    # The Claude Agent SDK is intentionally exercised in tool-free mode so the
    # example runs the same way on every machine. We bake the order facts into
    # the prompt; what we are showcasing here is the *adapter*, not the SDK's
    # built-in tools.
    order_text = _lookup_order("ORD-1007")
    policy_text = _shipping_policy("delayed_weather_hub")
    prompt = (
        f"A customer asks: {CUSTOMER_QUESTION}\n\n"
        f"Order data: {order_text}\n"
        f"Shipping policy: {policy_text}\n\n"
        "Write a short final answer for the customer that names the status, "
        "the ETA, the policy summary, and the next step. Do not use tools."
    )

    runner = KitaruClaudeRunner(
        name="claude_support_agent",
        options_factory=lambda request: ClaudeAgentOptions(
            allowed_tools=[],
            cwd=request.cwd,
            resume=request.resume_session_id,
            max_turns=request.max_turns,
        ),
        checkpoint_config={"cache": False},
    )

    @flow
    def claude_sdk_flow(prompt_text: str) -> str:
        # The runner produces one Kitaru checkpoint per invocation and
        # materializes its result back into flow-body scope.
        result = runner.run_sync(
            ClaudeRunRequest.start(
                prompt_text,
                cwd=os.getcwd(),
                max_turns=1,
                metadata={"showcase": "all_adapters"},
            )
        )
        # Attribute access rather than isinstance: on remote stacks the
        # ClaudeRunResult class is re-imported with a fresh identity
        # (see kitaru issue #348).
        final_text = getattr(result, "final_text", None)
        if final_text is None:
            raise TypeError(f"Unexpected Claude result: {type(result).__name__}")
        return final_text or "(empty)"

    started = time.monotonic()
    handle = claude_sdk_flow.run(prompt)
    output = cast(str, handle.wait())
    duration = time.monotonic() - started

    status, checkpoints = _summarize_execution(handle.exec_id)
    return AdapterResult(
        adapter="Claude Agent SDK",
        framework="claude-agent-sdk",
        exec_id=handle.exec_id,
        status=status,
        checkpoints=checkpoints,
        duration_s=duration,
        final_output=output,
        note="tool-free invocation; facts injected in prompt",
    )


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


def _require_keys() -> None:
    missing = [
        name for name in ("OPENAI_API_KEY", "ANTHROPIC_API_KEY") if not os.getenv(name)
    ]
    if missing:
        raise SystemExit(
            "Missing required credentials: "
            + ", ".join(missing)
            + ".\nExport both keys and rerun."
        )


def _print_table(results: list[AdapterResult]) -> None:
    headers = ["Adapter", "Framework", "Exec ID", "Status", "Ckpts", "Took", "Notes"]
    rows: list[list[str]] = [headers]
    for result in results:
        rows.append(
            [
                result.adapter,
                result.framework,
                result.exec_id[:8],
                result.status,
                str(result.checkpoints),
                f"{result.duration_s:.1f}s",
                result.note or "-",
            ]
        )

    widths = [max(len(row[col]) for row in rows) for col in range(len(headers))]

    def render(row: list[str]) -> str:
        return "  ".join(cell.ljust(widths[col]) for col, cell in enumerate(row))

    bar = "  ".join("-" * w for w in widths)
    print()
    print(render(rows[0]))
    print(bar)
    for row in rows[1:]:
        print(render(row))


def _print_final_outputs(results: list[AdapterResult]) -> None:
    print("\nFinal outputs:")
    for result in results:
        print(f"\n[{result.adapter}]  exec_id={result.exec_id}")
        print(f"  {_truncate(result.final_output)}")


def main() -> None:
    _require_keys()

    adapters = [
        ("PydanticAI", run_pydantic_ai),
        ("OpenAI Agents", run_openai_agents),
        ("LangGraph", run_langgraph),
        ("Claude Agent SDK", run_claude_sdk),
    ]

    results: list[AdapterResult] = []
    failures: list[tuple[str, Exception]] = []
    for name, fn in adapters:
        print(f"\n=== Running {name} adapter ===")
        try:
            result = fn()
        except Exception as exc:
            failures.append((name, exc))
            print(f"  FAILED: {type(exc).__name__}: {exc}")
            continue
        results.append(result)
        print(
            f"  ok  exec_id={result.exec_id[:8]} "
            f"status={result.status} checkpoints={result.checkpoints}"
        )

    if results:
        _print_table(results)
        _print_final_outputs(results)

    if failures:
        print(f"\n{len(failures)} adapter(s) failed:")
        for name, exc in failures:
            print(f"  - {name}: {type(exc).__name__}: {exc}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
