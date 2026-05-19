"""Capability smoke test for the Kitaru PydanticAI adapter.

Each ``demo_*`` function exercises one feature of ``KitaruAgent`` using
``TestModel`` so the whole file runs without API keys.

Run:
    uv sync --extra local --extra pydantic-ai
    uv run examples/integrations/pydantic_ai_agent/pydantic_ai_adapter.py
"""

import uuid
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel
from pydantic_ai import Agent, RunContext
from pydantic_ai.exceptions import ApprovalRequired, CallDeferred
from pydantic_ai.models.test import TestModel

from kitaru import checkpoint, flow
from kitaru.adapters import pydantic_ai as kp
from kitaru.adapters.pydantic_ai import CapturePolicy, KitaruAgent, hitl_tool

# Fresh suffix per run so cached artifacts from prior runs don't collide.
_RUN_TAG = uuid.uuid4().hex[:8]


@dataclass
class ResearchDeps:
    source_priority: list[str]


class ResearchBrief(BaseModel):
    topic: str
    headline: str
    sources: list[str]


def _make_structured_agent() -> Agent[ResearchDeps, ResearchBrief]:
    """Structured output + a tool + deps; safe inside an explicit `@checkpoint`."""
    agent = Agent(
        TestModel(call_tools=[]),
        name=f"structured_researcher_{_RUN_TAG}",
        deps_type=ResearchDeps,
        output_type=ResearchBrief,
        instructions="Research the topic and cite a source.",
    )

    @agent.tool
    def search_index(ctx: RunContext[ResearchDeps], query: str) -> list[str]:
        return [
            f"https://example.com/{tag}/{query}" for tag in ctx.deps.source_priority
        ]

    return agent


def _make_str_agent(
    name: str, *, call_tools: list[str] | str = "all"
) -> Agent[ResearchDeps, str]:
    """``str`` output — auto-checkpoint friendly (no generic in the cache)."""
    agent = Agent(
        TestModel(call_tools=call_tools),
        name=f"{name}_{_RUN_TAG}",
        deps_type=ResearchDeps,
        output_type=str,
    )

    @agent.tool
    def search_index(ctx: RunContext[ResearchDeps], query: str) -> list[str]:
        return [f"https://example.com/{query}"]

    return agent


def demo_passthrough() -> ResearchBrief:
    """Inside an explicit ``@checkpoint`` the adapter is a pure passthrough."""
    researcher = KitaruAgent(_make_structured_agent())

    @checkpoint(type="llm_call")
    def run_research(topic: str) -> ResearchBrief:
        deps = ResearchDeps(source_priority=["wiki", "news"])
        return researcher.run_sync(f"Research {topic!r}.", deps=deps).output

    @flow
    def passthrough_flow(topic: str) -> ResearchBrief:
        return run_research(topic)

    return passthrough_flow.run("kitaru").wait()


def demo_auto_checkpoint() -> str:
    """Inside ``@flow`` without ``@checkpoint`` — adapter opens a turn checkpoint."""
    researcher = KitaruAgent(_make_str_agent("auto_cp_agent", call_tools=[]))
    deps = ResearchDeps(source_priority=["wiki"])

    @flow
    def auto_checkpoint_flow(topic: str) -> str:
        return researcher.run_sync(f"Research {topic!r}.", deps=deps).output

    return auto_checkpoint_flow.run("kitaru").wait()


def demo_auto_flow() -> str:
    """Outside any ``@flow`` — the adapter auto-opens one."""
    researcher = KitaruAgent(_make_str_agent("auto_flow_agent", call_tools=[]))
    deps = ResearchDeps(source_priority=["wiki"])
    return researcher.run_sync("Research 'kitaru'.", deps=deps).output


def demo_granular() -> str:
    """Granular mode: each model / tool / MCP call becomes its own checkpoint."""
    inner = Agent(
        TestModel(call_tools=[]), name=f"granular_agent_{_RUN_TAG}", output_type=str
    )

    @inner.tool_plain
    def lookup_price(sku: str) -> float:
        return 9.99

    researcher = KitaruAgent(
        inner,
        granular_checkpoints=True,
        model_checkpoint_config={"retries": 2},
        tool_checkpoint_config={"retries": 1},
        tool_checkpoint_config_by_name={
            "lookup_price": {"retries": 5},
        },
    )

    @flow
    def granular_flow(topic: str) -> str:
        return researcher.run_sync(f"Research {topic!r}.").output

    return granular_flow.run("kitaru").wait()


def demo_turn_retries() -> str:
    """``turn_checkpoint_config`` tunes the auto-opened turn checkpoint."""
    researcher = KitaruAgent(
        _make_str_agent("turn_retries_agent", call_tools=[]),
        turn_checkpoint_config={"retries": 2, "type": "llm_call"},
    )
    deps = ResearchDeps(source_priority=["wiki"])

    @flow
    def turn_retries_flow(topic: str) -> str:
        return researcher.run_sync(f"Research {topic!r}.", deps=deps).output

    return turn_retries_flow.run("kitaru").wait()


def demo_message_history() -> str:
    """``persist_message_history`` threads message history across turns."""
    chat_agent = Agent(
        TestModel(call_tools=[]), name=f"chat_{_RUN_TAG}", output_type=str
    )
    chatter = KitaruAgent(chat_agent, persist_message_history=True)

    @checkpoint
    def converse() -> str:
        first = chatter.run_sync("Hi, I am Alice.").output
        second = chatter.run_sync("What's my name?").output
        return f"{first!r} + {second!r}"

    @flow
    def message_history_flow() -> str:
        return converse()

    return message_history_flow.run().wait()


def demo_capture_policy() -> str:
    """Metadata-only tool capture + a per-tool override that drops capture entirely.

    The agent calls a tool, so without an enclosing ``@checkpoint`` the
    adapter would create three sibling checkpoints and the flow would have
    no single sink for ``.wait()``. Wrapping the run in ``research_brief``
    switches the adapter into passthrough mode: per-call work is recorded
    as child events under the outer checkpoint while the flow keeps one
    terminal output. The capture policy still applies to those child
    events.
    """
    policy = CapturePolicy(
        save_prompts=True,
        save_responses=True,
        tool_capture="metadata",
        tool_capture_overrides={"search_index": None},
    )
    researcher = KitaruAgent(_make_str_agent("capture_agent"), capture=policy)
    deps = ResearchDeps(source_priority=["wiki"])

    @checkpoint
    def research_brief(topic: str) -> str:
        return researcher.run_sync(f"Research {topic!r}.", deps=deps).output

    @flow
    def capture_policy_flow(topic: str) -> str:
        return research_brief(topic)

    return capture_policy_flow.run("kitaru").wait()


def demo_event_stream_handler() -> str:
    """Wire an ``event_stream_handler`` onto a ``KitaruAgent``.

    Construction-only: ``TestModel`` hits an upstream parts-manager
    ``IndexError`` when actually streamed, which doesn't reproduce with real
    providers.
    """

    async def handler(ctx: Any, stream: Any) -> None:
        async for _event in stream:
            pass

    inner = Agent(
        TestModel(call_tools=[]), name=f"stream_agent_{_RUN_TAG}", output_type=str
    )
    wrapped = KitaruAgent(inner, event_stream_handler=handler)
    return f"wired handler on {wrapped.name}"


@hitl_tool(question="Approve publishing this brief?", schema=bool)
def publish_brief_hitl(headline: str, sources: list[str]) -> str:
    return f"published: {headline} ({len(sources)} sources)"


def demo_hitl_wiring() -> str:
    """Wire explicit and native HITL patterns on one agent.

    ``@hitl_tool`` is the safest default because the adapter routes it outside
    granular tool checkpoints. Native ``ApprovalRequired`` / ``CallDeferred``
    tools that may wait should be opted out of tool checkpointing. Ordinary sync
    tools that call ``kp.wait_for_input(...)`` also need that opt-out so Kitaru
    can keep the wait out of the tool checkpoint and on the workflow thread.
    """
    agent = Agent(
        TestModel(call_tools=[]),
        name=f"hitl_demo_{_RUN_TAG}",
        output_type=str,
        tools=[publish_brief_hitl],
    )

    @agent.tool_plain
    def approval_required() -> str:
        raise ApprovalRequired("Confirm before proceeding.")

    @agent.tool_plain
    def deferred_side_effect() -> str:
        raise CallDeferred("Queued for async completion.")

    @agent.tool_plain
    def ask_human_directly(question: str = "What should happen next?") -> str:
        return kp.wait_for_input(schema=str, question=question)

    wired = KitaruAgent(
        agent,
        tool_checkpoint_config_by_name={
            "approval_required": False,
            "deferred_side_effect": False,
            "ask_human_directly": False,
        },
    )

    @flow
    def hitl_wiring_flow() -> str:
        return wired.run_sync("Draft a brief.").output

    return hitl_wiring_flow.run().wait()


def main() -> None:
    demos = [
        ("passthrough (structured output)", demo_passthrough),
        ("auto_checkpoint", demo_auto_checkpoint),
        ("auto_flow", demo_auto_flow),
        ("granular", demo_granular),
        ("turn_retries", demo_turn_retries),
        ("message_history", demo_message_history),
        ("capture_policy", demo_capture_policy),
        ("event_stream", demo_event_stream_handler),
        ("hitl_wiring", demo_hitl_wiring),
    ]
    failures: list[tuple[str, Exception]] = []
    for name, fn in demos:
        print(f"--- {name} ---")
        try:
            fn()
        except Exception as exc:
            failures.append((name, exc))
            print(f"  FAILED: {type(exc).__name__}: {exc}")
            continue
        print("  ok")

    if failures:
        print(f"\n{len(failures)} demo(s) failed:")
        for name, exc in failures:
            print(f"  - {name}: {type(exc).__name__}: {exc}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
