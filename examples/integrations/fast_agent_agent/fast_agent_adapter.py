"""Provider-free fast-agent + Kitaru adapter example.

Run:
    uv sync --extra fast-agent --no-dev
    uv run kitaru init
    uv run python examples/integrations/fast_agent_agent/fast_agent_adapter.py

The example uses real fast-agent AgentApp/ToolAgent objects, a local Python
``uppercase`` tool, and an in-memory LLM. No provider API key is needed.
"""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

from fast_agent.agents.agent_types import AgentConfig
from fast_agent.agents.tool_agent import ToolAgent
from fast_agent.core.agent_app import AgentApp
from fast_agent.mcp import prompt_message_extended as prompt_message_extended_module
from fast_agent.types import (
    LlmStopReason,
    PromptMessageExtended,
    RequestParams,
    text_content,
)
from mcp.types import CallToolRequest, CallToolRequestParams

from kitaru import flow
from kitaru._client._models import ExecutionStatus
from kitaru.adapters.fast_agent import KitaruFastAgent
from kitaru.client import KitaruClient

if not hasattr(prompt_message_extended_module, "PromptMessageExtended"):
    prompt_message_extended_module.PromptMessageExtended = PromptMessageExtended


@dataclass
class MemoryTurnUsage:
    """Provider-free usage turn captured by the example LLM."""

    provider: str
    model: str
    input_tokens: int
    output_tokens: int
    total_tokens: int
    raw_usage: dict[str, Any]
    tool_calls: int = 0

    @property
    def display_input_tokens(self) -> int:
        return self.input_tokens


@dataclass
class MemoryUsageAccumulator:
    """Small accumulator with the same ``turns`` shape fast-agent exposes."""

    turns: list[MemoryTurnUsage]

    @property
    def current_context_tokens(self) -> int:
        return self.turns[-1].total_tokens if self.turns else 0

    @property
    def context_usage_percentage(self) -> float | None:
        return None

    def count_tools(self, tool_calls: int) -> None:
        if self.turns:
            self.turns[-1].tool_calls = tool_calls


@dataclass(frozen=True)
class FastAgentDemoResult:
    """Small serializable summary printed by the example."""

    model_reply: str
    app_tool_reply: str
    direct_tool_reply: str
    model_generate_calls: int
    note: str


class ManualFastAgentRun:
    """Tiny stand-in for ``FastAgent.run()`` around a prepared AgentApp.

    The public adapter wraps objects yielded by a fast-agent run context. This
    class keeps the example provider-free while preserving that same shape:
    ``KitaruFastAgent`` still receives an object with an async ``run()`` context
    manager, and that context manager still yields a real ``AgentApp``.
    """

    def __init__(self, app: AgentApp) -> None:
        self._app = app

    @asynccontextmanager
    async def run(self) -> AsyncIterator[AgentApp]:
        yield self._app


class MemoryLLM:
    """No-network LLM that returns real fast-agent message objects."""

    model_name = "memory-fast-agent-demo"
    provider = "memory"
    resolved_model = None

    def __init__(self) -> None:
        self.usage_accumulator = MemoryUsageAccumulator(turns=[])
        self.default_request_params = RequestParams(use_history=True)
        self.instruction = ""
        self.generate_calls = 0
        self._tool_call_requested = False

    def get_request_params(self, request_params: Any | None = None) -> Any:
        return request_params or self.default_request_params

    def request_tool_call_once(self) -> None:
        """Make the next model response ask fast-agent to call ``uppercase``."""
        self._tool_call_requested = True

    async def generate(
        self,
        messages: list[Any],
        request_params: Any | None = None,
        tools: list[Any] | None = None,
    ) -> PromptMessageExtended:
        del request_params, tools
        self.generate_calls += 1
        if self._tool_call_requested:
            self._tool_call_requested = False
            response = self._assistant_tool_call("uppercase", {"text": "kitaru"})
        elif any(getattr(message, "tool_results", None) for message in messages):
            response = self._assistant_message("memory tool loop complete")
        else:
            last_user_text = messages[-1].last_text() if messages else "<empty>"
            response = self._assistant_message(f"memory reply to {last_user_text}")
        self._record_usage(messages, response)
        return response

    async def structured(
        self,
        messages: list[Any],
        model: type[object],
        request_params: Any | None = None,
    ) -> tuple[None, PromptMessageExtended]:
        del model, request_params
        response = self._assistant_message("structured memory reply")
        self._record_usage(messages, response)
        return None, response

    def _record_usage(
        self,
        messages: list[Any],
        response: PromptMessageExtended,
    ) -> None:
        input_text = "\n".join(
            text
            for message in messages
            if callable(last_text := getattr(message, "last_text", None))
            and isinstance((text := last_text()), str)
        )
        output_text = response.last_text() or ""
        input_tokens = _wordish_count(input_text)
        output_tokens = _wordish_count(output_text)
        payload = {
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "total_tokens": input_tokens + output_tokens,
            "unit": "wordish_count",
            "source": "example_memory_llm",
        }
        self.usage_accumulator.turns.append(
            MemoryTurnUsage(
                provider=self.provider,
                model=self.model_name,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                total_tokens=input_tokens + output_tokens,
                raw_usage=payload,
            )
        )

    def _assistant_message(self, text: str) -> PromptMessageExtended:
        return PromptMessageExtended(
            role="assistant",
            content=[text_content(text)],
        )

    def _assistant_tool_call(
        self, name: str, arguments: dict[str, Any]
    ) -> PromptMessageExtended:
        return PromptMessageExtended(
            role="assistant",
            content=[text_content("calling local tool")],
            tool_calls={
                "call-1": CallToolRequest(
                    method="tools/call",
                    params=CallToolRequestParams(name=name, arguments=arguments),
                ),
            },
            stop_reason=LlmStopReason.TOOL_USE,
        )


def _wordish_count(text: str) -> int:
    """Return a deterministic local token-like count for the memory LLM."""
    return len([part for part in text.split() if part])


def uppercase(text: str) -> str:
    """Local fast-agent tool used by the memory LLM."""
    return text.upper()


def _content_text(result: Any) -> str:
    return "\n".join(
        text
        for item in result.content
        if isinstance((text := getattr(item, "text", None)), str)
    )


async def _attach_memory_llm(agent: ToolAgent, llm: MemoryLLM) -> None:
    def llm_factory(**_kwargs: Any) -> MemoryLLM:
        return llm

    await agent.attach_llm(llm_factory)


async def _build_demo_app() -> tuple[ManualFastAgentRun, ToolAgent, MemoryLLM]:
    agent = ToolAgent(
        config=AgentConfig(name="fast_agent_demo", use_history=True),
        tools=[uppercase],
    )
    llm = MemoryLLM()
    await _attach_memory_llm(agent, llm)
    return ManualFastAgentRun(AgentApp({"fast_agent_demo": agent})), agent, llm


async def _run_agent_turns(prompt: str) -> FastAgentDemoResult:
    fast_agent_run, agent, llm = await _build_demo_app()
    runner = KitaruFastAgent(
        fast_agent_run,
        model_checkpoint_config={"cache": True},
        tool_checkpoint_config={"cache": True},
    )

    async with runner.run() as app:
        agent.force_non_streaming_next_turn(reason="deterministic Kitaru example")
        model_reply = await app.send(prompt, agent_name="fast_agent_demo")

        llm.request_tool_call_once()
        app_tool_reply = await app.send(
            "please uppercase kitaru",
            agent_name="fast_agent_demo",
        )

        direct_tool_result = await agent.call_tool("uppercase", {"text": "replay"})

    return FastAgentDemoResult(
        model_reply=model_reply,
        app_tool_reply=app_tool_reply,
        direct_tool_reply=_content_text(direct_tool_result),
        model_generate_calls=llm.generate_calls,
        note=(
            "Kitaru wrapped the AgentApp after run() yielded, then recorded "
            "reachable generate and call_tool calls as checkpoints."
        ),
    )


@flow
def fast_agent_demo_flow(prompt: str = "hello from fast-agent") -> dict[str, Any]:
    """Run the provider-free fast-agent demo inside a Kitaru flow."""
    result = asyncio.run(_run_agent_turns(prompt))
    summary = {
        "model_reply": result.model_reply,
        "app_tool_reply": result.app_tool_reply,
        "direct_tool_reply": result.direct_tool_reply,
        "model_generate_calls": result.model_generate_calls,
        "note": result.note,
    }
    _print_summary(summary)
    return summary


def _print_summary(result: dict[str, Any]) -> None:
    print("\nfast-agent adapter demo summary:")
    print(f"- model_reply: {result['model_reply']}")
    print(f"- app_tool_reply: {result['app_tool_reply']}")
    print(f"- direct_tool_reply: {result['direct_tool_reply']}")
    print(f"- model_generate_calls: {result['model_generate_calls']}")
    print(f"- note: {result['note']}")


def run_demo(prompt: str = "hello from fast-agent") -> str:
    """Submit the demo flow and return the Kitaru execution ID.

    Calls-mode adapter checkpoints are separate terminal steps, so this example
    does not call ``handle.wait()``. The flow prints the user-facing summary
    before it finishes, and the saved checkpoints are visible on the execution.
    """
    handle = fast_agent_demo_flow.run(prompt, cache=False)
    execution = KitaruClient().executions.get(handle.exec_id)
    if execution.status != ExecutionStatus.COMPLETED:
        raise RuntimeError(
            f"fast-agent demo execution {handle.exec_id} ended as {execution.status}"
        )
    print(f"\nSubmitted Kitaru execution: {handle.exec_id}")
    return handle.exec_id


def main() -> None:
    run_demo()
    print("\nInspect the execution in the Kitaru UI or with:")
    print("  uv run kitaru executions list")
    print("  uv run kitaru executions get <execution-id>")


if __name__ == "__main__":
    main()
