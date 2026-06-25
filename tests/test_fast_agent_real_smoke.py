"""Smoke tests against real fast-agent objects when fast-agent is available."""

from __future__ import annotations

import asyncio
import importlib
import os
import sys
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest


def _purge_fast_agent_modules() -> None:
    for cached in list(sys.modules):
        if cached == "fast_agent" or cached.startswith("fast_agent."):
            sys.modules.pop(cached, None)


def _import_real_fast_agent() -> SimpleNamespace:
    """Import real fast-agent modules or skip when the optional runtime is absent."""
    errors: list[str] = []
    local_src = Path(__file__).resolve().parents[1] / "design" / "fast-agent" / "src"
    candidates: list[Path | None] = [None]
    if local_src.exists() and os.environ.get("KITARU_TEST_FAST_AGENT_LOCAL_SRC") == "1":
        candidates.insert(0, local_src)

    for candidate in candidates:
        _purge_fast_agent_modules()
        if str(local_src) in sys.path:
            sys.path.remove(str(local_src))
        if candidate is not None:
            sys.path.insert(0, str(candidate))
        try:
            return SimpleNamespace(
                AgentApp=importlib.import_module("fast_agent.core.agent_app").AgentApp,
                AgentConfig=importlib.import_module(
                    "fast_agent.agents.agent_types"
                ).AgentConfig,
                PromptMessageExtended=importlib.import_module(
                    "fast_agent.types"
                ).PromptMessageExtended,
                RequestParams=importlib.import_module("fast_agent.types").RequestParams,
                LlmStopReason=importlib.import_module("fast_agent.types").LlmStopReason,
                ToolAgent=importlib.import_module(
                    "fast_agent.agents.tool_agent"
                ).ToolAgent,
                text_content=importlib.import_module("fast_agent.types").text_content,
            )
        except Exception as exc:  # pragma: no cover - depends on optional package state
            source = (
                str(candidate) if candidate is not None else "installed fast-agent-mcp"
            )
            errors.append(f"{source}: {type(exc).__name__}: {exc}")

    pytest.skip("fast-agent is unavailable or incompatible: " + "; ".join(errors))


class _ManualFastAgentRun:
    def __init__(self, app: Any) -> None:
        self._app = app

    @asynccontextmanager
    async def run(self) -> AsyncIterator[Any]:
        yield self._app


class _MemoryLLM:
    """No-network LLM that returns real fast-agent message objects."""

    model_name = "memory-smoke-model"
    provider = "memory-smoke-provider"
    resolved_model = None
    usage_accumulator = None

    def __init__(self, fast_agent_modules: SimpleNamespace) -> None:
        self.default_request_params = fast_agent_modules.RequestParams(use_history=True)
        self.instruction = ""
        self.generate_calls = 0
        self.structured_calls = 0
        self._tool_call_requested = False
        self._fast_agent_modules = fast_agent_modules

    def get_request_params(self, request_params: Any | None = None) -> Any:
        return request_params or self.default_request_params

    def request_tool_call_once(self) -> None:
        self._tool_call_requested = True

    async def generate(
        self,
        messages: list[Any],
        request_params: Any | None = None,
        tools: list[Any] | None = None,
    ) -> Any:
        del request_params, tools
        self.generate_calls += 1
        if self._tool_call_requested:
            self._tool_call_requested = False
            return self._assistant_tool_call("uppercase", {"text": "kitaru"})
        last_user_text = messages[-1].last_text() if messages else "<empty>"
        if any(getattr(message, "tool_results", None) for message in messages):
            return self._assistant_message("memory tool loop complete")
        return self._assistant_message(f"memory reply to {last_user_text}")

    async def structured(
        self,
        messages: list[Any],
        model: type[object],
        request_params: Any | None = None,
    ) -> tuple[None, Any]:
        del messages, model, request_params
        self.structured_calls += 1
        return None, self._assistant_message("structured memory reply")

    def _assistant_message(self, text: str) -> Any:
        fast_agent = self._fast_agent_modules
        return fast_agent.PromptMessageExtended(
            role="assistant",
            content=[fast_agent.text_content(text)],
        )

    def _assistant_tool_call(self, name: str, arguments: dict[str, Any]) -> Any:
        from mcp.types import CallToolRequest, CallToolRequestParams

        fast_agent = self._fast_agent_modules
        return fast_agent.PromptMessageExtended(
            role="assistant",
            content=[fast_agent.text_content("calling local tool")],
            tool_calls={
                "call-1": CallToolRequest(
                    method="tools/call",
                    params=CallToolRequestParams(name=name, arguments=arguments),
                ),
            },
            stop_reason=fast_agent.LlmStopReason.TOOL_USE,
        )


def _content_text(result: Any) -> str:
    return "\n".join(
        text
        for item in result.content
        if isinstance((text := getattr(item, "text", None)), str)
    )


async def _attach_memory_llm(agent: Any, llm: _MemoryLLM) -> None:
    def llm_factory(**_kwargs: Any) -> _MemoryLLM:
        return llm

    await agent.attach_llm(llm_factory)


def test_real_fast_agent_agent_app_model_and_tool_paths_are_wrapped() -> None:
    fast_agent = _import_real_fast_agent()

    from kitaru.adapters.fast_agent import KitaruFastAgent

    def uppercase(text: str) -> str:
        return text.upper()

    agent = fast_agent.ToolAgent(
        config=fast_agent.AgentConfig(name="smoke", use_history=True),
        tools=[uppercase],
    )
    llm = _MemoryLLM(fast_agent)
    asyncio.run(_attach_memory_llm(agent, llm))
    app = fast_agent.AgentApp({"smoke": agent})
    recorded: list[Any] = []

    def recorder(call: Any, proceed: Callable[[], Any]) -> Any:
        recorded.append(call)
        return proceed()

    runner = KitaruFastAgent(_ManualFastAgentRun(app), call_recorder=recorder)

    async def exercise() -> tuple[str, str, str]:
        async with runner.run() as wrapped_app:
            assert wrapped_app is app

            # This assertion intentionally documents the current prototype shape:
            # Kitaru installs calls-mode interception by replacing the active
            # agent's attached LLM object. If fast-agent exposes a public
            # post-build hook for active agents later, this monkeypatch can move
            # behind that hook instead of reaching into the live agent object.
            assert agent.llm.original_llm is llm

            agent.force_non_streaming_next_turn(reason="deterministic Kitaru smoke")
            model_reply = await wrapped_app.send("hello", agent_name="smoke")
            llm.request_tool_call_once()
            app_driven_tool_reply = await wrapped_app.send(
                "please uppercase kitaru",
                agent_name="smoke",
            )
            tool_result = await agent.call_tool("uppercase", {"text": "kitaru"})
            return model_reply, app_driven_tool_reply, _content_text(tool_result)

    model_reply, app_driven_tool_reply, tool_text = asyncio.run(exercise())

    assert model_reply == "memory reply to hello"
    assert app_driven_tool_reply == "memory tool loop complete"
    assert "KITARU" in tool_text
    assert llm.generate_calls == 3
    assert [(call.kind, call.operation, call.agent_name) for call in recorded] == [
        ("model", "generate", "smoke"),
        ("model", "generate", "smoke"),
        ("tool", "call_tool", "smoke"),
        ("model", "generate", "smoke"),
        ("tool", "call_tool", "smoke"),
    ]
    assert recorded[0].model_name == "memory-smoke-model"
    assert recorded[0].provider == "memory-smoke-provider"
    assert recorded[2].tool_name == "uppercase"
    assert recorded[4].tool_name == "uppercase"
