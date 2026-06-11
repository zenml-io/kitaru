"""OpenAI Agents SDK tool backed by Kitaru's active-stack sandbox.

Story:
- The agent receives a small diagnostic task.
- It calls Kitaru's sandbox command tool instead of running code locally.
- Kitaru executes the command through the sandbox on the active stack.

Run:
    uv sync --extra local --extra openai-agents
    uv run kitaru init
    export OPENAI_API_KEY=sk-...
    uv run python \
        examples/integrations/openai_agents_agent/openai_agents_sandbox_tool.py
"""

import os
from typing import Literal

from agents import Agent, RunConfig

from kitaru import flow
from kitaru.adapters.openai_agents import (
    KitaruRunner,
    OpenAIRunRequest,
    sandbox_command_tool,
)
from kitaru.errors import KitaruAmbiguousFlowResultError

SAFE_INSPECTION_COMMAND = (
    'python -c "import os, platform; '
    'print(platform.python_version()); print(os.getcwd())"'
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
        name="sandbox_command_agent",
        instructions=(
            "You can run one safe diagnostic command through the "
            "kitaru_sandbox_command tool. The tool returns JSON with stdout, "
            "stderr, exit_code, truncation flags, and cleanup status. Always "
            "check exit_code first. If exit_code is 0, summarize stdout. If it "
            "is non-zero, explain stderr and do not pretend the command worked."
        ),
        model=model,
        tools=[sandbox_command_tool(max_chars=4_000, cleanup="destroy")],
    )


def _run_once(checkpoint_strategy: Literal["calls", "runner_call"]) -> str:
    agent = _build_agent()
    runner = KitaruRunner(
        agent,
        checkpoint_strategy=checkpoint_strategy,
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )

    @flow
    def sandbox_tool_flow(prompt: str) -> str:
        result = runner.run_sync(OpenAIRunRequest.start(prompt))
        if result.status != "completed":
            raise RuntimeError(f"Expected completed run, got status={result.status!r}.")
        return str(result.final_output)

    prompt = (
        "Use the sandbox command tool to run this exact command: "
        f"{SAFE_INSPECTION_COMMAND!r}. Check exit_code before using stdout. "
        "Then tell me the Python version and sandbox working directory."
    )
    return str(sandbox_tool_flow.run(prompt).wait())


def main() -> None:
    _require_openai_api_key()

    model_label = os.getenv("OPENAI_AGENTS_MODEL", "gpt-5-nano")
    print(f"Using model: {model_label}")

    runner_call_output = _run_once("runner_call")
    print("\n=== sandbox tool runner_call output ===")
    print(runner_call_output)

    if os.getenv("OPENAI_AGENTS_COMPARE_CALLS", "").lower() in {
        "1",
        "true",
        "yes",
    }:
        try:
            calls_output = _run_once("calls")
        except KitaruAmbiguousFlowResultError as error:
            print("\n=== sandbox tool calls output ===")
            print(f"(per-checkpoint artifacts only; .wait() raised: {error})")
        else:
            print("\n=== sandbox tool calls output ===")
            print(calls_output)


if __name__ == "__main__":
    main()
