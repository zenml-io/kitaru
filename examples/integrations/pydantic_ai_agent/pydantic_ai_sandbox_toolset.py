"""Run a PydanticAI tool call inside the active Kitaru stack sandbox.

This example gives the model one Kitaru-provided tool named
``run_sandbox_command``. When the model calls that tool, Kitaru creates one
fresh sandbox session from the active stack, runs one command, collects stdout,
stderr, and exit code, then cleans up the session.

Prerequisites:
    uv sync --extra local --extra pydantic-ai --extra openai
    uv run kitaru init
    export OPENAI_API_KEY=sk-...

Your active stack must have exactly one sandbox component. The default local
Kitaru stack includes one when the installed ZenML version supports sandbox
components.

Run:
    uv run python examples/integrations/pydantic_ai_agent/pydantic_ai_sandbox_toolset.py
"""

import os
from typing import Any

from pydantic_ai import Agent

from kitaru import flow
from kitaru.adapters.pydantic_ai import (
    DEFAULT_SANDBOX_TOOL_MAX_CHARS,
    SANDBOX_COMMAND_TOOL_NAME,
    KitaruAgent,
    sandbox_command_toolset,
)
from kitaru.errors import (
    KitaruBackendError,
    KitaruFeatureNotAvailableError,
    KitaruStateError,
)


def _default_model() -> str:
    return os.getenv("PYDANTIC_AI_MODEL", "openai:gpt-5-nano")


def _require_provider_configuration(model: Any) -> None:
    if not isinstance(model, str):
        return
    if model.startswith("openai:") and not os.getenv("OPENAI_API_KEY"):
        raise SystemExit(
            "Missing OPENAI_API_KEY.\n"
            "Set it first, then rerun:\n"
            "  export OPENAI_API_KEY='sk-...'\n"
            "Or choose another PydanticAI model with PYDANTIC_AI_MODEL."
        )


def build_agent(
    *, model: Any | None = None, max_chars: int = DEFAULT_SANDBOX_TOOL_MAX_CHARS
) -> KitaruAgent[None, str]:
    """Build the sandbox-enabled PydanticAI agent without running it."""
    model = _default_model() if model is None else model
    agent = Agent(
        model,
        name="sandboxed_pydantic_ai_agent",
        output_type=str,
        instructions=(
            "You are a careful command-running assistant. Use the "
            f"{SANDBOX_COMMAND_TOOL_NAME} tool when asked to inspect the sandbox. "
            "When reporting the result, include the exit code and any stdout or "
            "stderr that matters."
        ),
        toolsets=[sandbox_command_toolset(max_chars=max_chars)],
    )
    return KitaruAgent(
        agent,
        tool_checkpoint_config_by_name={
            SANDBOX_COMMAND_TOOL_NAME: {"cache": False},
        },
    )


def main() -> None:
    model = _default_model()
    _require_provider_configuration(model)
    sandboxed_agent = build_agent(model=model)

    @flow
    def sandbox_toolset_flow() -> str:
        result = sandboxed_agent.run_sync(
            f"Use {SANDBOX_COMMAND_TOOL_NAME} to run `python --version` in "
            "the sandbox. Then answer with the exact exit code and output."
        )
        return result.output

    try:
        answer = sandbox_toolset_flow.run(cache=False).wait()
    except (KitaruFeatureNotAvailableError, KitaruStateError) as error:
        raise SystemExit(
            "This example needs an active Kitaru stack with exactly one sandbox "
            f"component. Current setup could not provide that sandbox: {error}"
        ) from error
    except KitaruBackendError as error:
        raise SystemExit(
            "The sandbox command reached the active stack, but the backend could "
            f"not execute it successfully: {error}"
        ) from error

    print("=== sandbox command result ===")
    print(answer)


if __name__ == "__main__":
    main()
