"""Run a PydanticAI tool call inside the active Kitaru stack sandbox.

This example gives the model one Kitaru-provided tool named
``run_sandbox_command``. When the model calls that tool, Kitaru creates one
fresh sandbox session from the active stack, runs one command, collects stdout,
stderr, and exit code, then cleans up the session.

The model controls the command string and optional working directory. Treat that
like letting the model type in a terminal attached to the sandbox process: files,
network access, environment variables, and credentials visible there can be
printed to stdout/stderr, returned to the model, and persisted in Kitaru
execution/checkpoint artifacts. The local sandbox is a local development
convenience, not a security boundary.

Prerequisites:
    uv sync --extra local --extra pydantic-ai --extra openai
    uv run kitaru init
    export OPENAI_API_KEY=sk-...

Your active stack must have exactly one sandbox component. Check the active
stack with:
    uv run kitaru stack current
    uv run kitaru stack show <name>

If the active stack has no sandbox component, create a sandbox-enabled local
stack and make it active:
    uv run kitaru stack create sandbox-demo --sandbox local

Run:
    uv run python examples/integrations/pydantic_ai_agent/pydantic_ai_sandbox_toolset.py
"""

import os
import time
from typing import Any

from pydantic_ai import Agent
from pydantic_ai.messages import ToolCallPart

from kitaru import checkpoint, flow
from kitaru.adapters.pydantic_ai import (
    DEFAULT_SANDBOX_TOOL_MAX_CHARS,
    SANDBOX_COMMAND_TOOL_NAME,
    KitaruAgent,
    sandbox_command_toolset,
)
from kitaru.client import ExecutionStatus
from kitaru.errors import (
    KitaruBackendError,
    KitaruFeatureNotAvailableError,
    KitaruStateError,
)

_SANDBOX_PROMPT = (
    f"Use {SANDBOX_COMMAND_TOOL_NAME} to run `python --version` in the sandbox. "
    "Then answer with the exact exit code and output."
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
        checkpoint_strategy="calls",
        tool_checkpoint_config_by_name={
            SANDBOX_COMMAND_TOOL_NAME: {"cache": False},
        },
    )


def _result_called_sandbox_tool(result: Any) -> bool:
    """Return whether PydanticAI recorded a sandbox tool call in the run."""
    return any(
        isinstance(part, ToolCallPart) and part.tool_name == SANDBOX_COMMAND_TOOL_NAME
        for message in result.all_messages()
        for part in getattr(message, "parts", [])
    )


def run_sandbox_agent_turn(sandboxed_agent: KitaruAgent[None, str]) -> str:
    """Run the agent at flow scope so model/tool calls get their own checkpoints."""
    result = sandboxed_agent.run_sync(_SANDBOX_PROMPT)
    if not _result_called_sandbox_tool(result):
        raise RuntimeError(
            f"The model answered without calling {SANDBOX_COMMAND_TOOL_NAME}. "
            "This live example must exercise the active stack sandbox, so the "
            "run is treated as failed. Try rerunning, or set PYDANTIC_AI_MODEL "
            "to a model that follows tool-use instructions."
        )
    return result.output


@checkpoint
def publish_sandbox_answer(answer: str) -> str:
    """Store the final answer on a named checkpoint for UI/CLI inspection."""
    return answer


@flow
def sandbox_toolset_flow(
    model: str | None = None,
    max_chars: int = DEFAULT_SANDBOX_TOOL_MAX_CHARS,
) -> str:
    """Run the sandbox-enabled agent while keeping per-tool checkpoints visible."""
    sandboxed_agent = build_agent(model=model, max_chars=max_chars)
    answer = run_sandbox_agent_turn(sandboxed_agent)
    return publish_sandbox_answer(answer)


def submit_sandbox_toolset_flow(
    *,
    model: str | None = None,
    max_chars: int = DEFAULT_SANDBOX_TOOL_MAX_CHARS,
) -> Any:
    """Submit the example flow with caching disabled for an honest live demo.

    ``flow.run(cache=False)`` reruns the model and tool work on each example run.
    The agent also disables cache for the sandbox tool checkpoint itself, which
    matters when ``checkpoint_strategy="calls"`` creates ``run_sandbox_command_tool``.
    """
    return sandbox_toolset_flow.run(model=model, max_chars=max_chars, cache=False)


def wait_for_completion(
    handle: Any,
    *,
    poll_seconds: float = 1.0,
    timeout_seconds: float = 300.0,
) -> ExecutionStatus:
    """Wait for terminal execution status without extracting a flow result.

    This demo intentionally keeps per-call adapter checkpoints visible. In that
    shape, ``FlowHandle.wait()`` is not the right API because result extraction
    sees several terminal model/tool checkpoints and refuses to guess. Polling
    ``handle.status`` waits for completion without asking Kitaru to choose one
    output value.
    """
    start = time.monotonic()
    last_status = handle.status
    while True:
        if last_status.is_finished:
            return last_status
        if time.monotonic() - start >= timeout_seconds:
            execution_id = getattr(handle, "exec_id", "<unknown>")
            raise TimeoutError(
                "Timed out waiting for sandbox toolset flow "
                f"execution {execution_id} after {timeout_seconds:g}s. "
                f"Last observed status: {last_status.value}."
            )
        time.sleep(poll_seconds)
        last_status = handle.status


def main() -> None:
    model = _default_model()
    _require_provider_configuration(model)

    try:
        handle = submit_sandbox_toolset_flow(model=model)
        print(f"Submitted sandbox toolset flow execution: {handle.exec_id}")
        status = wait_for_completion(handle)
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
    except TimeoutError as error:
        raise SystemExit(str(error)) from error

    if status is not ExecutionStatus.COMPLETED:
        raise SystemExit(
            f"Sandbox toolset flow finished with status {status.value}. "
            f"Inspect execution {handle.exec_id} for details."
        )

    print("=== sandbox command checkpoints ===")
    print(f"Execution: {handle.exec_id}")
    print("Open the Kitaru UI or run:")
    print(f"  uv run kitaru executions get {handle.exec_id}")
    print("You should see these checkpoints:")
    print("  - sandboxed_pydantic_ai_agent_model_request")
    print("  - run_sandbox_command_tool")
    print("  - sandboxed_pydantic_ai_agent_model_request_2")
    print("  - publish_sandbox_answer")
    print(
        "The final text is stored on the publish_sandbox_answer checkpoint. "
        "This per-tool checkpoint demo does not use `.wait()` for the final answer "
        "because adapter-created model/tool checkpoints are also terminal graph steps."
    )


if __name__ == "__main__":
    main()
