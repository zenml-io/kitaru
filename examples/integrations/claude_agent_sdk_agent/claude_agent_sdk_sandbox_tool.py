"""Claude Agent SDK example using Kitaru's sandbox command MCP tool.

Story:
- Claude receives one MCP tool: mcp__kitaru__run_command.
- Claude's built-in Bash is denied.
- When Claude calls the MCP tool, Kitaru runs the command through the active
  stack's sandbox component with kitaru.run_sandbox_command(...).
- The whole Claude SDK invocation is still one Kitaru checkpoint.

Run:
    uv sync --extra local --extra claude-agent-sdk
    uv run kitaru init
    uv run kitaru stack create claude-sandbox --sandbox local
    export ANTHROPIC_API_KEY=<your-anthropic-api-key>
    uv run python \
        examples/integrations/claude_agent_sdk_agent/claude_agent_sdk_sandbox_tool.py
"""

import argparse
import json
import math
import os
import tempfile
from pathlib import Path
from typing import Any

from claude_agent_sdk import ClaudeAgentOptions

from kitaru import flow
from kitaru.adapters.claude_agent_sdk import (
    KITARU_SANDBOX_COMMAND_ALLOWED_TOOL_NAME,
    ClaudeRunRequest,
    ClaudeRunResult,
    KitaruClaudeRunner,
    create_kitaru_sandbox_mcp_server,
)

ANTHROPIC_API_KEY_ENV = "ANTHROPIC_API_KEY"
DEFAULT_SANDBOX_COMMAND = "python --version"
DEFAULT_MODEL = "sonnet"
DEFAULT_MAX_BUDGET_USD = 0.10
DEFAULT_CLAUDE_CWD_NAME = "kitaru-claude-sandbox-tool-demo"


def _has_claude_credentials() -> bool:
    """Return whether this process appears able to authenticate the SDK."""
    if os.getenv(ANTHROPIC_API_KEY_ENV):
        return True
    return (
        os.getenv("CLAUDE_CODE_USE_BEDROCK") == "1"
        or os.getenv("CLAUDE_CODE_USE_VERTEX") == "1"
    )


def _require_claude_credentials() -> None:
    if _has_claude_credentials():
        return
    raise SystemExit(
        "Missing Claude/Anthropic credentials.\n"
        "For the direct Anthropic API path, set:\n"
        "  export ANTHROPIC_API_KEY='<your-anthropic-api-key>'\n"
        "Bedrock and Vertex modes are also supported by the Claude SDK when "
        "their provider-specific environment is configured."
    )


def _build_runner(
    *,
    model: str | None,
    max_budget_usd: float | None,
) -> KitaruClaudeRunner:
    sandbox_mcp_server = create_kitaru_sandbox_mcp_server()
    return KitaruClaudeRunner(
        name="claude_sdk_kitaru_sandbox_tool",
        options_factory=lambda request: ClaudeAgentOptions(
            cwd=request.cwd,
            resume=request.resume_session_id,
            max_turns=request.max_turns,
            model=model,
            max_budget_usd=max_budget_usd,
            effort="low",
            mcp_servers={"kitaru": sandbox_mcp_server},
            strict_mcp_config=True,
            permission_mode="dontAsk",
            # Disable Claude's built-in tools. The MCP server below still exposes
            # mcp__kitaru__run_command as the one pre-approved command tool.
            tools=[],
            system_prompt=(
                "You are testing Kitaru's sandbox command MCP tool. Use the "
                "requested Kitaru MCP command tool, then summarize the command "
                "result briefly."
            ),
            # Built-in Bash remains Claude-owned. Deny it so this example proves
            # command execution goes through the Kitaru sandbox MCP tool instead.
            disallowed_tools=["Bash"],
            allowed_tools=[KITARU_SANDBOX_COMMAND_ALLOWED_TOOL_NAME],
            # Keep this demo independent from the user's Claude Code project or
            # local settings. The command itself still runs through Kitaru's
            # active stack sandbox, not through Claude's working directory.
            setting_sources=[],
            extra_args={"bare": None},
        ),
        checkpoint_config={"cache": False},
    )


@flow
def inspect_sandbox_with_claude(
    command: str,
    sandbox_cwd: str | None,
    claude_cwd: str,
    max_turns: int,
    model: str | None,
    max_budget_usd: float | None,
) -> ClaudeRunResult:
    """Ask Claude to run one command through Kitaru's sandbox MCP tool."""
    prompt = (
        "Use only the MCP tool named "
        f"{KITARU_SANDBOX_COMMAND_ALLOWED_TOOL_NAME} to run this command through "
        "Kitaru's active stack sandbox. Do not use Bash.\n\n"
        f"Command: {command!r}\n"
        f"Sandbox cwd: {sandbox_cwd!r}\n\n"
        "After the tool returns, summarize stdout, stderr, exit_code, stack, "
        "sandbox, session_id, and cleanup status."
    )
    request = ClaudeRunRequest.start(
        prompt,
        cwd=claude_cwd,
        max_turns=max_turns,
        metadata={"example": "claude_agent_sdk_sandbox_tool"},
    )
    runner = _build_runner(model=model, max_budget_usd=max_budget_usd)
    return runner.run_sync(request)


def _coerce_result(value: Any) -> ClaudeRunResult:
    if isinstance(value, ClaudeRunResult):
        return value
    if isinstance(value, dict):
        return ClaudeRunResult.model_validate(value)
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        return ClaudeRunResult.model_validate(model_dump(mode="python"))
    raise TypeError(f"Expected ClaudeRunResult from flow, got {type(value).__name__}.")


def _json_block(value: Any) -> str:
    if value is None:
        return "(not reported by SDK)"
    return json.dumps(value, indent=2, sort_keys=True, default=str)


def _print_result(result: ClaudeRunResult) -> None:
    print("\n=== What happened ===")
    print(
        "Claude was allowed to call the Kitaru sandbox MCP command tool, while "
        "Claude's built-in tools were disabled. Kitaru still recorded one "
        "completed Claude SDK invocation as one checkpoint."
    )

    print("\n=== Claude final text ===")
    print(result.final_text or "(empty final text)")

    print("\n=== Invocation details ===")
    print(f"Session ID: {result.session_id or '(not reported by SDK)'}")
    print(f"Turns: {result.num_turns if result.num_turns is not None else '(unknown)'}")
    print(f"Stop reason: {result.stop_reason or '(not reported by SDK)'}")
    if result.cost_usd is None:
        print("Cost: (not reported by SDK)")
    else:
        print(f"Cost: ${result.cost_usd:.6f}")

    print("\n=== Usage ===")
    print(_json_block(result.usage))

    print("\n=== Kitaru artifact names ===")
    print(f"Messages: {result.messages_artifact_name or '(disabled)'}")
    print(f"Manifest: {result.options_manifest_artifact_name or '(disabled)'}")
    print(f"Output: {result.output_artifact_name or '(disabled)'}")
    print(f"Events: {result.event_log_artifact_name or '(disabled)'}")
    print(f"Run summary: {result.run_summary_artifact_name or '(disabled)'}")

    if result.warnings:
        print("\n=== Warnings ===")
        for warning in result.warnings:
            print(f"- {warning}")


def _default_claude_cwd() -> str:
    """Create and return a small Claude SDK working directory for this demo."""
    path = Path(tempfile.gettempdir()) / DEFAULT_CLAUDE_CWD_NAME
    path.mkdir(parents=True, exist_ok=True)
    return str(path)


def _coerce_optional_budget(value: float) -> float | None:
    """Return an SDK budget cap, treating 0 as an explicit opt-out."""
    if not math.isfinite(value):
        raise SystemExit("--max-budget-usd must be finite.")
    if value < 0:
        raise SystemExit("--max-budget-usd must be non-negative.")
    if value == 0:
        return None
    return value


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Ask Claude to run a safe command through Kitaru's active stack "
            "sandbox MCP tool."
        )
    )
    parser.add_argument(
        "--command",
        default=DEFAULT_SANDBOX_COMMAND,
        help="Command Claude should run through the Kitaru sandbox tool.",
    )
    parser.add_argument(
        "--sandbox-cwd",
        default=None,
        help="Optional working directory inside the active stack sandbox.",
    )
    parser.add_argument(
        "--claude-cwd",
        default=None,
        help=(
            "Working directory passed to the Claude SDK itself. Defaults to a "
            "small temp directory so Claude does not load your whole repository "
            "context just to run the sandbox command."
        ),
    )
    parser.add_argument(
        "--max-turns",
        type=int,
        default=3,
        help="Maximum Claude SDK turns. Defaults to 3 so Claude can call the tool.",
    )
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        help=(
            "Claude model passed to ClaudeAgentOptions. Defaults to the "
            f"tool-capable Claude Code alias {DEFAULT_MODEL!r}."
        ),
    )
    parser.add_argument(
        "--max-budget-usd",
        type=float,
        default=DEFAULT_MAX_BUDGET_USD,
        help=(
            "Claude SDK budget cap for this demo. Defaults to $0.10; pass 0 to "
            "disable the cap."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    _require_claude_credentials()

    if args.max_turns <= 0:
        raise SystemExit("--max-turns must be positive.")

    claude_cwd = args.claude_cwd or _default_claude_cwd()
    max_budget_usd = _coerce_optional_budget(args.max_budget_usd)

    handle = inspect_sandbox_with_claude.run(
        args.command,
        args.sandbox_cwd,
        claude_cwd,
        args.max_turns,
        args.model,
        max_budget_usd,
    )
    result = _coerce_result(handle.wait())
    _print_result(result)


if __name__ == "__main__":
    main()
