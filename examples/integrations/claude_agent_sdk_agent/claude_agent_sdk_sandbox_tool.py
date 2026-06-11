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
    export ANTHROPIC_API_KEY=<your-anthropic-api-key>
    uv run python \
        examples/integrations/claude_agent_sdk_agent/claude_agent_sdk_sandbox_tool.py
"""

import argparse
import json
import os
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


def _build_runner() -> KitaruClaudeRunner:
    sandbox_mcp_server = create_kitaru_sandbox_mcp_server()
    return KitaruClaudeRunner(
        name="claude_sdk_kitaru_sandbox_tool",
        options_factory=lambda request: ClaudeAgentOptions(
            cwd=request.cwd,
            resume=request.resume_session_id,
            max_turns=request.max_turns,
            mcp_servers={"kitaru": sandbox_mcp_server},
            # Built-in Bash remains Claude-owned. Deny it so this example proves
            # command execution goes through the Kitaru sandbox MCP tool instead.
            disallowed_tools=["Bash"],
            allowed_tools=[KITARU_SANDBOX_COMMAND_ALLOWED_TOOL_NAME],
        ),
        checkpoint_config={"cache": False},
    )


@flow
def inspect_sandbox_with_claude(
    command: str,
    sandbox_cwd: str | None,
    claude_cwd: str,
    max_turns: int,
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
    runner = _build_runner()
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
        "Claude was allowed to call the Kitaru sandbox MCP command tool, but "
        "Claude's built-in Bash was denied. Kitaru still recorded one completed "
        "Claude SDK invocation as one checkpoint."
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
        default=str(Path.cwd()),
        help="Working directory passed to the Claude SDK itself.",
    )
    parser.add_argument(
        "--max-turns",
        type=int,
        default=3,
        help="Maximum Claude SDK turns. Defaults to 3 so Claude can call the tool.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    _require_claude_credentials()

    if args.max_turns <= 0:
        raise SystemExit("--max-turns must be positive.")

    handle = inspect_sandbox_with_claude.run(
        args.command,
        args.sandbox_cwd,
        args.claude_cwd,
        args.max_turns,
    )
    result = _coerce_result(handle.wait())
    _print_result(result)


if __name__ == "__main__":
    main()
