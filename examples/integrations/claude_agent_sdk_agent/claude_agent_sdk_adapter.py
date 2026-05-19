"""Real Claude Agent SDK + Kitaru adapter example.

Story:
- A Kitaru flow asks Claude for a short, tool-free summary.
- `KitaruClaudeRunner` wraps that one non-streaming Claude SDK invocation in one
  Kitaru checkpoint.
- The script prints the final text plus session, usage, cost, and artifact
  names captured at the invocation boundary.

Run:
    uv sync --extra local --extra claude-agent-sdk
    uv run kitaru init
    export ANTHROPIC_API_KEY=<your-anthropic-api-key>
    uv run python \
        examples/integrations/claude_agent_sdk_agent/claude_agent_sdk_adapter.py
"""

import argparse
import json
import os
from pathlib import Path
from typing import Any

from claude_agent_sdk import ClaudeAgentOptions

from kitaru import flow
from kitaru.adapters.claude_agent_sdk import (
    ClaudeRunRequest,
    ClaudeRunResult,
    KitaruClaudeRunner,
)

ANTHROPIC_API_KEY_ENV = "ANTHROPIC_API_KEY"
DEFAULT_PROMPT = (
    "In five plain sentences, explain why checkpointing one agent invocation is "
    "useful for a long-running AI workflow. Do not use tools, Bash, or files."
)


def _has_claude_credentials() -> bool:
    """Return whether this process appears able to authenticate the SDK."""
    if os.getenv(ANTHROPIC_API_KEY_ENV):
        return True
    # The Claude SDK also supports Bedrock and Vertex modes. The cloud provider
    # credentials themselves are validated by the SDK; this check only avoids a
    # guaranteed local failure when no provider mode is configured at all.
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
    return KitaruClaudeRunner(
        name="claude_sdk_summary",
        options_factory=lambda request: ClaudeAgentOptions(
            # Keep this integration example non-destructive: the prompt asks for
            # no tools, and `allowed_tools=[]` gives Claude no built-in tools to
            # call even if it tries.
            allowed_tools=[],
            cwd=request.cwd,
            resume=request.resume_session_id,
            max_turns=request.max_turns,
        ),
        checkpoint_config={"cache": False},
    )


RUNNER = _build_runner()


@flow
def summarize_with_claude(prompt: str, cwd: str, max_turns: int) -> ClaudeRunResult:
    """Run one Claude Agent SDK invocation as one Kitaru checkpoint."""
    request = ClaudeRunRequest.start(
        prompt,
        cwd=cwd,
        max_turns=max_turns,
        metadata={"example": "claude_agent_sdk_agent"},
    )
    return RUNNER.run_sync(request)


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
        "Kitaru opened one checkpoint, Claude ran once, and the checkpoint "
        "returned one ClaudeRunResult."
    )

    print("\n=== Claude final text ===")
    print(result.final_text or "(empty final text)")

    print("\n=== Invocation details ===")
    print(f"Session ID: {result.session_id or '(not reported by SDK)'}")
    print(f"Transcript: {result.transcript_path or '(not captured)'}")
    print(f"Turns: {result.num_turns if result.num_turns is not None else '(unknown)'}")
    print(f"Stop reason: {result.stop_reason or '(not reported by SDK)'}")
    if result.cost_usd is None:
        print("Cost: (not reported by SDK)")
    else:
        print(f"Cost: ${result.cost_usd:.6f}")

    print("\n=== Usage ===")
    print(_json_block(result.usage))
    if result.model_usage:
        print("\n=== Model usage ===")
        print(_json_block(result.model_usage))

    print("\n=== Kitaru artifact names ===")
    print(f"Messages: {result.messages_artifact_name or '(disabled)'}")
    print(f"Output: {result.output_artifact_name or '(disabled)'}")
    print(f"Usage: {result.usage_artifact_name or '(not captured)'}")
    print(f"Events: {result.event_log_artifact_name or '(disabled)'}")
    print(f"Run summary: {result.run_summary_artifact_name or '(disabled)'}")

    if result.warnings:
        print("\n=== Warnings ===")
        for warning in result.warnings:
            print(f"- {warning}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run one Claude Agent SDK invocation inside one Kitaru checkpoint."
        )
    )
    parser.add_argument(
        "--prompt",
        default=DEFAULT_PROMPT,
        help="Prompt to send to Claude. Defaults to a short tool-free summary prompt.",
    )
    parser.add_argument(
        "--cwd",
        default=str(Path.cwd()),
        help=(
            "Working directory passed to the Claude SDK. "
            "Defaults to the current directory."
        ),
    )
    parser.add_argument(
        "--max-turns",
        type=int,
        default=1,
        help="Maximum Claude SDK turns for this example. Defaults to 1.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    _require_claude_credentials()

    if args.max_turns <= 0:
        raise SystemExit("--max-turns must be positive.")

    handle = summarize_with_claude.run(args.prompt, args.cwd, args.max_turns)
    result = _coerce_result(handle.wait())
    _print_result(result)


if __name__ == "__main__":
    main()
