"""Stream Claude Agent SDK live events through Kitaru.

Story:
- A Kitaru flow asks Claude for a short, tool-free explanation.
- `KitaruClaudeRunner.run_stream_sync(...)` wraps that one Claude SDK stream in
  one Kitaru checkpoint.
- Kitaru forwards useful Claude stream updates while the checkpoint is active,
  then saves the final ClaudeRunResult as the durable record.

Run:
    uv sync --extra local --extra claude-agent-sdk
    uv run kitaru init
    export ANTHROPIC_API_KEY=<your-anthropic-api-key>
    uv run python \
        examples/integrations/claude_agent_sdk_agent/claude_agent_sdk_streaming.py
"""

import argparse
import json
import os
import threading
from pathlib import Path
from typing import Any

from claude_agent_sdk import ClaudeAgentOptions

from kitaru import flow
from kitaru.adapters.claude_agent_sdk import (
    CLAUDE_STREAM_EVENT_KINDS,
    CLAUDE_STREAM_TERMINAL_EVENT_KINDS,
    ClaudeRunRequest,
    ClaudeRunResult,
    KitaruClaudeRunner,
)
from kitaru.client import KitaruClient
from kitaru.errors import KitaruBackendError, KitaruFeatureNotAvailableError

ANTHROPIC_API_KEY_ENV = "ANTHROPIC_API_KEY"
DEFAULT_PROMPT = (
    "In five plain sentences, explain why live progress updates are useful "
    "while an AI workflow is running. Do not use tools, Bash, or files."
)


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
    return KitaruClaudeRunner(
        name="claude_sdk_streaming_summary",
        options_factory=lambda request: ClaudeAgentOptions(
            # Keep the demo deterministic: no built-in tools means a simple stream.
            allowed_tools=[],
            cwd=request.cwd,
            resume=request.resume_session_id,
            max_turns=request.max_turns,
        ),
        # Demo runs should show live events instead of replaying a cached result.
        checkpoint_config={"cache": False},
    )


RUNNER = _build_runner()


@flow
def stream_with_claude(prompt: str, cwd: str, max_turns: int) -> ClaudeRunResult:
    """Run one streamed Claude Agent SDK invocation as one Kitaru checkpoint."""
    request = ClaudeRunRequest.start(
        prompt,
        cwd=cwd,
        max_turns=max_turns,
        metadata={"example": "claude_agent_sdk_streaming"},
    )
    return RUNNER.run_stream_sync(request)


def _event_data(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data")
    return data if isinstance(data, dict) else {}


def _watch_claude_stream(exec_id: str, stop_event: threading.Event) -> None:
    print("\n=== live Claude Agent SDK stream events ===")
    try:
        for event in KitaruClient().executions.events(
            exec_id,
            kinds=list(CLAUDE_STREAM_EVENT_KINDS),
        ):
            if stop_event.is_set():
                return
            data = _event_data(event.payload)
            display = data.get("display") or event.kind
            category = data.get("category")
            prefix = f"[{category}] " if isinstance(category, str) else ""
            print(f"- {prefix}{display}")
            text_delta = data.get("text_delta")
            if isinstance(text_delta, str) and text_delta and text_delta != display:
                print(f"  text_delta: {text_delta}")
            if event.kind in CLAUDE_STREAM_TERMINAL_EVENT_KINDS:
                return
    except (KitaruBackendError, KitaruFeatureNotAvailableError) as error:
        print("\nLive event watching is unavailable on this backend.")
        print(f"The durable result will still be read with .wait(): {error}")


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


def _print_final_result(result: ClaudeRunResult) -> None:
    print("\n=== durable ClaudeRunResult ===")
    print(f"status: {result.status}")

    print("\nfinal text:")
    print(result.final_text or "(empty final text)")

    print("\ninvocation details:")
    print(f"session_id: {result.session_id or '(not reported by SDK)'}")
    print(f"turns: {result.num_turns if result.num_turns is not None else '(unknown)'}")
    print(f"stop_reason: {result.stop_reason or '(not reported by SDK)'}")
    if result.cost_usd is None:
        print("cost: (not reported by SDK)")
    else:
        print(f"cost: ${result.cost_usd:.6f}")

    print("\nusage:")
    print(_json_block(result.usage))
    if result.model_usage:
        print("\nmodel usage:")
        print(_json_block(result.model_usage))

    print("\nKitaru artifact names:")
    print(f"messages: {result.messages_artifact_name or '(disabled)'}")
    print(f"transcript: {result.transcript_artifact_name or '(not captured)'}")
    print(f"options manifest: {result.options_manifest_artifact_name or '(disabled)'}")
    print(f"output: {result.output_artifact_name or '(disabled)'}")
    print(f"usage: {result.usage_artifact_name or '(not captured)'}")
    print(f"events: {result.event_log_artifact_name or '(disabled)'}")
    print(f"run summary: {result.run_summary_artifact_name or '(disabled)'}")

    if result.warnings:
        print("\nwarnings:")
        for warning in result.warnings:
            print(f"- {warning}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run one streamed Claude Agent SDK invocation inside one Kitaru "
            "checkpoint and watch best-effort live events."
        )
    )
    parser.add_argument(
        "--prompt",
        default=DEFAULT_PROMPT,
        help="Prompt to send to Claude. Defaults to a short tool-free prompt.",
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

    handle = stream_with_claude.run(args.prompt, args.cwd, args.max_turns, cache=False)
    exec_id = handle.exec_id
    print(f"Submitted execution: {exec_id}")
    print(
        "Live watching requires a REST-backed Kitaru stream-event backend. "
        "If watching is unavailable, the durable .wait() result still works."
    )

    stop_watching = threading.Event()
    watcher = threading.Thread(
        target=_watch_claude_stream,
        args=(exec_id, stop_watching),
        daemon=True,
    )
    watcher.start()

    wait_value = handle.wait()
    stop_watching.set()
    watcher.join(timeout=1.0)
    if watcher.is_alive():
        print("\nLive watcher is still open; showing the durable result now.")

    _print_final_result(_coerce_result(wait_value))


if __name__ == "__main__":
    main()
