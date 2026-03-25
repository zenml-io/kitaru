"""General-purpose coding agent — Kitaru primitives + direct provider SDKs.

An agent loop where each LLM call and tool execution is a visible
checkpoint. The initial task is passed as a flow parameter. If the LLM
needs clarification it calls ``ask_user``, which triggers
``kitaru.wait()``. When done, it calls ``hand_back`` and waits for a
follow-up instruction.

Usage::

    cd examples/coding_agent
    uv run python agent.py "Create a plotly chart"

Or supply follow-up input via the CLI::

    kitaru executions input <exec-id> \
        --wait ask_0 --value "use population data"
    kitaru executions resume <exec-id>
"""

import html
import json
import tempfile
import time
from pathlib import Path
from typing import Any

import click
import materializers as _materializers  # noqa: F401 — registers custom materializers
from llm import MAX_TOOL_ROUNDS, MODEL, complete_agent_turn
from models import (
    FollowUp,
    LLMResponse,
    ToolCallResult,
)
from prompts import SYSTEM_PROMPT
from tools import ALL_TOOLS, dispatch_tool, sanitize_display_name, save_generated_files
from zenml.types import HTMLString

import kitaru
from kitaru import checkpoint, flow

_WORKSPACE = Path(tempfile.mkdtemp(prefix="agent_"))


# ---------------------------------------------------------------------------
# Checkpoints
# ---------------------------------------------------------------------------


@checkpoint(type="llm_call")
def llm_call(messages: list[dict[str, Any]]) -> LLMResponse:
    """Single LLM completion — tracked as a checkpoint."""
    started_at = time.perf_counter()
    response, usage = complete_agent_turn(messages, tools=ALL_TOOLS)
    latency_ms = round((time.perf_counter() - started_at) * 1000, 3)

    kitaru.log(
        llm_usage={
            k: v
            for k, v in {
                "model": MODEL,
                "latency_ms": latency_ms,
                "tokens_input": usage.prompt_tokens,
                "tokens_output": usage.completion_tokens,
                "total_tokens": usage.total_tokens,
            }.items()
            if v is not None
        }
    )

    return response


@checkpoint(type="tool_call")
def tool_call(
    tool_name: str,
    arguments: dict[str, Any],
    cwd: str,
) -> ToolCallResult:
    """Execute a single tool — tracked as a checkpoint."""
    cwd_path = Path(cwd)
    files_before = (
        {p.name for p in cwd_path.iterdir() if p.is_file()}
        if cwd_path.is_dir()
        else set()
    )

    # Save source code as a browsable artifact before execution
    if tool_name == "python_exec" and "code" in arguments:
        _save_html_artifact("script.py", arguments["code"])

    raw_result = dispatch_tool(cwd, tool_name, arguments)

    # Save full file content as a browsable artifact (LLM only sees truncated)
    if tool_name == "read_file" and "path" in arguments:
        try:
            full_content = (Path(cwd) / arguments["path"]).read_text()
            _save_html_artifact(arguments["path"], full_content)
        except Exception as exc:
            kitaru.log(
                artifact_warning=f"Could not save full file artifact for "
                f"{arguments['path']}: {type(exc).__name__}: {exc}"
            )

    # Persist any new files the tool created (HTML, CSV, PNG, etc.)
    save_generated_files(cwd, files_before)

    return ToolCallResult(tool_name=tool_name, output=str(raw_result))


def _save_html_artifact(name: str, code: str) -> None:
    """Save a code string as a syntax-highlighted HTML artifact."""
    escaped = html.escape(code)
    kitaru.save(
        name,
        HTMLString(
            f'<pre style="background:#1e1e1e;color:#d4d4d4;padding:16px;'
            f"border-radius:8px;overflow-x:auto;font-size:13px;"
            f'font-family:monospace;white-space:pre">{escaped}</pre>'
        ),
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(
    image={
        "requirements": ["openai", "anthropic"],
        "apt_packages": ["curl", "ca-certificates"],
    },
)
def coding_agent(task: str) -> str:
    """Agent that solves tasks using tools, then waits for follow-ups.

    1. Receives a task as input
    2. Loops: calls the LLM, executes tool calls, feeds results back
    3. When done, calls ``hand_back`` → waits for the next instruction
    4. Repeats until the tool-call budget is exhausted
    """
    cwd = str(_WORKSPACE)
    kitaru.log(task=task)

    messages: list[dict[str, Any]] = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": f"Working directory: {cwd}\n\nTask: {task}"},
    ]

    results: list[str] = []
    counter = 0  # monotonic counter for unique checkpoint IDs

    for _ in range(MAX_TOOL_ROUNDS):
        response: LLMResponse = llm_call(messages, id=f"llm_{counter}").load()
        counter += 1

        # No tool calls → the LLM is done (shouldn't happen if it follows
        # instructions to call hand_back, but handle it gracefully)
        if not response.has_tool_calls:
            results.append(response.content or "")
            kitaru.log(phase="done")
            break

        messages.append(response.to_message())

        for tc in response.tool_calls:
            args = _parse_args(tc.function.arguments)
            display_name = _make_display_name(
                tc.function.name, args.pop("_display_name", None), counter
            )

            # --- hand_back: task complete, wait for user follow-up ----------
            if tc.function.name == "hand_back":
                summary = args.get("summary", "")
                question = args.get("question", "What would you like to do next?")
                results.append(summary)
                kitaru.log(phase="hand_back")

                follow_up: FollowUp = kitaru.wait(
                    name=f"follow_up_{counter}",
                    timeout=600,
                    schema=FollowUp,
                    question=question,
                )
                if follow_up.is_finished:
                    return "\n\n---\n\n".join(results)
                msg = {"role": "tool", "tool_call_id": tc.id, "content": summary}
                messages.append(msg)
                messages.append({"role": "user", "content": follow_up.message})
                counter += 1
                continue

            # --- ask_user: needs clarification mid-task ---------------------
            if tc.function.name == "ask_user":
                question = args.get("question", "The agent needs your input:")
                answer = kitaru.wait(
                    name=f"ask_{counter}",
                    timeout=600,
                    schema=str,
                    question=question,
                )
                msg = {"role": "tool", "tool_call_id": tc.id, "content": answer}
                messages.append(msg)
                counter += 1
                continue

            # --- Regular tool call ------------------------------------------
            result: ToolCallResult = tool_call(
                tc.function.name, args, cwd, id=display_name
            ).load()
            msg = {"role": "tool", "tool_call_id": tc.id, "content": result.output}
            messages.append(msg)
            counter += 1

    else:
        # Exhausted tool-call budget — ask for a summary
        messages.append(
            {
                "role": "user",
                "content": "Tool call limit reached. Summarize what you accomplished.",
            }
        )
        final: LLMResponse = llm_call(messages, id=f"llm_{counter}").load()
        results.append(final.content or "")

    return "\n\n---\n\n".join(results) if results else "No tasks completed."


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_args(raw: str) -> dict[str, Any]:
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}


def _make_display_name(tool_name: str, llm_name: Any, counter: int) -> str:
    """Build a checkpoint ID from the LLM-suggested name or fall back."""
    if isinstance(llm_name, str) and llm_name.strip():
        return sanitize_display_name(llm_name, counter)
    return f"{tool_name}_{counter}"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


@click.command(help="General-purpose interactive agent.")
@click.argument("task")
def main(task: str) -> None:
    kitaru.configure()
    coding_agent.run(task)


if __name__ == "__main__":
    main()
