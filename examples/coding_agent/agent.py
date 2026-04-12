"""General-purpose coding agent — Kitaru primitives + direct provider SDKs.

An agent loop where each LLM call and tool execution is a visible
checkpoint.  When the LLM returns multiple tool calls, they are
submitted in parallel via ``checkpoint.submit()``.

Memory is flow-scoped (derived from the flow name at runtime). LLM
tools (remember, recall, list_memories) let the model store and
retrieve facts across sessions.  Task summaries are saved after each
hand_back.  When the conversation grows past a threshold, older
messages are compacted via LLM summarization.

Usage::

    cd examples/coding_agent
    python agent.py "Create a plotly chart"

Follow-up input::

    kitaru executions input <exec-id> --value "use population data"
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
from agent_memory import (
    FLOW_BODY_TOOLS,
    compact_messages,
    handle_list_memories,
    handle_recall,
    handle_remember,
    load_session_context,
    save_task_summary,
)
from llm import MAX_TOOL_ROUNDS, MODEL, complete_agent_turn_resilient
from models import FollowUp, LLMResponse, ToolCallResult
from prompts import SYSTEM_PROMPT
from tools import (
    ALL_TOOLS,
    dispatch_tool_with_retries,
    sanitize_display_name,
    save_generated_files,
)
from zenml.types import HTMLString

import kitaru
from kitaru import checkpoint, flow, memory

WORKSPACE = Path(tempfile.mkdtemp(prefix="agent_"))

_FINISHED = object()


# ---------------------------------------------------------------------------
# Checkpoints
# ---------------------------------------------------------------------------


@checkpoint(type="llm_call")
def llm_call(messages: list[dict[str, Any]]) -> LLMResponse:
    """Single LLM completion — tracked as a checkpoint."""
    started = time.perf_counter()
    response, usage = complete_agent_turn_resilient(messages, tools=ALL_TOOLS)
    latency_ms = round((time.perf_counter() - started) * 1000, 3)

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
def run_tool(
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

    if tool_name == "python_exec" and "code" in arguments:
        _save_code_artifact("script.py", arguments["code"])

    try:
        raw_result = dispatch_tool_with_retries(cwd, tool_name, arguments)
    except Exception as exc:
        kitaru.log(
            tool_exception=tool_name,
            error=f"{type(exc).__name__}: {exc}",
        )
        return ToolCallResult(
            tool_name=tool_name,
            output=f"[tool error after retries: {type(exc).__name__}] {exc}",
        )

    if tool_name == "read_file" and "path" in arguments:
        try:
            full_content = (Path(cwd) / arguments["path"]).read_text()
            _save_code_artifact(arguments["path"], full_content)
        except Exception as exc:
            kitaru.log(
                artifact_warning=f"Could not save artifact for "
                f"{arguments['path']}: {type(exc).__name__}: {exc}"
            )

    save_generated_files(cwd, files_before)
    return ToolCallResult(tool_name=tool_name, output=str(raw_result))


def _save_code_artifact(name: str, code: str) -> None:
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
        "requirements": ["openai", "anthropic", "kitaru>=0.3.0"],
        "apt_packages": ["curl", "ca-certificates"],
    },
)
def coding_agent(task: str) -> str:
    """Agent loop: LLM calls, tool execution, memory, and follow-ups."""
    cwd = str(WORKSPACE)
    kitaru.log(task=task)
    memory.configure(scope_type="flow")

    messages = _build_initial_messages(task, cwd)
    results: list[str] = []
    task_count = 0
    step_id = 0

    for _ in range(MAX_TOOL_ROUNDS):
        messages = compact_messages(messages)

        response: LLMResponse = llm_call(messages, id=f"llm_{step_id}").load()
        step_id += 1

        if not response.has_tool_calls:
            results.append(response.content or "")
            kitaru.log(phase="done")
            break

        messages.append(response.to_message())
        parsed, step_id = _parse_and_validate_tool_calls(response, messages, step_id)

        checkpoint_calls = [
            call for call in parsed if call[0].function.name not in FLOW_BODY_TOOLS
        ]
        flow_body_calls = [
            call for call in parsed if call[0].function.name in FLOW_BODY_TOOLS
        ]

        _run_checkpoint_tools(checkpoint_calls, cwd, messages)
        outcome = _run_flow_body_tools(
            flow_body_calls, messages, results, task_count, step_id
        )
        if outcome is _FINISHED:
            return "\n\n---\n\n".join(results)
        task_count, step_id = outcome

    else:
        messages.append(
            {
                "role": "user",
                "content": "Tool call limit reached. Summarize what you accomplished.",
            }
        )
        final: LLMResponse = llm_call(messages, id=f"llm_{step_id}").load()
        results.append(final.content or "")

    return "\n\n---\n\n".join(results) if results else "No tasks completed."


# ---------------------------------------------------------------------------
# Flow helpers
# ---------------------------------------------------------------------------


def _build_initial_messages(task: str, cwd: str) -> list[dict[str, Any]]:
    messages: list[dict[str, Any]] = [
        {"role": "system", "content": SYSTEM_PROMPT},
    ]

    session_context = load_session_context()
    if session_context:
        messages.append({"role": "user", "content": session_context})
        messages.append(
            {
                "role": "assistant",
                "content": "Understood, I have context from our previous sessions.",
            }
        )
        kitaru.log(session_context_loaded=True)

    messages.append(
        {
            "role": "user",
            "content": f"Working directory: {cwd}\n\nTask: {task}",
        }
    )
    return messages


def _parse_and_validate_tool_calls(
    response: LLMResponse,
    messages: list[dict[str, Any]],
    step_id: int,
) -> tuple[list[tuple[Any, dict[str, Any], str]], int]:
    """Parse tool calls from an LLM response.

    Returns (parsed_calls, updated_step_id).  Parse errors are
    appended to *messages* so the LLM can self-heal.
    """
    parsed: list[tuple[Any, dict[str, Any], str]] = []

    for tool_call_request in response.tool_calls:
        raw_args = tool_call_request.function.arguments.strip()
        if not raw_args:
            args: dict[str, Any] = {}
            error = None
        else:
            try:
                args = json.loads(raw_args)
                error = None
            except json.JSONDecodeError as exc:
                args = {}
                error = f"Invalid JSON in tool arguments: {exc}"

        if error:
            kitaru.log(
                tool_parse_error=tool_call_request.function.name,
                detail=error,
            )
            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": tool_call_request.id,
                    "content": error,
                }
            )
            continue

        display_name = args.pop("_display_name", None)
        if isinstance(display_name, str) and display_name.strip():
            checkpoint_id = sanitize_display_name(display_name, step_id)
        else:
            checkpoint_id = f"{tool_call_request.function.name}_{step_id}"
        step_id += 1

        parsed.append((tool_call_request, args, checkpoint_id))

    return parsed, step_id


def _run_checkpoint_tools(
    calls: list[tuple[Any, dict[str, Any], str]],
    cwd: str,
    messages: list[dict[str, Any]],
) -> None:
    """Submit tool calls as checkpoints in parallel, collect results."""
    futures = [
        (
            request,
            run_tool.submit(request.function.name, args, cwd, id=checkpoint_id),
        )
        for request, args, checkpoint_id in calls
    ]

    for request, future in futures:
        try:
            result: ToolCallResult = future.load()
        except Exception as exc:
            kitaru.log(
                tool_checkpoint_error=request.function.name,
                error=f"{type(exc).__name__}: {exc}",
            )
            result = ToolCallResult(
                tool_name=request.function.name,
                output=f"[execution error: {type(exc).__name__}] {exc}",
            )
        messages.append(
            {
                "role": "tool",
                "tool_call_id": request.id,
                "content": result.output,
            }
        )


def _run_flow_body_tools(
    calls: list[tuple[Any, dict[str, Any], str]],
    messages: list[dict[str, Any]],
    results: list[str],
    task_count: int,
    step_id: int,
) -> tuple[int, int] | object:
    """Handle tools that must run in the flow body (memory, HITL).

    Returns ``(updated_task_count, updated_step_id)`` normally, or the
    sentinel ``_FINISHED`` when the user ends the session.
    """
    for request, args, _checkpoint_id in calls:
        name = request.function.name

        if name == "hand_back":
            summary = args.get("summary", "")
            question = args.get("question", "What would you like to do next?")
            results.append(summary)
            kitaru.log(phase="hand_back")

            save_task_summary(summary, task_count)
            task_count += 1

            kitaru.log(follow_up_prompt=question)
            follow_up: FollowUp = kitaru.wait(
                name=None,
                timeout=600,
                schema=FollowUp,
                question=(
                    "Follow-up or finish (see execution logs for the agent's prompt)."
                ),
                metadata={},
            )
            if follow_up.is_finished:
                return _FINISHED
            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": request.id,
                    "content": summary,
                }
            )
            messages.append({"role": "user", "content": follow_up.message})
            step_id += 1

        elif name == "ask_user":
            question = args.get("question", "The agent needs your input:")
            kitaru.log(ask_user_prompt=question)
            answer = kitaru.wait(
                name=None,
                timeout=600,
                schema=str,
                question=(
                    "Input requested (see execution logs for the question text)."
                ),
                metadata={},
            )
            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": request.id,
                    "content": answer,
                }
            )
            step_id += 1

        elif name == "remember":
            messages.append(handle_remember(request.id, args, step_id))
            step_id += 1

        elif name == "recall":
            messages.append(handle_recall(request.id, args, step_id))
            step_id += 1

        elif name == "list_memories":
            messages.append(handle_list_memories(request.id, args, step_id))
            step_id += 1

    return task_count, step_id


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


@click.command(help="General-purpose interactive agent.")
@click.argument("task")
def main(task: str) -> None:
    kitaru.configure(
        # stack="local_remote",
        image={
            "dockerfile": "../../docker/Dockerfile.dev",
            "build_context_root": "../../",
            "platform": "linux/amd64",
        },
    )
    coding_agent.run(task)


if __name__ == "__main__":
    main()
