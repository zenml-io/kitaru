"""General-purpose coding agent — Kitaru primitives + direct provider SDKs.

An agent loop where each LLM call and tool execution is a visible
checkpoint. When the LLM returns multiple tool calls, they are submitted
in parallel via ``checkpoint.submit()``. The initial task is passed as a
flow parameter. If the LLM needs clarification it calls ``ask_user``,
which triggers ``kitaru.wait()``. When done, it calls ``hand_back`` and
waits for a follow-up instruction.

Usage::

    cd examples/coding_agent
    python agent.py "Create a plotly chart"

Or supply follow-up input via the CLI::

    kitaru executions input <exec-id> --value "use population data"
    kitaru executions resume <exec-id>

Optional env (see also ``llm.py``): ``CODING_AGENT_TOOL_TRANSIENT_RETRIES`` (default
``2``) for transient network failures during tool execution; LLM backoff and
recovery rounds are controlled by ``CODING_AGENT_LLM_MAX_RETRIES``,
``CODING_AGENT_LLM_RECOVERY_ROUNDS``, etc.
"""

import html
import json
import os
import tempfile
import time
from pathlib import Path
from typing import Any

import click
import materializers as _materializers  # noqa: F401 — registers custom materializers
from llm import MAX_TOOL_ROUNDS, MODEL, complete_agent_turn_resilient
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

_TOOL_TRANSIENT_RETRIES: int = max(
    1, int(os.environ.get("CODING_AGENT_TOOL_TRANSIENT_RETRIES", "2"))
)


def _is_transient_error(exc: BaseException) -> bool:
    """Return True for network-ish failures that may succeed on retry."""
    if isinstance(exc, (TimeoutError, ConnectionError, OSError)):
        return True
    name = type(exc).__name__
    if "Timeout" in name or "Connection" in name:
        return True
    mod = type(exc).__module__
    if mod.startswith(("httpx", "httpcore", "urllib3", "requests")):
        return name in (
            "ConnectError",
            "ReadTimeout",
            "ConnectTimeout",
            "RemoteProtocolError",
            "ProtocolError",
        )
    return False


def _dispatch_tool_with_retries(
    cwd: str,
    tool_name: str,
    arguments: dict[str, Any],
) -> Any:
    """Run ``dispatch_tool`` with a few retries on transient failures."""
    last: Exception | None = None
    for attempt in range(_TOOL_TRANSIENT_RETRIES):
        try:
            return dispatch_tool(cwd, tool_name, arguments)
        except Exception as exc:
            last = exc
            if attempt < _TOOL_TRANSIENT_RETRIES - 1 and _is_transient_error(exc):
                time.sleep(min(2**attempt, 8))
                continue
            raise
    assert last is not None
    raise last


# ---------------------------------------------------------------------------
# Checkpoints
# ---------------------------------------------------------------------------


@checkpoint(type="llm_call")
def llm_call(messages: list[dict[str, Any]]) -> LLMResponse:
    """Single LLM completion — tracked as a checkpoint."""
    started_at = time.perf_counter()
    response, usage = complete_agent_turn_resilient(messages, tools=ALL_TOOLS)
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

    try:
        raw_result = _dispatch_tool_with_retries(cwd, tool_name, arguments)
    except Exception as exc:
        kitaru.log(
            tool_exception=tool_name,
            error=f"{type(exc).__name__}: {exc}",
        )
        return ToolCallResult(
            tool_name=tool_name,
            output=(f"[tool error after retries: {type(exc).__name__}] {exc}"),
        )

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
        "requirements": ["openai", "anthropic", "kitaru>=0.3.0"],
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

        _INTERACTIVE = ("hand_back", "ask_user")
        parsed_calls: list[tuple[Any, dict[str, Any], str]] = []
        for tc in response.tool_calls:
            args, parse_err = _parse_tool_arguments(tc.function.arguments)
            if parse_err:
                kitaru.log(tool_parse_error=tc.function.name, detail=parse_err)
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tc.id,
                        "content": parse_err,
                    }
                )
                continue
            display_name = _make_display_name(
                tc.function.name, args.pop("_display_name", None), counter
            )
            counter += 1
            parsed_calls.append((tc, args, display_name))

        regular = [
            (tc, a, dn)
            for tc, a, dn in parsed_calls
            if tc.function.name not in _INTERACTIVE
        ]
        special = [
            (tc, a, dn)
            for tc, a, dn in parsed_calls
            if tc.function.name in _INTERACTIVE
        ]

        futures = [
            (tc, tool_call.submit(tc.function.name, args, cwd, id=display_name))
            for tc, args, display_name in regular
        ]
        for tc, future in futures:
            try:
                result: ToolCallResult = future.load()
            except Exception as exc:
                kitaru.log(
                    tool_checkpoint_error=tc.function.name,
                    error=f"{type(exc).__name__}: {exc}",
                )
                result = ToolCallResult(
                    tool_name=tc.function.name,
                    output=(f"[execution error: {type(exc).__name__}] {exc}"),
                )
            msg = {"role": "tool", "tool_call_id": tc.id, "content": result.output}
            messages.append(msg)

        # --- Handle special tools sequentially ------------------------------
        for tc, args, _dn in special:
            if tc.function.name == "hand_back":
                summary = args.get("summary", "")
                question = args.get("question", "What would you like to do next?")
                results.append(summary)
                kitaru.log(phase="hand_back")

                kitaru.log(follow_up_prompt=question)
                follow_up: FollowUp = kitaru.wait(
                    name=None,
                    timeout=600,
                    schema=FollowUp,
                    question=(
                        "Follow-up or finish "
                        "(see execution logs for the agent's prompt)."
                    ),
                    metadata={},
                )
                if follow_up.is_finished:
                    return "\n\n---\n\n".join(results)
                msg = {"role": "tool", "tool_call_id": tc.id, "content": summary}
                messages.append(msg)
                messages.append({"role": "user", "content": follow_up.message})
                counter += 1

            elif tc.function.name == "ask_user":
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
                msg = {"role": "tool", "tool_call_id": tc.id, "content": answer}
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


def _parse_tool_arguments(raw: str) -> tuple[dict[str, Any], str | None]:
    """Parse tool JSON; on failure return ``({}, error_message)`` for self-heal."""
    stripped = raw.strip()
    if not stripped:
        return {}, None
    try:
        return json.loads(stripped), None
    except json.JSONDecodeError as exc:
        return {}, f"Invalid JSON in tool arguments: {exc}"


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
    coding_agent.run(task)


if __name__ == "__main__":
    main()
