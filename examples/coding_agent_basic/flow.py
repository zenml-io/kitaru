"""General-purpose coding agent — kitaru primitives + LiteLLM.

Agent loop where each LLM call and tool call is a visible checkpoint.
The initial task is passed as a flow parameter. If the LLM needs
clarification it calls the ask_user tool, which triggers kitaru.wait().

    PYTHONPATH=. uv run python -m examples.coding_agent_basic.flow "Create a plotly chart"

Or via the CLI:

    kitaru executions input <exec-id> --wait ask_0 --value "use population data"
    kitaru executions resume <exec-id>
"""

import copy
import html
import json
import re
import tempfile
from pathlib import Path
from typing import Any

import click
from litellm import completion
from pydantic import BaseModel
from zenml.materializers.materializer_registry import materializer_registry
from zenml.types import CSVString, HTMLString, JSONString, MarkdownString

import kitaru
from kitaru import checkpoint, flow

try:
    from .llm import MAX_TOOL_ROUNDS, MODEL
    from .materializers import LLMResponseMaterializer, ToolCallResultMaterializer
    from .tools import ALL_SCHEMAS, dispatch_tool
except ImportError:
    from llm import MAX_TOOL_ROUNDS, MODEL
    from materializers import LLMResponseMaterializer, ToolCallResultMaterializer
    from tools import ALL_SCHEMAS, dispatch_tool


_WORKSPACE = Path(tempfile.mkdtemp(prefix="agent_"))

SYSTEM_PROMPT = """\
You are a capable general-purpose agent. You can solve any task the user gives \
you by combining the available tools.

Your capabilities:
- **File operations**: read, write, edit, search, and list files
- **Shell commands**: run any command in the working directory
- **Python execution**: write and run Python scripts for math, data processing, \
plotting (plotly, matplotlib), analysis, or any computational task
- **Web browsing**: search the web and fetch pages for research, documentation, \
API references, or current information

IMPORTANT rules:
- For ANY HTTP request or web access, ALWAYS use the web_fetch or web_search \
tools. NEVER write Python code (requests, urllib, httpx, etc.) to make HTTP \
requests — use the dedicated web tools instead.
- python_exec is for computation, data processing, and file generation ONLY — \
not for network I/O.
- python_exec runs in a minimal environment with ONLY the standard library. \
Any third-party package (numpy, pandas, plotly, matplotlib, scipy, etc.) MUST \
be declared in PEP 723 inline script metadata at the very top of the script. \
ALWAYS include this block — scripts without it WILL fail for any non-stdlib import:
  # /// script
  # dependencies = ["plotly", "pandas", "numpy"]
  # ///
- If web_search returns poor results, try web_fetch with a direct URL instead.
- If a tool returns an error, report the error honestly. Do NOT claim the \
environment is restricted — diagnose the specific failure and try a \
different approach.
- When you need clarification or a decision from the user, call ask_user with \
a clear question. Do NOT guess — ask.
- When you have completed a task, ALWAYS call hand_back with a summary and a \
question for the user. Do NOT just respond with text — use hand_back so the \
user can give you follow-up instructions.

Guidelines:
- Think step by step. Break complex problems into smaller parts.
- For math/computation: write a Python script with python_exec rather than \
trying to compute in your head.
- For visualizations: use python_exec to write a script that generates the \
output (e.g. plotly write_html, matplotlib savefig). Save files to the \
working directory.
- For research: use web_search to find information, then web_fetch to read \
specific pages.
- For code tasks: read relevant files first, then make targeted edits.
- Prefer edit_file over write_file for existing files (smaller, safer edits).
- Run verification commands after making changes.
- Report what you did, key results, and where any output files were saved.\
"""

# Add ask_user to the tool schemas sent to the LLM
_ASK_USER_SCHEMA: dict[str, Any] = {
    "type": "function",
    "function": {
        "name": "ask_user",
        "description": (
            "Ask the user a question and wait for their response. "
            "Use when you need clarification, a decision, or additional "
            "information to proceed with the current task."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "question": {
                    "type": "string",
                    "description": "The question to ask the user",
                },
            },
            "required": ["question"],
        },
    },
}

_HAND_BACK_SCHEMA: dict[str, Any] = {
    "type": "function",
    "function": {
        "name": "hand_back",
        "description": (
            "Hand control back to the user after completing the current task. "
            "You MUST call this tool when you are done with a task instead of "
            "just responding with text. Provide a summary of what you did and "
            "a suggested next step or question for the user."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "summary": {
                    "type": "string",
                    "description": "A brief summary of what you accomplished",
                },
                "question": {
                    "type": "string",
                    "description": (
                        "A question or prompt for the user about what to do next, "
                        "e.g. 'Would you like me to refine the chart?' or "
                        "'What should I work on next?'"
                    ),
                },
            },
            "required": ["summary", "question"],
        },
    },
}

def _inject_display_name(schemas: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Add a _display_name parameter to every tool schema.

    The LLM fills this with a short, descriptive label (e.g.
    "fetch_population_data") which becomes the checkpoint ID in the
    dashboard.
    """
    augmented: list[dict[str, Any]] = []
    for schema in schemas:
        s = copy.deepcopy(schema)
        props = s["function"]["parameters"]["properties"]
        props["_display_name"] = {
            "type": "string",
            "description": (
                "A short, descriptive snake_case label for this tool call "
                "(e.g. 'fetch_population_data', 'generate_pyramid_chart', "
                "'search_demographics'). Used as the step name in the dashboard."
            ),
        }
        augmented.append(s)
    return augmented


_ALL_TOOLS = _inject_display_name([*ALL_SCHEMAS, _ASK_USER_SCHEMA, _HAND_BACK_SCHEMA])


# ---------------------------------------------------------------------------
# Checkpoint response models
# ---------------------------------------------------------------------------


class FollowUp(BaseModel):
    """Schema for the follow-up wait after the agent completes a task."""

    message: str = ""


class ToolCallFunction(BaseModel):
    name: str
    arguments: str


class ToolCallRequest(BaseModel):
    id: str
    type: str = "function"
    function: ToolCallFunction


class LLMResponse(BaseModel):
    """Normalized LLM response that round-trips cleanly through ZenML."""

    role: str
    content: str | None = None
    tool_calls: list[ToolCallRequest] | None = None

    @property
    def has_tool_calls(self) -> bool:
        return self.tool_calls is not None and len(self.tool_calls) > 0

    def to_message(self) -> dict[str, Any]:
        """Convert to a dict suitable for the LiteLLM messages list."""
        msg: dict[str, Any] = {"role": self.role, "content": self.content}
        if self.has_tool_calls:
            msg["tool_calls"] = [tc.model_dump() for tc in self.tool_calls]  # type: ignore[union-attr]
        return msg


class ToolCallResult(BaseModel):
    """Normalized tool call result."""

    tool_name: str
    output: str


# ---------------------------------------------------------------------------
# Materializer registration — must happen after model classes are defined
# but before any checkpoint runs. This overrides the default
# PydanticMaterializer so our save_visualizations() is used.
# ---------------------------------------------------------------------------

LLMResponseMaterializer.ASSOCIATED_TYPES = (LLMResponse,)
ToolCallResultMaterializer.ASSOCIATED_TYPES = (ToolCallResult,)
materializer_registry.register_and_overwrite_type(LLMResponse, LLMResponseMaterializer)
materializer_registry.register_and_overwrite_type(ToolCallResult, ToolCallResultMaterializer)


# ---------------------------------------------------------------------------
# Checkpoints
# ---------------------------------------------------------------------------


@checkpoint(type="llm_call")
def llm_call(messages: list[dict[str, Any]]) -> LLMResponse:
    """Single LLM completion call tracked as a checkpoint."""
    response = completion(model=MODEL, messages=messages, tools=_ALL_TOOLS)
    msg = response.choices[0].message

    tool_calls = None
    if getattr(msg, "tool_calls", None):
        tool_calls = [
            ToolCallRequest(
                id=tc.id,
                type="function",
                function=ToolCallFunction(
                    name=tc.function.name,
                    arguments=tc.function.arguments,
                ),
            )
            for tc in msg.tool_calls
        ]

    return LLMResponse(role=msg.role, content=msg.content, tool_calls=tool_calls)


_SAVE_EXTENSIONS: dict[str, type] = {
    ".html": HTMLString,
    ".md": MarkdownString,
    ".csv": CSVString,
    ".json": JSONString,
}


def _save_generated_files(cwd: str, before: set[str]) -> None:
    """Save files created in cwd since `before` as separate artifacts."""
    cwd_path = Path(cwd)
    if not cwd_path.is_dir():
        return

    for path in sorted(cwd_path.iterdir()):
        if not path.is_file() or path.name in before:
            continue
        ext = path.suffix.lower()
        wrapper = _SAVE_EXTENSIONS.get(ext)
        if wrapper is None:
            continue
        try:
            content = path.read_text(errors="replace")
            kitaru.save(path.name, wrapper(content))
            print(f"Saved generated file as artifact: {path.name} ({wrapper.__name__})")
        except Exception as exc:
            print(f"Failed to save generated file {path.name}: {exc}")
            continue


@checkpoint(type="tool_call")
def tool_call(
    tool_name: str,
    arguments: dict[str, Any],
    cwd: str,
) -> ToolCallResult:
    """Execute a single tool call tracked as a checkpoint."""
    cwd_path = Path(cwd)
    before = {p.name for p in cwd_path.iterdir() if p.is_file()} if cwd_path.is_dir() else set()

    # Save the source code as an artifact before execution
    if tool_name == "python_exec" and "code" in arguments:
        code = arguments["code"]
        escaped = html.escape(code)
        kitaru.save(
            "script.py",
            HTMLString(
                f'<pre style="background:#1e1e1e;color:#d4d4d4;padding:16px;'
                f'border-radius:8px;overflow-x:auto;font-size:13px;'
                f'font-family:monospace;white-space:pre">{escaped}</pre>'
            ),
        )

    raw_result = dispatch_tool(cwd, tool_name, arguments)

    # Save any new files the tool created as separate typed artifacts
    _save_generated_files(cwd, before)

    return ToolCallResult(tool_name=tool_name, output=str(raw_result))


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(
    image={
        "base_image": "strickvl/kitaru-dev:latest",
        "requirements": ["litellm"],
        "apt_packages": ["curl", "ca-certificates"],
    },
)
def coding_agent_basic(task: str) -> str:
    """General-purpose agent that solves tasks using available tools.

    The initial task is passed as a parameter. After the agent completes
    a task it hands control back to the user via kitaru.wait() so
    they can give follow-up instructions or quit.
    """
    cwd = str(_WORKSPACE)
    kitaru.log(task=task)

    messages: list[dict[str, Any]] = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {
            "role": "user",
            "content": f"Working directory: {cwd}\n\nTask: {task}",
        },
    ]

    results: list[str] = []
    ask_count = 0
    hand_back_count = 0
    tool_calls_made = 0
    llm_count = 0

    for _ in range(MAX_TOOL_ROUNDS):
        response: LLMResponse = llm_call(
            messages, id=f"llm_{llm_count}"
        ).load()
        llm_count += 1

        if not response.has_tool_calls:
            # LLM responded without calling hand_back — treat as final
            results.append(response.content or "")
            kitaru.log(phase="done", tool_calls=tool_calls_made)
            break

        messages.append(response.to_message())

        for tc in response.tool_calls:  # type: ignore[union-attr]
            try:
                tc_args = json.loads(tc.function.arguments)
            except json.JSONDecodeError:
                tc_args = {}

            # Extract LLM-provided display name, fall back to tool_name + counter
            display_name = tc_args.pop("_display_name", None)
            if not display_name or not isinstance(display_name, str):
                display_name = f"{tc.function.name}_{tool_calls_made}"
            else:
                # Sanitize: lowercase, replace non-alnum with underscore
                display_name = re.sub(r"[^a-z0-9_]", "_", display_name.strip().lower())
                display_name = f"{display_name}_{tool_calls_made}"

            # hand_back: LLM is done, wait for user follow-up
            if tc.function.name == "hand_back":
                summary = tc_args.get("summary", "")
                question = tc_args.get("question", "What would you like to do next?")
                results.append(summary)
                kitaru.log(phase="hand_back", tool_calls=tool_calls_made)

                follow_up: FollowUp = kitaru.wait(
                    name=f"follow_up_{hand_back_count}",
                    timeout=600,
                    schema=FollowUp,
                    question=question,
                )
                hand_back_count += 1

                # Feed the user's follow-up back into the conversation
                messages.append(
                    {"role": "tool", "tool_call_id": tc.id, "content": summary}
                )
                messages.append(
                    {"role": "user", "content": follow_up.message}
                )
                tool_calls_made += 1
                continue

            # ask_user: LLM needs clarification mid-task
            if tc.function.name == "ask_user":
                question = tc_args.get("question", "The agent needs your input:")
                print(f"Asking user: {question}")
                user_answer = kitaru.wait(
                    name=f"ask_{ask_count}",
                    timeout=600,
                    schema=str,
                    question=question,
                )
                ask_count += 1
                messages.append(
                    {"role": "tool", "tool_call_id": tc.id, "content": user_answer}
                )
                tool_calls_made += 1
                continue

            # Regular tool call
            print(f"Tool call: {display_name} — {tc.function.name}({tc_args})")
            result: ToolCallResult = tool_call(
                tc.function.name,
                tc_args,
                cwd,
                id=display_name,
            ).load()

            print(f"Tool result [{tc.function.name}]: {result.output[:500]}")
            tool_calls_made += 1

            messages.append(
                {"role": "tool", "tool_call_id": tc.id, "content": result.output}
            )
    else:
        # Exhausted rounds
        messages.append(
            {
                "role": "user",
                "content": "Tool call limit reached. Summarize what you accomplished.",
            }
        )
        final: LLMResponse = llm_call(messages, id=f"llm_{llm_count}").load()
        llm_count += 1
        results.append(final.content or "")

    return "\n\n---\n\n".join(results) if results else "No tasks completed."


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


@click.command(help="General-purpose interactive agent.")
@click.argument("task")
def main(task: str) -> None:
    coding_agent_basic.run(task)


if __name__ == "__main__":
    main()
