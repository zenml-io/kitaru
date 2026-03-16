import logging
import shlex
from pathlib import Path

import click
from pydantic_ai import Agent

import kitaru
from kitaru import KitaruClient, checkpoint, flow
from kitaru.adapters import pydantic_ai as kp

from .skills import select_skills
from .tools import CODER_TOOLS
from .utils import (
    CODER_PROMPT,
    MODEL,
    AgentMemory,
    coder,
    planner,
    reflector,
    researcher,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Memory — persistent scratchpad across runs
# ---------------------------------------------------------------------------


@checkpoint
def load_memory() -> AgentMemory:
    """Load memory from most recent completed execution, or start fresh."""
    client = KitaruClient()
    try:
        prev = client.executions.latest(flow="coding_agent", status="completed")
    except LookupError:
        return AgentMemory()
    try:
        return kitaru.load(prev.exec_id, "agent_memory")
    except Exception:
        logger.warning(
            "Failed to load memory from execution %s, starting fresh",
            prev.exec_id,
            exc_info=True,
        )
        return AgentMemory()


@checkpoint
def save_memory(memory: AgentMemory) -> AgentMemory:
    """Persist memory for future runs."""
    kitaru.save("agent_memory", memory, type="context")
    return memory


@checkpoint
def load_context(context_paths: list[str]) -> str:
    """Read external reference files and return them as a labeled text block.

    Files are read eagerly so their contents are cached by the checkpoint
    for replay. Glob patterns are expanded. Missing files are noted inline.
    """
    if not context_paths:
        return ""

    expanded: list[Path] = []
    for raw in context_paths:
        p = Path(raw).expanduser()
        if "*" in raw or "?" in raw:
            expanded.extend(sorted(Path(".").glob(raw)))
        elif p.is_dir():
            expanded.extend(sorted(p.rglob("*")))
        else:
            expanded.append(p)

    sections: list[str] = []
    for fp in expanded:
        if not fp.is_file():
            sections.append(f"--- {fp} ---\n[not found or not a file]")
            continue
        try:
            text = fp.read_text()
            # Cap individual files to avoid blowing up context
            if len(text) > 30_000:
                text = text[:30_000] + f"\n... [truncated, {len(text)} chars total]"
            sections.append(f"--- {fp} ---\n{text}")
        except Exception as exc:
            sections.append(f"--- {fp} ---\n[error reading file: {exc}]")

    block = "\n\n".join(sections)
    kitaru.log(context_files=len(sections), context_chars=len(block))
    return block


# ---------------------------------------------------------------------------
# Core checkpoints
# ---------------------------------------------------------------------------


@checkpoint(type="llm_call")
def research(task: str, cwd: str, memory: AgentMemory, context: str = "") -> str:
    """Read the codebase and build context."""
    memory_ctx = ""
    if memory.conventions or memory.decisions or memory.notes:
        parts = []
        if memory.conventions:
            parts.append(f"Conventions: {memory.conventions}")
        if memory.decisions:
            parts.append(f"Decisions: {memory.decisions}")
        if memory.notes:
            parts.append(f"Notes: {memory.notes}")
        memory_ctx = "\n\nMEMORY FROM PAST RUNS:\n" + "\n".join(f"- {p}" for p in parts)

    context_ctx = ""
    if context:
        context_ctx = (
            "\n\nREFERENCE FILES (provided as extra context outside the working "
            "directory — use these to inform your analysis):\n" + context
        )

    result = researcher.run_sync(
        f"Analyze this codebase for the following task. Read relevant files, "
        f"identify which files are involved, current behavior, and constraints. "
        f"Do NOT make changes.\n\nTASK: {task}{memory_ctx}{context_ctx}",
        deps=cwd,
    )
    kitaru.log(phase="research", result_length=len(result.output))
    return result.output


@checkpoint(type="llm_call")
def plan(task: str, analysis: str, cwd: str, context: str = "") -> str:
    """Create an implementation plan from the research output.

    The planner has no tools — it works only from the supplied analysis.
    """
    context_ctx = ""
    if context:
        context_ctx = "\n\nREFERENCE FILES (provided as extra context):\n" + context

    result = planner.run_sync(
        f"Create a numbered implementation plan for this task.\n\n"
        f"TASK: {task}\n\nRESEARCH ANALYSIS:\n{analysis}{context_ctx}\n\n"
        f"Include: files to modify, specific changes in each file, order of "
        f"operations, and verification steps.",
        deps=cwd,
    )
    kitaru.log(phase="plan", result_length=len(result.output))
    return result.output


@checkpoint(type="llm_call")
def implement(
    task: str,
    analysis: str,
    implementation_plan: str,
    cwd: str,
    skill_names: list[str] | None,
    mcp_servers: list | None,
    context: str = "",
) -> str:
    """Execute the approved plan. Most expensive phase — replay target."""
    active_skills = select_skills(task, analysis, explicit=skill_names)
    if active_skills:
        kitaru.log(active_skills=[s.name for s in active_skills])
    if active_skills or mcp_servers:
        extra_prompt = "\n\n".join(f"## {s.name}\n{s.prompt}" for s in active_skills)
        base_tool_names = {f.__name__ for f in CODER_TOOLS}
        extra_tools = [
            t
            for s in active_skills
            for t in s.tools
            if getattr(t, "__name__", None) not in base_tool_names
        ]
        prompt = CODER_PROMPT + ("\n\n" + extra_prompt if extra_prompt else "")
        agent = kp.wrap(
            Agent(
                MODEL,
                tools=list(CODER_TOOLS) + extra_tools,
                toolsets=list(mcp_servers or []),
                system_prompt=prompt,
            ),
            name="coder",
            tool_capture_config={"mode": "metadata_only"},
        )
    else:
        agent = coder

    context_ctx = ""
    if context:
        context_ctx = (
            f"\n\nREFERENCE FILES (extra context — read-only, outside the "
            f"working directory):\n{context}"
        )

    result = agent.run_sync(
        f"Execute this plan. Make the code changes, then verify.\n\n"
        f"TASK: {task}\n\n"
        f"RESEARCH CONTEXT:\n{analysis}\n\n"
        f"PLAN:\n{implementation_plan}{context_ctx}",
        deps=cwd,
    )
    kitaru.log(phase="implement", result_length=len(result.output))
    return result.output


@checkpoint(type="llm_call")
def reflect(task: str, result: str, memory: AgentMemory, cwd: str) -> AgentMemory:
    """Extract lessons learned and update memory for future runs."""
    r = reflector.run_sync(
        f"TASK: {task}\n\n"
        f"IMPLEMENTATION RESULT:\n{result}\n\n"
        f"CURRENT MEMORY: {memory!r}",
        deps=cwd,
    )
    return r.output


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow
def coding_agent(
    task: str,
    cwd: str = ".",
    mcp_servers: list | None = None,
    skills: list[str] | None = None,
    context_paths: list[str] | None = None,
) -> str:
    """research -> plan -> [human approval] -> implement -> reflect"""
    memory = load_memory()
    context = load_context(context_paths or [])
    analysis = research(task, cwd, memory, context)
    implementation_plan = plan(task, analysis, cwd, context)

    kitaru.wait(
        name="approve_plan",
        question=f"Approve this plan?\n\n{implementation_plan}",
        metadata={"task": task},
    )

    result = implement(
        task, analysis, implementation_plan, cwd, skills, mcp_servers, context
    )
    updated_memory = reflect(task, result, memory, cwd)
    save_memory(updated_memory)
    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_mcp_server(value: str) -> object:
    """Parse an MCP server spec into a PydanticAI toolset.

    URLs (http:// or https://) become SSE servers.
    Everything else is treated as a shell command for stdio transport.
    """
    from pydantic_ai.mcp import MCPServerSSE, MCPServerStdio

    if value.startswith(("http://", "https://")):
        return MCPServerSSE(url=value)
    parts = shlex.split(value)
    return MCPServerStdio(parts[0], args=parts[1:])


@click.command(help="Durable coding agent (PydanticAI).")
@click.option("--task", required=True, help="The coding task to execute")
@click.option("--cwd", default=".", help="The current working directory")
@click.option(
    "--skills",
    default=None,
    help="Comma-separated skill names (e.g. testing,docs). Auto-selected if omitted.",
)
@click.option(
    "--mcp",
    multiple=True,
    help=(
        "MCP server(s) to add as tool sources (repeatable). "
        "URLs use SSE transport; other values use stdio "
        "(e.g. 'npx -y @modelcontextprotocol/server-filesystem /tmp')."
    ),
)
@click.option(
    "--context",
    multiple=True,
    help=(
        "File path(s) to include as reference context (repeatable). "
        "Files are read upfront and injected into all agent prompts. "
        "Supports globs and directories (recursively included). "
        "Example: --context ../shared/types.py --context ../docs/*.md"
    ),
)
def main(
    task: str,
    cwd: str,
    skills: str | None,
    mcp: tuple[str, ...],
    context: tuple[str, ...],
) -> None:
    skill_list = [s.strip() for s in skills.split(",")] if skills else None
    mcp_servers = [_parse_mcp_server(v) for v in mcp] if mcp else None
    context_paths = list(context) if context else None
    coding_agent.run(
        task,
        cwd,
        mcp_servers=mcp_servers,
        skills=skill_list,
        context_paths=context_paths,
    )


if __name__ == "__main__":
    main()
