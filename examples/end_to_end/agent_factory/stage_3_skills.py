"""Stage 3 — the agent's procedure lives in markdown, not Python.

The agent's behavior is no longer hardcoded in the system prompt. The
system prompt only says "find and follow your skill"; the actual
procedure (what shell commands to run, what to summarize, how to
return) lives in `skills/default-agent/SKILL.md` — a host-side
markdown file the operator edits in their IDE.

This is the kami architectural distinctive: **main loop in Python,
capabilities in markdown**. Edit `skills/default-agent/SKILL.md`,
re-run the flow, watch the agent's behavior change without touching
any Python.

The `skill` tool runs *host-side* (not inside the sandbox) — same
pattern as kami. The agent calls `skill(action="list")` to discover
available skill files, then `skill(action="read", path=...)` to fetch
one. Path validation prevents directory escape.

Env-var toggles:

    DISABLE_CACHE=1    force every checkpoint to re-execute (useful when
                       the agent's already cached and you want to see the
                       skill tool actually being called)
"""

import os
from pathlib import Path

from agent_factory.agent import build_agent
from agent_factory.profile import LocalSkillSource, Profile
from agent_factory.sandbox import DockerSandbox

import kitaru
from kitaru.adapters.pydantic_ai import KitaruAgent
from kitaru.runtime import _get_current_execution_id

DISABLE_CACHE = bool(os.environ.get("DISABLE_CACHE"))

_SKILLS_DIR = Path(__file__).parent / "skills"

DEFAULT_PROFILE = Profile(
    name="default",
    system_prompt=(
        "You are a helpful assistant. Your procedure lives in a skill file, "
        "not in this prompt. **Always start by calling `skill(action='list')` "
        "to discover available skills, then `skill(action='read', path=...)` "
        "to fetch the procedure, then follow it exactly using the `exec` "
        "tool.** One tool call per step."
    ),
    model="openai:gpt-5.4-nano",
    allowed_tools={"exec", "skill"},
    # LocalSkillSource = "edit markdown in your IDE, agent picks it up next call".
    # See agent_factory/profile.py for production-shaped alternatives
    # (GitRepoSkillSource, InlineMarkdownSkillSource, etc.).
    skill_source=LocalSkillSource(path=str(_SKILLS_DIR)),
)


@kitaru.flow
def agent_factory_flow() -> str:
    execution_id = _get_current_execution_id() or "local"

    with DockerSandbox(execution_id=execution_id) as sandbox:
        agent = build_agent(DEFAULT_PROFILE, sandbox=sandbox)
        agent = KitaruAgent(agent)
        result = agent.run_sync(
            "Carry out your procedure and return the result."
        )

    print(f"\n{result.output}\n")
    return result.output


if __name__ == "__main__":
    agent_factory_flow.run(cache=False if DISABLE_CACHE else None)
