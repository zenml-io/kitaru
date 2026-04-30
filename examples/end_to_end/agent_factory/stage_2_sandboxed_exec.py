"""Stage 2 — the agent's `exec` tool runs inside a Docker sandbox.

Same agent as stage 1; the only difference is that shell commands now
execute in an isolated container instead of the host process. The agent's
`/workspace` is a named Docker volume that survives pause/resume.

The sandbox prints `[sandbox] ...` lines for every lifecycle event and
every shell command — start, each `exec`, stop — so you can watch it
working in real time. (See README for the docker image build step.)

Env-var toggles:

    DISABLE_CACHE=1    force every checkpoint to re-execute (useful when
                       the agent's already cached and you want to see the
                       sandbox actually running shell commands)
"""

import os

from agent_factory.agent import build_agent
from agent_factory.profile import Profile
from agent_factory.sandbox import DockerSandbox

import kitaru
from kitaru.adapters.pydantic_ai import KitaruAgent
from kitaru.runtime import _get_current_execution_id

DISABLE_CACHE = bool(os.environ.get("DISABLE_CACHE"))

DEFAULT_PROFILE = Profile(
    name="default",
    system_prompt=(
        "You are a helpful assistant with a single tool: `exec`, which runs "
        "shell commands in an isolated container. Use it to investigate "
        "questions the user asks. Explain what you find concisely."
    ),
    model="openai:gpt-5.4-nano",
    allowed_tools={"exec"},
)


@kitaru.flow
def agent_factory_flow(prompt: str) -> str:
    execution_id = _get_current_execution_id() or "local"
    with DockerSandbox(execution_id=execution_id) as sandbox:
        agent = build_agent(DEFAULT_PROFILE, sandbox=sandbox)
        agent = KitaruAgent(agent)
        output = agent.run_sync(prompt).output
        print(f"\n{output}\n")
        return output


if __name__ == "__main__":
    agent_factory_flow.run(
        "Inspect this machine: what's the OS, kernel version, and current "
        "user? Use one shell command per question, then summarize.",
        cache=False if DISABLE_CACHE else None,
    )
