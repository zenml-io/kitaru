"""Stage 2 — the agent's `exec` tool runs inside a Docker sandbox.

The flow runs the agent **twice**, sharing a single `DockerSandbox` (one
container, one persistent bash) across both turns. Turn 1 changes shell
state (`cd /tmp`, `export GREETING=...`); turn 2 reads it back. The
persistent shell makes turn 2's `pwd` return `/tmp` and `echo $GREETING`
return the message — within a single flow run. (Across flow runs, shell
state is intentionally not preserved — bash commands have side effects
that a snapshot can't replay safely.)

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


_TURN_1_PROMPT = (
    "Investigate this machine: report the OS, kernel version, and current "
    "user (one shell command per question). Then cd into /tmp and "
    "`export GREETING='hello from turn 1'`. Confirm both worked."
)

_TURN_2_PROMPT = (
    "Run `pwd` and `echo \"$GREETING\"`. Tell me what you see — and what "
    "that tells you about how the shell behaves between turns."
)


@kitaru.flow
def agent_factory_flow() -> str:
    execution_id = _get_current_execution_id() or "local"

    with DockerSandbox(execution_id=execution_id) as sandbox:
        agent = build_agent(DEFAULT_PROFILE, sandbox=sandbox)
        agent = KitaruAgent(agent)

        # Turn 1: changes shell state (cd, export).
        turn_1 = agent.run_sync(_TURN_1_PROMPT)

        # Turn 2: reads the state back. The persistent shell makes turn 2's
        # `pwd` return /tmp and `echo $GREETING` return the message —
        # without it, every turn starts in /workspace with empty env.
        turn_2 = agent.run_sync(_TURN_2_PROMPT)

    final = (
        f"# Turn 1 (set state)\n\n{turn_1.output}\n\n"
        f"# Turn 2 (read state)\n\n{turn_2.output}"
    )
    print(f"\n{final}\n")
    return final


if __name__ == "__main__":
    agent_factory_flow.run(cache=False if DISABLE_CACHE else None)
