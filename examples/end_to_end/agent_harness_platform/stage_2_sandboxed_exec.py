"""Stage 2 — the agent's `exec` tool runs inside a Docker sandbox.

The flow runs the agent **twice**, sharing a single `DockerSandbox` (one
container, one persistent bash) across both turns. Turn 1 investigates
the machine and `cd`s into `/tmp`. Turn 2 writes a summary file in the
current directory — and because the persistent shell carried turn 1's
`cd` across the turn boundary, the file lands in `/tmp/summary.txt`
without turn 2 ever stating an absolute path. (Across flow runs, shell
state is intentionally not preserved — bash commands have side effects
that a snapshot can't replay safely.)

The sandbox prints `[sandbox] ...` lines for every lifecycle event and
every shell command — start, each `exec`, stop — so you can watch it
working in real time. (See README for the docker image build step.)

Env-var toggles:

    DISABLE_CACHE=1    force every checkpoint to re-execute (useful when
                       the agent's already cached and you want to see the
                       sandbox actually running shell commands)
    FORCE_FAILURE=1    raise between turn 1 and turn 2. Turn 1's
                       checkpoint is cached across the failure (same as
                       stage 1) — re-run without FORCE_FAILURE and turn
                       1 is served from cache, no LLM call.
"""

import os

from agent_harness_platform.agent import build_agent
from agent_harness_platform.profile import Profile
from agent_harness_platform.sandbox import DockerSandbox

import kitaru
from kitaru.adapters.pydantic_ai import KitaruAgent
from kitaru.runtime import _get_current_execution_id

DISABLE_CACHE = bool(os.environ.get("DISABLE_CACHE"))
FORCE_FAILURE = bool(os.environ.get("FORCE_FAILURE"))

DEFAULT_PROFILE = Profile(
    name="default",
    system_prompt=(
        "You are a helpful assistant with a single tool: `exec`, which runs "
        "shell commands in an isolated container. Use it to investigate "
        "questions the user asks. Explain what you find concisely."
    ),
    model="openai:gpt-5-nano",
    allowed_tools={"exec"},
)


_TURN_1_PROMPT = (
    "Investigate this machine: report the OS, kernel version, and current "
    "user (one shell command per question). Then cd into /tmp."
)

_TURN_2_PROMPT = (
    "Write a brief 2-sentence summary of what you found to a file called "
    "summary.txt in the current directory. Then `cat` it back to confirm "
    "what's there."
)


@kitaru.flow
def agent_harness_platform_flow() -> str:
    execution_id = _get_current_execution_id() or "local"

    with DockerSandbox(execution_id=execution_id) as sandbox:
        agent = build_agent(DEFAULT_PROFILE, sandbox=sandbox)
        # Turn mode for log-clarity — see stage 1's note. Production
        # forks should drop the kwarg for per-call durability.
        agent = KitaruAgent(agent, granular_checkpoints=False)

        # Turn 1: investigates the machine and cd's into /tmp.
        turn_1 = agent.run_sync(_TURN_1_PROMPT)

        if FORCE_FAILURE:
            # Same toggle as stage 1: turn 1's checkpoint is cached across
            # the failure, so a re-run skips the LLM calls. Turn 2 on
            # re-run runs against a *fresh* shell though — kitaru caches
            # the agent's reasoning, not the bash side effects.
            raise RuntimeError(
                "Simulated downstream blip between the two agent turns. "
                "Re-run without FORCE_FAILURE — turn 1 will be served "
                "from cache."
            )

        # Turn 2: writes summary.txt without naming a path — lands in /tmp
        # because the persistent shell carried turn 1's `cd /tmp`.
        turn_2 = agent.run_sync(_TURN_2_PROMPT)

    final = (
        f"# Turn 1 (investigate + cd)\n\n{turn_1.output}\n\n"
        f"# Turn 2 (write summary in the cwd turn 1 left)\n\n{turn_2.output}"
    )
    print(f"\n{final}\n")
    return final


if __name__ == "__main__":
    agent_harness_platform_flow.run(cache=False if DISABLE_CACHE else None)
