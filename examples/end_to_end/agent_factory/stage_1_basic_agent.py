"""Stage 1 — a durable PydanticAI agent in ~30 lines.

The chapter 1 hero demo: pydantic-ai gives the agent loop, kitaru gives
durable execution. Together: durable agents without learning a graph DSL
or rewriting your control flow as a state machine.

Run the happy path:

    python stage_1_basic_agent.py

Watch durability work — kill mid-run and resume:

    1. python stage_1_basic_agent.py &
    2. kill %1                          # before it finishes
    3. kitaru executions list           # the run is `running` (orphaned)
    4. kitaru executions resume <id>    # the turn re-runs and the flow
                                        # completes; without kitaru, the
                                        # killed process would have lost
                                        # the run entirely

Stage 1 uses *turn mode* (the adapter's default): one aggregating checkpoint
per `agent.run_sync()`. Per-call cache (granular mode) lands in a later
chapter where there are explicit `@checkpoint` boundaries to amortize.
"""

from agent_factory.agent import build_agent
from agent_factory.profile import Profile

import kitaru
from kitaru.adapters.pydantic_ai import KitaruAgent

DEFAULT_PROFILE = Profile(
    name="default",
    system_prompt=(
        "You are a helpful assistant with a single tool: `exec`, which runs "
        "shell commands in the host process. Use it to investigate questions "
        "the user asks. Explain what you find concisely."
    ),
    model="openai:gpt-5.4-nano",
    allowed_tools={"exec"},
)


@kitaru.flow
def agent_factory_flow(prompt: str) -> str:
    # The kitaru ↔ pydantic-ai integration seam, in plain sight:
    # build_agent() returns a vanilla pydantic-ai Agent; KitaruAgent
    # wraps it for durable execution, capture, and HITL bridging.
    agent = build_agent(DEFAULT_PROFILE)
    agent = KitaruAgent(agent)
    return agent.run_sync(prompt).output


if __name__ == "__main__":
    handle = agent_factory_flow.run(
        # A multi-step prompt that needs ~3-5 tool calls — gives you a window to
        # kill the process between calls and see resume serve cached work.
        "Inspect this machine: what's the OS, the kernel version, the current "
        "user, and how many processes are running? Use one shell command per "
        "question. Summarize at the end."
    )
    print(handle.wait().output)
