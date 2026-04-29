"""Stage 1 — a durable PydanticAI agent in ~30 lines.

The chapter 1 hero demo: pydantic-ai gives the agent loop, kitaru gives
durable execution. Together: durable agents without learning a graph DSL.

Run it:

    python stage_1_basic_agent.py

Watch durability work — kill it mid-run and resume:

    1. Edit the prompt to something that needs a few tool calls.
    2. python stage_1_basic_agent.py &
       Watch the dashboard or the terminal show the agent working.
    3. kill that process.
    4. kitaru executions list           # the run is now orphaned
    5. kitaru executions resume <id>    # picks up exactly where it stopped
"""

import kitaru

from agent_factory.agent import build_agent
from agent_factory.profile import Profile

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
    agent = build_agent(DEFAULT_PROFILE)
    return agent.run_sync(prompt).output


if __name__ == "__main__":
    handle = agent_factory_flow.run(
        "Look at /etc/hosts and tell me what hostnames are configured."
    )
    print(handle.wait().output)
