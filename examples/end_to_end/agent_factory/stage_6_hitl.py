"""Stage 6 — agents that pause to ask humans things.

The chapter 6 hero demo: some questions only the operator can answer.
Stage 6 introduces `ask_question`, a freeform HITL tool. The agent
calls it like any other tool; the tool body calls `wait_for_input(...)`
from the kitaru pydantic-ai adapter, which routes through `kitaru.wait()`:

- The agent's turn suspends mid-execution.
- The flow's status becomes `waiting`.
- The operator answers via dashboard / `kitaru executions input` /
  REST API. Whatever they reply becomes the tool's return value.
- The agent's turn resumes from exactly the same point and continues.

There's no separate "deferred" plumbing: from the agent's perspective,
`ask_question("...")` returns a string after a brief pause. From kitaru's
perspective, the flow paused, durably, until input arrived.

How to drive the demo (two ways — pick one):

**Interactive (recommended for first run):**

    DISABLE_CACHE=1 python stage_6_hitl.py

When the agent calls `ask_question`, kitaru's local runtime prompts on
the same terminal. Type your answer, hit enter, the flow resumes.

**Non-interactive (the production-shaped path):**

    DISABLE_CACHE=1 python stage_6_hitl.py </dev/null &
    # in another terminal, after `Waiting on ask_question:...` shows up:
    kitaru executions list                                # find the waiting execution
    kitaru executions input <execution_id> --value '"Verified by ops on call"'

That's exactly how it would work in production: the flow runs on a
server, the operator answers via the dashboard / CLI / REST API.

Env-var toggles:

    DISABLE_CACHE=1    force every checkpoint to re-execute
"""

import os
from pathlib import Path

from agent_factory.agent import build_agent
from agent_factory.profile import LocalSkillSource, Profile, SandboxProxyRule
from agent_factory.sandbox import DockerProxy, DockerSandbox
from agent_factory.secrets import build_credential_map
from mocks import DockerMockServices

import kitaru
from kitaru.adapters.pydantic_ai import KitaruAgent
from kitaru.runtime import _get_current_execution_id

DISABLE_CACHE = bool(os.environ.get("DISABLE_CACHE"))

_SKILLS_DIR = Path(__file__).parent / "skills" / "with-hitl"

DEFAULT_PROFILE = Profile(
    name="default",
    system_prompt=(
        "You are a helpful assistant. Your procedure lives in a skill file. "
        "Always start by calling skill(action='list') and skill(action='read'), "
        "then follow the procedure exactly. "
        "One tool call per step."
    ),
    model="openai:gpt-5.4-nano",
    # Stage 6 turns on `ask_question` alongside the stage 5 toolkit. The
    # adapter handles the wait() bridge automatically when the agent calls it.
    allowed_tools={"exec", "skill", "exec_service", "ask_question"},
    skill_source=LocalSkillSource(path=str(_SKILLS_DIR)),
    allowed_services={"lookup_wiki", "publish_summary"},
    sandbox_proxy_rules=[
        SandboxProxyRule(
            name="wiki-auth",
            hosts=["wiki.local"],
            headers={"Authorization": "Bearer {{ wiki-token.value }}"},
        ),
    ],
)


@kitaru.flow
def agent_factory_flow() -> str:
    execution_id = _get_current_execution_id() or "local"
    credential_map = build_credential_map(DEFAULT_PROFILE)

    with (
        DockerMockServices(execution_id=execution_id) as _mock,
        DockerProxy(
            credential_map=credential_map, execution_id=execution_id
        ) as proxy,
        DockerSandbox(execution_id=execution_id, proxy=proxy) as sandbox,
    ):
        agent = build_agent(DEFAULT_PROFILE, sandbox=sandbox)
        agent = KitaruAgent(agent)
        result = agent.run_sync(
            "Carry out your procedure and return the result."
        )

    print(f"\n{result.output}\n")
    return result.output


if __name__ == "__main__":
    agent_factory_flow.run(cache=False if DISABLE_CACHE else None)
