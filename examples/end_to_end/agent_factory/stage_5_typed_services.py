"""Stage 5 — typed services via host-side `exec_service`.

The chapter 5 hero demo: not every agent call deserves a shell. When
the agent needs structured input AND a structured response — looking
up records, publishing webhooks — give it a typed service instead.

See `agent_factory/services/__init__.py` for the architecture overview
and the README's stage 5 section for the credential-paths framing.

Setup: `bash setup.sh` (idempotent — re-run after pulling new stages
to pick up the `webhook-token` secret).

Run:

    DISABLE_CACHE=1 python stage_5_typed_services.py

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

_SKILLS_DIR = Path(__file__).parent / "skills" / "with-services"

DEFAULT_PROFILE = Profile(
    name="default",
    system_prompt=(
        "You are a helpful assistant. Your procedure lives in a skill file. "
        "Always start by calling skill(action='list') and skill(action='read'), "
        "then follow the procedure exactly. "
        "One tool call per step."
    ),
    model="openai:gpt-5-nano",
    # Stage 5's profile gates ALL THREE non-skill tools — exec (sandboxed),
    # exec_service (host-side typed), and skill (host-side markdown reader).
    allowed_tools={"exec", "skill", "exec_service"},
    skill_source=LocalSkillSource(path=str(_SKILLS_DIR)),
    # Both services the agent can dispatch to. The exec_service tool's
    # description is built from this set — the LLM sees only what it can call.
    allowed_services={"lookup_wiki", "publish_summary"},
    # Same proxy rule as stage 4 — `exec("curl http://wiki.local/...")` would
    # still get the bearer injected via the proxy. Stage 5 doesn't exercise
    # that path, but the rule stays so a forking dev can mix both styles in
    # the same skill if they want to.
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
        DockerProxy(credential_map=credential_map, execution_id=execution_id) as proxy,
        DockerSandbox(execution_id=execution_id, proxy=proxy) as sandbox,
    ):
        agent = build_agent(DEFAULT_PROFILE, sandbox=sandbox)
        agent = KitaruAgent(agent)
        result = agent.run_sync("Carry out your procedure and return the result.")

    print(f"\n{result.output}\n")
    return result.output


if __name__ == "__main__":
    agent_factory_flow.run(cache=False if DISABLE_CACHE else None)
