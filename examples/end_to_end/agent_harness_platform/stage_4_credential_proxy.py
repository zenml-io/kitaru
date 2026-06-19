"""Stage 4 — credentials the agent can never see.

The stage 4 hero demo: a separate `proxy` container holds the
agent's credentials in `AGENT_HARNESS_PLATFORM_CREDENTIALS` env, the worker
container has nothing, and HTTP/HTTPS from the worker is routed
through the proxy via `http_proxy` env vars + a self-signed CA cert
the worker trusts. The proxy's mitmproxy addon matches each request's
host against the credential map and injects the right `Authorization`
header — so the agent's `curl http://wiki.local/snippets/durability`
returns 200 even though the agent never held a bearer token.

Setup once (the README's "Stage 4 onward" block):

    bash setup.sh    # builds proxy + mock images, sets the wiki-token secret

Then run:

    DISABLE_CACHE=1 python stage_4_credential_proxy.py

What you'll see, in three log streams:

    [proxy]          Started container … (injecting for hosts=['wiki.local'])
    [mock-services]  Started container … (network aliases=['wiki.local'])
    [sandbox]        Started container … (proxy-wired)
    [sandbox] $ curl -s http://wiki.local/snippets/durability
    [sandbox]   → exit=0, stdout=… cwd=/tmp
    [agent-harness-platform-proxy] injected headers for wiki.local: ['Authorization']
    [mock-services] GET /snippets/durability (host=wiki.local, auth=Bearer w…) → 200

The bearer arrived at the mock — but the worker never had it. The
proxy injected it on the way out; the credential was resolved on the
host once at flow start via `kitaru.get_secret("wiki-token")` and
handed to the proxy container's env, never the worker's.

Env-var toggles:

    DISABLE_CACHE=1    force every checkpoint to re-execute
"""

import os
from pathlib import Path

from agent_harness_platform.agent import build_agent
from agent_harness_platform.profile import LocalSkillSource, Profile, SandboxProxyRule
from agent_harness_platform.sandbox import DockerProxy, DockerSandbox
from agent_harness_platform.secrets import build_credential_map
from mocks import DockerMockServices

import kitaru
from kitaru.adapters.pydantic_ai import KitaruAgent
from kitaru.runtime import _get_current_execution_id

DISABLE_CACHE = bool(os.environ.get("DISABLE_CACHE"))

_SKILLS_DIR = Path(__file__).parent / "skills" / "with-wiki"

DEFAULT_PROFILE = Profile(
    name="default",
    system_prompt=(
        "You are a helpful assistant. Your procedure lives in a skill file. "
        "Always start by calling skill(action='list') and skill(action='read'), "
        "then follow the procedure exactly using the `exec` tool. "
        "One tool call per step."
    ),
    model="openai:gpt-5-nano",
    allowed_tools={"exec", "skill"},
    skill_source=LocalSkillSource(path=str(_SKILLS_DIR)),
    # The credential proxy injects `Authorization: Bearer <wiki-token-value>`
    # on every request the agent makes to wiki.local. The `{{ ... }}`
    # template resolves at flow start via `kitaru.get_secret("wiki-token")`.
    sandbox_proxy_rules=[
        SandboxProxyRule(
            name="wiki-auth",
            hosts=["wiki.local"],
            headers={"Authorization": "Bearer {{ wiki-token.value }}"},
        ),
    ],
)


@kitaru.flow
def agent_harness_platform_flow() -> str:
    execution_id = _get_current_execution_id() or "local"
    # Resolve `{{ secret-name.key }}` templates ONCE on the host before
    # we ever start the proxy container. The worker never sees these.
    credential_map = build_credential_map(DEFAULT_PROFILE)

    with (
        DockerMockServices(execution_id=execution_id) as _mock,
        DockerProxy(credential_map=credential_map, execution_id=execution_id) as proxy,
        DockerSandbox(execution_id=execution_id, proxy=proxy) as sandbox,
    ):
        agent = build_agent(DEFAULT_PROFILE, sandbox=sandbox)
        # Use turn strategy for this tour; stage 1 explains why.
        agent = KitaruAgent(agent, checkpoint_strategy="turn")
        result = agent.run_sync("Carry out your procedure and return the result.")

    print(f"\n{result.output}\n")
    return result.output


if __name__ == "__main__":
    agent_harness_platform_flow.run(cache=False if DISABLE_CACHE else None)
