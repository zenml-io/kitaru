# agent_factory

A starter kit for an internal agent factory: the runnable foundation a platform engineer can fork to give their team's developers a way to spin up durable, sandboxed, profile-gated agents fast — with credential isolation, HITL gates, memory, and replay all wired up correctly.

The architecture is pioneered by [`kami`](../../../../kami-agent/) (an internal ZenML project), simplified to be locally runnable with one `docker compose up` and zero external accounts.

## Who this is for

Platform engineers building their org's internal agent platform on top of [Kitaru](https://kitaru.ai/) and [PydanticAI](https://ai.pydantic.dev). The example is intentionally generic — every stage's demo prompt is a throwaway exercising the new tool that stage introduces. The *framework* is the through-line, not a vertical domain. Forking this for your team means swapping the prompt + fixtures + skill markdown; the framework stays the same.

## Quick start

```bash
cd examples/end_to_end/agent_factory
uv sync                                 # already wired up in this repo
export OPENAI_API_KEY=sk-...            # stage 1 needs an OpenAI key
python stage_1_basic_agent.py
```

You should see the agent investigate `/etc/hosts` and return the hostnames configured there. That's the foundation — durable PydanticAI in ~30 lines.

## The 8-stage tour

The example builds up one capability at a time. Each stage adds one tool or one architectural primitive; the library at `agent_factory/` grows monotonically; the **Profile** is the per-stage gate (`allowed_tools={...}`) that controls which capabilities each stage's agent actually exercises. Older stage files stay valid as the library expands.

---

### Stage 1 — Building blocks for your internal agent factory

**Stage file:** `stage_1_basic_agent.py`
**The pitch:** PydanticAI gives you the agent loop. Kitaru gives you durable execution. Together: durable agents without learning a graph DSL or rewriting your control flow as a state machine.

**What's in it:**

- `Profile` (Pydantic model: `name`, `system_prompt`, `model`, `allowed_tools`)
- `PermissionHandler` — gates every tool call against the profile
- `build_tools(permission_handler)` — pydantic-ai toolset filtered by `allowed_tools`
- `build_agent(profile)` — returns a vanilla pydantic-ai `Agent`
- The flow body: wraps the Agent with `KitaruAgent(...)` for durable execution

The kitaru wrap stays at *flow scope*, not in the library helper, so you see the integration as a deliberate seam rather than something the library hides.

**The hero demo — durability via kill-and-resume:**

```bash
python stage_1_basic_agent.py &      # run in background
kill %1                               # kill it mid-turn
kitaru executions list                # the run is now orphaned
kitaru executions resume <id>         # picks up exactly where it stopped
```

Kitaru persisted every checkpoint output as the run progressed; the kill left an orphaned execution; resuming re-runs from the last incomplete checkpoint. PydanticAI's agent loop didn't need to know about any of it.

**Mode:** turn (default). Each `agent.run_sync()` is one aggregating checkpoint. Kill mid-turn → resume re-runs the whole turn. Granular per-call caching (one checkpoint per LLM/tool call) is introduced in a later stage where it earns its keep.

**Not yet here:** sandbox (stage 2), credential isolation (stage 3), playbook (stage 4), typed services (stage 5), HITL (stages 6–7), replay (stage 8).

---

### Stage 2 — Your agents need a sandbox

**Stage file:** `stage_2_sandboxed_exec.py` *(not yet built)*

The `exec` tool runs in your host process right now. Production agents need a sandbox so they can't `rm -rf /`. Stage 2 ports kami's `ModalSandboxRuntime` to Docker — persistent `bash --noprofile --norc` in a worker container, marker-based completion protocol, per-execution workspace volume.

*Section will be filled in when stage 2 lands.*

---

### Stage 3 — Your agents need credentials they can't see

**Stage file:** `stage_3_credential_proxy.py` *(not yet built)*

The agent has no credentials yet. When it does, you'll want them isolated from the worker so a prompt-injected agent can't exfiltrate them. Stage 3 ports kami's mitmproxy addon — the worker has no `Authorization` headers; a separate `proxy` container holds them in `AGENT_FACTORY_CREDENTIALS` env and injects them on matching hosts. Backed by `kitaru.secrets`.

*Section will be filled in when stage 3 lands.*

---

### Stage 4 — Your agents need a procedure

**Stage file:** `stage_4_skills.py` *(not yet built)*

The agent's behavior is hardcoded in the system prompt right now. Real agents have *playbooks* — operator-edited markdown files — that describe their procedure. Stage 4 introduces the `skill` tool with `list`/`read`/`search` semantics and an escape-prevention path validation, ported from kami's `tools.py:135`.

*Section will be filled in when stage 4 lands.*

---

### Stage 5 — Your agents need typed services

**Stage file:** `stage_5_typed_services.py` *(not yet built)*

Some agent calls deserve a typed schema and a typed response — posting a webhook, looking up structured records. Stage 5 introduces `exec_service`, a single tool that dispatches to a `ServiceCall` discriminated union. Two cases: `lookup_wiki` and `publish_summary`. Tool description is built dynamically from the profile's allowed services. This is also where the **two credential paths** distinction lands: sandboxed `exec` (proxy-injected) vs. host-side `exec_service` (resolves creds directly).

*Section will be filled in when stage 5 lands.*

---

### Stage 6 — Your agents need to ask humans things

**Stage file:** `stage_6_hitl.py` *(not yet built)*

Some questions only the operator can answer. Stage 6 introduces `ask_question`, a typed-union HITL dispatcher with a `freeform` kind. Marked with `@hitl_tool`; suspends via the kitaru pydantic-ai adapter's exported `wait_for_input(...)` helper. Flow pauses, dashboard shows `waiting`, operator answers, flow resumes.

*Section will be filled in when stage 6 lands.*

---

### Stage 7 — Your agents need to remember

**Stage file:** `stage_7_memory.py` *(not yet built)*

Some preferences shouldn't be re-asked every run. Stage 7 adds the `remembered_choice` kind to `ask_question`, paired with flow-scope `kitaru.memory`. First run asks; second run returns the cached answer. Read-ask-record loop co-located in the tool body.

*Section will be filled in when stage 7 lands.*

---

### Stage 8 — Your agents need to be re-runnable

**Stage file:** `stage_8_replay.py` *(not yet built)*

Knowledge bases change. Without re-running every checkpoint and re-asking every HITL question, you want to swap one input and let the agent re-reason against the new context. Stage 8 demos `flow.replay()` with `overrides={"checkpoint.fetch_wiki": <new content>}`: cached HITL answers stay intact, only the downstream re-executes.

*Section will be filled in when stage 8 lands.*

---

## Architecture overview

The full architectural rationale lives in [`DESIGN.md`](./DESIGN.md). Highlights:

- **Single shared library; no per-stage code copies.** The `agent_factory/` library grows monotonically; per-stage progressive disclosure happens through the Profile, not through versioning the library.
- **Two credential paths.** `exec` (sandboxed shell, proxy-injected creds) vs. `exec_service` (host-side typed call, direct `kitaru.secrets` resolution). Both demoed.
- **Profile is the central factory artifact.** Adding a new agent = creating a new Profile.
- **Tests ship with the example.** Smoke (stage 1), proxy injection, full-loop happy path. Doubles as kitaru CI.

## Forking for your team

When this example is mature, the fork-this-for-your-team guide lands here. For now: clone, swap `DEFAULT_PROFILE`'s system prompt and `allowed_tools` for whatever your agents need, point `LocalSkillSource` at your team's playbook directory, and update the mock services to talk to your real wiki/webhook endpoints.

## Tips for production use

- **Model aliases.** This example hardcodes a provider string (`"openai:gpt-5.4-nano"`) in the Profile so chapter 1 has the smallest possible setup. In production, prefer `kitaru model register <alias> --provider openai --model <id> --api-key ...` and reference the alias in your Profile so credentials are managed centrally.
- **Compaction.** `kitaru.memory` keeps every version. After your agents have been running for a while, schedule `kitaru memory compact ...` to summarize old preferences and reduce storage cost.
