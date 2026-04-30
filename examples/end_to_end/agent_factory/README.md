# Build a production-ready agent factory

A starter kit for an internal agent factory: the runnable foundation a platform engineer can fork to give their team's developers a way to spin up durable, sandboxed, profile-gated agents fast — with credential isolation, HITL gates, memory, and replay all wired up correctly. Locally runnable with one `docker compose up` and zero external accounts.

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

**The hero demo — durability via cached checkpoints surviving a failure:**

The flow has **three checkpoints**: two real LLM turns (`default` and `default_2`) plus a `join_turns` step that combines them. A `FORCE_FAILURE` env var flips a simulated downstream blip between the two turns. The two-step tour:

```bash
# Step 1: simulate failure between turn 1 and turn 2.
# The first turn does real LLM + tool work and is checkpointed,
# then the flow body raises before the second turn starts.
FORCE_FAILURE=1 python stage_1_basic_agent.py

# Step 2: re-run without the flag.
# `default` is served from cache (zero LLM calls), `default_2`
# runs fresh against new LLM work, `join_turns` prints the result.
python stage_1_basic_agent.py
```

What you see in the kitaru log lines:

| | Step 1 | Step 2 |
|---|---|---|
| `default` | `started` → `finished in 15s` (3 LLM calls) | **`cached`** (instant, $0) |
| `default_2` | (never runs — flow raised first) | `started` → `finished in 8s` (1 LLM call) |
| `join_turns` | (never runs) | `started` → `finished` |
| Total time | ~25s ending in failure | ~12s ending in printed output |

Without kitaru, step 1's failure would have wasted the first turn's work and you'd pay for *both* turns on the retry. With kitaru, only the part that didn't complete the first time gets re-paid for. *That's the durability story.*

**Mode:** turn (default). Each `agent.run_sync()` is one aggregating checkpoint. Granular per-call caching (one checkpoint per LLM/tool call) is introduced in a later stage where it earns its keep.

**Env-var toggles:**

- `FORCE_FAILURE=1` — simulate the post-turn-1 blip described above.
- `DISABLE_CACHE=1` — force every checkpoint to re-execute even if a prior cached output exists (useful when iterating on the flow itself).

**Not yet here:** sandbox (stage 2), credential isolation (stage 3), playbook (stage 4), typed services (stage 5), HITL (stages 6–7), replay (stage 8).

---

### Stage 2 — Your agents need a sandbox

**Stage file:** `stage_2_sandboxed_exec.py`
**The pitch:** the `exec` tool runs in your host process in stage 1. Production agents need a sandbox so they can't `rm -rf /`. Stage 2 wraps each agent run in a `DockerSandbox` context manager — every shell command now runs inside an isolated container with its own filesystem and network namespace.

**One-time setup (build the sandbox image):**

```bash
docker build -t agent-factory-sandbox -f docker/sandbox.Dockerfile docker/
```

**Run it:**

```bash
DISABLE_CACHE=1 python stage_2_sandboxed_exec.py
```

The flow has two `agent.run_sync()` turns sharing one `with DockerSandbox(...)`. Turn 1 changes shell state; turn 2 reads it back. Watch the persistent shell carry state across turns:

```
[sandbox] Started container 11828848d040 (image=agent-factory-sandbox, /workspace ← workspace_df8dd2a1)
Kitaru: Checkpoint `default` started.
[sandbox] $ cat /etc/os-release
[sandbox]   → exit=0, stdout=285 chars, cwd=/workspace
[sandbox] $ uname -r
[sandbox]   → exit=0, stdout=16 chars, cwd=/workspace
[sandbox] $ whoami
[sandbox]   → exit=0, stdout=4 chars, cwd=/workspace
[sandbox] $ cd /tmp && export GREETING='hello from turn 1' && …
[sandbox]   → exit=0, stdout=31 chars, cwd=/tmp                       ← cwd just changed
Kitaru: Checkpoint `default` finished in 27.9s.
Kitaru: Checkpoint `default_2` started.
[sandbox] $ pwd && echo "$GREETING"
[sandbox]   → exit=0, stdout=22 chars, cwd=/tmp                       ← still /tmp from turn 1
Kitaru: Checkpoint `default_2` finished in 9.7s.
[sandbox] Stopping container 11828848d040 (workspace volume preserved for pause/resume durability)
```

The `cwd=/tmp` line on turn 2 proves the shell is the same shell — without the persistent-bash port, every turn would start in `/workspace` with empty env. The agent also reports Debian / root (the container's image and user), not your macOS/Linux host.

**Watch it boot from another terminal:**

```bash
docker ps                                              # see agent_factory_sandbox_<id>
docker exec -it agent_factory_sandbox_<id> bash        # peek inside the live container
```

**What's in it:**

- `agent_factory/sandbox/runtime.py` — `DockerSandbox` context manager: `docker run -d` on entry, `docker stop` on exit. Every lifecycle event prints a `[sandbox]` log line and attaches structured metadata via `kitaru.log()` for the dashboard.
- `docker/sandbox.Dockerfile` — minimal `python:3.11-slim` + bash + curl + jq
- `agent_factory/tools.py` — `build_tools(permission_handler, sandbox=...)` accepts an optional sandbox; the `exec` tool routes through `sandbox.run(command)` when one's provided, otherwise runs in-process
- A named volume `workspace_<execution_id>` mounts at `/workspace` in the sandbox — durable filesystem state survives flow pause/resume

**Env-var toggles:**

- `DISABLE_CACHE=1` — force every checkpoint to re-execute (useful when the agent's already cached and you want to see the sandbox actually running shell commands)
- `FORCE_FAILURE=1` — raise between turn 1 and turn 2. Same durability story as stage 1: turn 1's checkpoint is cached across the failure, so a re-run without the flag serves it instantly. (Note: the agent's *reasoning* is cached; the bash *side effects* aren't replayed — turn 2 on re-run runs against a fresh shell.)

**Persistent shell — within a run:** stage 2 runs every `run(command)` through **one long-lived `bash --noprofile --norc` process** inside the container. Shell state — `cd`, `export`, file descriptors, background jobs — survives across `exec` calls, just like a normal interactive shell. The host writes commands into the shell's stdin and reads back output up to a unique completion-marker line (`<UUID> <exit_code> <cwd>`). Ported verbatim from kami's `modal_runtime.py`; the only Docker-specific bit is `subprocess.Popen(["docker", "exec", "-i", ...])` instead of `modal.Sandbox.exec`.

**Across runs (deliberately *not* preserved):** the bash process dies when the container stops, and we don't try to replay shell state across runs. Bash commands have side effects (`rm`, `git push`, `curl POST`, `psql -c "INSERT…"`) that a `cd + declare -px` snapshot can't capture or undo, so "restoring" a snapshot would silently drop every actual mutation. If the agent needs cross-run durable state, it should write to `/workspace` (a Docker named volume that survives container teardown) or use `kitaru.memory` deliberately at flow scope for specific values it explicitly wants to carry forward. Chapter 7 introduces the latter pattern.

**Not yet here:** credentials still come from the host process; chapter 3 isolates them via a separate proxy container that injects `Authorization` headers based on host patterns.

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
