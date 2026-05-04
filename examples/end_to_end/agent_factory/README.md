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

You should see the agent investigate the host (OS, kernel, current user, process count) and return a one-paragraph summary. That's the foundation — durable PydanticAI in ~30 lines.

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

The flow has **two checkpoints**, both real LLM turns (`default` and `default_2`). A `FORCE_FAILURE` env var raises a simulated downstream blip between them. The two-step tour:

```bash
# Step 1: simulate failure between turn 1 and turn 2.
# The first turn does real LLM + tool work and is checkpointed,
# then the flow body raises before the second turn starts.
FORCE_FAILURE=1 python stage_1_basic_agent.py

# Step 2: re-run without the flag.
# `default` is served from cache (zero LLM calls), `default_2`
# runs fresh against new LLM work, then the flow prints + returns.
python stage_1_basic_agent.py
```

What you see in the kitaru log lines:

| | Step 1 | Step 2 |
|---|---|---|
| `default` | `started` → `finished in 15s` (3 LLM calls) | **`cached`** (instant, $0) |
| `default_2` | (never runs — flow raised first) | `started` → `finished in 8s` (1 LLM call) |
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

The flow has two `agent.run_sync()` turns sharing one `with DockerSandbox(...)`. Turn 1 investigates the machine and `cd`s into `/tmp`. Turn 2 writes `summary.txt` "in the current directory" — and because the persistent shell carried turn 1's `cd` across the turn boundary, the file lands in `/tmp/summary.txt` without turn 2 ever stating an absolute path.

```
[sandbox] Started container e87af8e85ec2 (image=agent-factory-sandbox, /workspace ← workspace_…)
Kitaru: Checkpoint `default` started.
[sandbox] $ cat /etc/os-release
[sandbox]   → exit=0, stdout=285 chars, cwd=/workspace
[sandbox] $ uname -r
[sandbox]   → exit=0, stdout=16 chars, cwd=/workspace
[sandbox] $ whoami
[sandbox]   → exit=0, stdout=4 chars, cwd=/workspace
[sandbox] $ cd /tmp
[sandbox]   → exit=0, stdout=0 chars, cwd=/tmp                       ← cwd changed
Kitaru: Checkpoint `default` finished in 21.0s.
Kitaru: Checkpoint `default_2` started.
[sandbox] $ ls -la && pwd
[sandbox]   → exit=0, stdout=99 chars, cwd=/tmp                       ← still /tmp from turn 1
[sandbox] $ cat > summary.txt <<'EOF' …                              ← writes /tmp/summary.txt
[sandbox]   → exit=0, stdout=237 chars, cwd=/tmp
Kitaru: Checkpoint `default_2` finished in 13.1s.
[sandbox] Stopping container e87af8e85ec2
```

`summary.txt` ends up at `/tmp/summary.txt` because the bash session is the *same* bash session across turn 1 and turn 2 — the chapter's whole point. The agent also reports Debian / root (the container's image and user), not your macOS/Linux host.

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

**Persistent shell — within a run:** stage 2 runs every `run(command)` through **one long-lived `bash --noprofile --norc` process** inside the container. Shell state — `cd`, `export`, file descriptors, background jobs — survives across `exec` calls, just like a normal interactive shell. The host writes commands into the shell's stdin and reads back output up to a unique completion-marker line (`<UUID> <exit_code> <cwd>`).

**Across runs (deliberately *not* preserved):** the bash process dies when the container stops, and we don't try to replay shell state across runs. Bash commands have side effects (`rm`, `git push`, `curl POST`, `psql -c "INSERT…"`) that a `cd + declare -px` snapshot can't capture or undo, so "restoring" a snapshot would silently drop every actual mutation. If the agent needs cross-run durable state, it should write to `/workspace` (a Docker named volume that survives container teardown) or use `kitaru.memory` deliberately at flow scope for specific values it explicitly wants to carry forward. Chapter 7 introduces the latter pattern.

**Not yet here:** credentials still come from the host process; chapter 3 isolates them via a separate proxy container that injects `Authorization` headers based on host patterns.

---

### Stage 3 — Your agents need a procedure

**Stage file:** `stage_3_skills.py`
**The pitch:** the agent's behavior was hardcoded in the system prompt in stages 1–2. Real agents have *playbooks* — operator-edited markdown files — that describe their procedure. Stage 3 introduces the `skill` tool: the agent reads its instructions from `skills/<agent>/SKILL.md` instead of from Python.

**Run it:**

```bash
DISABLE_CACHE=1 python stage_3_skills.py
```

Watch the agent fetch its skill, then follow it:

```
[sandbox] Started container 14f809c4f370 (image=agent-factory-sandbox, /workspace ← workspace_…)
Kitaru: Checkpoint `default` started.
Kitaru: HTTP Request: POST https://api.openai.com/v1/chat/completions  ← agent calls skill(action="list")
Kitaru: HTTP Request: POST https://api.openai.com/v1/chat/completions  ← agent calls skill(action="read", path="default-agent/SKILL.md")
Kitaru: HTTP Request: POST https://api.openai.com/v1/chat/completions  ← agent's first decision after reading the skill
[sandbox] $ cat /etc/os-release
[sandbox]   → exit=0, stdout=285 chars, cwd=/workspace
[sandbox] $ uname -r
[sandbox]   → exit=0, stdout=16 chars, cwd=/workspace
[sandbox] $ whoami
[sandbox]   → exit=0, stdout=4 chars, cwd=/workspace
[sandbox] $ cd /tmp
[sandbox]   → exit=0, stdout=0 chars, cwd=/tmp
[sandbox] $ cat > summary.txt <<'EOF' …
[sandbox]   → exit=0, stdout=211 chars, cwd=/tmp
```

The system prompt only says "find your skill and follow it." The actual procedure (which commands to run, what to summarize, how to return) lives in `skills/default-agent/SKILL.md`. Edit the markdown, re-run, watch the agent's behavior change without touching Python.

**What's in it:**

- `agent_factory/tools.py` — `skill` tool factory with `list`/`read`/`search` actions, `.is_relative_to(skills_root)` escape prevention, `MAX_READ_BYTES=100_000`, default glob `**/SKILL.md`.
- `agent_factory/profile.py` — `LocalSkillSource(path=...)` Pydantic model + `SkillSource` alias. Forks add their own variants (see below).
- `skills/basic/default-agent/SKILL.md` — the agent's playbook for stage 3 (basic procedure with no external services). Operator edits this in their IDE.

**Where the skill tool runs:** *host-side*, not inside the sandbox. The agent calls the tool from inside its turn checkpoint; the tool reads files from the host's filesystem directly. This means operators can edit `skills/...` without touching containers, and the path validation is a single boundary on the host.

**Where skills live in production:** stage 3 ships **one** `SkillSource` variant — `LocalSkillSource(path=...)` — for local development. Real deployments will want one of:

- **`GitRepoSkillSource(repo_url=..., ref=...)`** — clone a versioned skill repo at flow start. The prod path: skills are code-reviewed via PRs, shared across teammates and running agents.
- **`InlineMarkdownSkillSource(name=..., markdown=...)`** — bake the markdown directly into the Profile. Useful for one-off agents, tests, or skills generated by another flow.
- **Object storage / kitaru artifacts / container-image bake** — for stricter deployment shapes.

The `SkillSource` seam is visible in `agent_factory/profile.py` so forking devs see the alternatives where the architecture lives, not just in docs. Adding a new source = subclass with a `resolve(self) -> Path` method; the skill tool itself doesn't change.

**Env-var toggles:** same as stage 2 (`DISABLE_CACHE=1`).

---

### Stage 4 — Your agents need credentials they can't see

**Stage file:** `stage_4_credential_proxy.py`
**The pitch:** in stages 1–3 the agent's `exec` either runs in your host process or in a container with no credentials. Once you give the agent real services to call, the credentials need to live *somewhere* — and "in the worker container alongside the agent's shell" is the wrong place. A prompt-injected agent could `cat $WIKI_TOKEN` and exfiltrate it. Stage 4 sets up a two-process pattern: a separate `proxy` container holds the credentials and injects `Authorization` headers on matching hosts; the worker stays credential-free.

**One-time setup (builds proxy + mock images, sets the wiki-token kitaru secret):**

```bash
bash setup.sh
```

**Run it:**

```bash
DISABLE_CACHE=1 python stage_4_credential_proxy.py
```

Three containers come up — mock-services, proxy, sandbox — all on the `agent_factory` Docker network. The agent follows the (extended) skill, hits `wiki.local` through the proxy, and the proxy injects the bearer:

```
[mock-services] Started container … (image=agent-factory-mock, network aliases=['wiki.local'])
[proxy]         Started container … (image=agent-factory-proxy, injecting for hosts=['wiki.local'])
[sandbox]       Started container … (proxy-wired)
Kitaru: Checkpoint `default` started.
[sandbox] $ cat /etc/os-release
[sandbox]   → exit=0, stdout=286 chars, cwd=/workspace
…  (uname -r, whoami)
[sandbox] $ cd /tmp
[sandbox]   → exit=0, stdout=0 chars, cwd=/tmp
[sandbox] $ curl -s http://wiki.local/snippets/durability
[sandbox]   → exit=0, stdout=514 chars, cwd=/tmp                ← JSON came back
…  (writes /tmp/summary.txt with a real fact pulled from the wiki snippet)
Kitaru: Checkpoint `default` finished in 41.8s.
[sandbox] Stopping container …
[proxy]   Stopping container …
[mock-services] Stopping container …
```

The proxy + mock containers print correlated logs (use `docker logs agent_factory_proxy` / `agent_factory_mock` while the flow is running):

```
[agent-factory-proxy] credentials loaded for hosts: ['wiki.local']
[agent-factory-proxy] proxy token configured
[agent-factory-proxy] injected headers for wiki.local: ['Authorization']
[mock-services] GET /snippets/durability (host=wiki.local, auth=Bearer w…) → 200
```

The bearer arrived at the mock — but **the worker never had it**. The credential was resolved on the host *once* at flow start (`kitaru.get_secret("wiki-token")`), handed to the proxy container's `AGENT_FACTORY_CREDENTIALS` env, and the proxy injected it on every outbound request to `wiki.local`. A prompt-injected agent inside the worker can't `echo $WIKI_TOKEN` because there *is* no `WIKI_TOKEN` env there.

**What's in it:**

- `agent_factory/sandbox/proxy.py` — `DockerProxy` context manager. Per-run bearer token (`secrets.token_urlsafe(32)`) wired into the worker's `http_proxy` URL via basic-auth-as-bearer; the addon's auth gate rejects requests without the right token so other host processes can't accidentally use this proxy.
- `agent_factory/sandbox/proxy_addon.py` — mitmproxy addon. Per-connection auth gate + per-request host-match header injection.
- `agent_factory/sandbox/certs.py` — self-signed CA generation. Worker trusts the public cert via `update-ca-certificates`; the combined key+cert lives only in the proxy container.
- `agent_factory/sandbox/runtime.py` — `DockerSandbox(proxy=…)` parameter wires `http_proxy`/`https_proxy` env vars + `REQUESTS_CA_BUNDLE` etc. + bind-mounts the public cert + runs `update-ca-certificates` on container start.
- `agent_factory/profile.py` — `SandboxProxyRule(name, hosts, headers)`. Header values can contain `{{ secret-name.key }}` templates resolved via `kitaru.get_secret(...)`.
- `agent_factory/secrets.py` — `build_credential_map(profile)` resolves all templates once on the host before any container sees them.
- `mocks/server.py` + `mocks/runner.py` — FastAPI mock with `/snippets/{topic}` (auth-gated, returns sample wiki snippets) and a `DockerMockServices` context manager.
- `skills/with-wiki/default-agent/SKILL.md` — stage 4's skill, extending stage 3's procedure with step 5 (curl wiki.local through the proxy). The agent now does real HTTP work as part of its procedure. Stage 3's `skills/basic/...` stays untouched, so re-running stage 3 still works without the proxy + mock containers.

**Env-var toggles:** same as earlier stages (`DISABLE_CACHE=1`).

**Architectural notes:**

- The credential template `{{ wiki-token.value }}` resolves at flow start, on the host, *before* the proxy container is spawned. The resolved value is passed to the proxy via Docker env. The worker never sees the resolved value, the template, or the secret name.
- The proxy listens for HTTP/HTTPS via `mitmdump --listen-host 0.0.0.0 --listen-port 8080` and intercepts both. HTTPS works because the worker trusts the proxy's self-signed CA.
- `--network-alias wiki.local` on the mock-services container makes Docker's embedded DNS resolve `wiki.local` to that container — so the agent's `curl http://wiki.local/...` lands at the mock without `/etc/hosts` hacks.
- The proxy + mock images need to be built once via `bash setup.sh`; `DockerProxy.__enter__` and `DockerMockServices.__enter__` pre-flight the image and raise a clear `KitaruRuntimeError` with the build command if the image is missing.

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
