# Build an internal agent factory

> **Reading on the docs site is recommended:** the same tour as a chapter-by-chapter walk lives at [kitaru.ai/docs/agent-factory](https://kitaru.ai/docs/agent-factory/). This README is the on-GitHub mirror — same content, slightly different formatting, useful when you've cloned the repo and want a single file to scroll while you run the stages.

A starter kit for an internal agent factory: the runnable foundation a platform engineer can fork to give their team's developers a way to spin up durable, sandboxed, profile-gated agents fast — with credential isolation and HITL gates wired up correctly. Locally runnable with `bash setup.sh && python stage_N_*.py` and zero external accounts.

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

## The 6-stage tour

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

**Mode.** Each stage in this tour explicitly passes `granular_checkpoints=False` for log-clarity in the chapter walk-throughs — one aggregating `default` / `default_2` checkpoint per `agent.run_sync()` keeps the trace readable when you're learning the primitives. Production forks should drop the kwarg: `KitaruAgent` defaults to `granular_checkpoints=True`, so every model request and every tool call gets its own checkpoint and its own cache key, and a flow that crashed after the third model request resumes by replaying only the calls after the third. Same FORCE_FAILURE durability story; finer cache granularity and richer dashboard rows.

**Env-var toggles:**

- `FORCE_FAILURE=1` — simulate the post-turn-1 blip described above.
- `DISABLE_CACHE=1` — force every checkpoint to re-execute even if a prior cached output exists (useful when iterating on the flow itself).

**Not yet here:** sandbox (stage 2), playbook (stage 3), credential isolation (stage 4), typed services (stage 5), HITL (stage 6).

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

**Across runs (deliberately *not* preserved):** the bash process dies when the container stops, and we don't try to replay shell state across runs. Bash commands have side effects (`rm`, `git push`, `curl POST`, `psql -c "INSERT…"`) that a `cd + declare -px` snapshot can't capture or undo, so "restoring" a snapshot would silently drop every actual mutation. If the agent needs cross-run durable state, it should write to `/workspace` (a Docker named volume that survives container teardown) or persist specific values via [`kitaru.save()`](https://kitaru.ai/docs/guides/artifacts) and reload them with `kitaru.load()` at the top of the next run.

**Not yet here:** credentials still come from the host process; chapter 4 isolates them via a separate proxy container that injects `Authorization` headers based on host patterns.

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

The system prompt only says "find your skill and follow it." The actual procedure (which commands to run, what to summarize, how to return) lives in `skills/basic/default-agent/SKILL.md`. Edit the markdown, re-run, watch the agent's behavior change without touching Python. **When iterating on a SKILL.md, run with `DISABLE_CACHE=1` — otherwise the prior run's turn output is cached and your edits won't visibly change anything until the cache is invalidated.**

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

The proxy + mock containers print correlated logs. Container names are suffixed with the execution ID — `docker ps` shows them as `agent_factory_proxy_<id>` and `agent_factory_mock_<id>`. Tail one with `docker logs <name>` while the flow is running:

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
- **Network reachability is not the same as authentication.** The worker, proxy, and mock containers all share the `agent_factory` Docker network, so an in-worker process could `curl http://wiki.local/...` directly *without* going through the proxy. That request would fail with `401 unauthorized` because the mock checks `Authorization` and only the proxy holds the bearer — but the network path itself isn't blocked. If your real services rely on network ACLs as well as bearer auth, mirror that in your fork (separate networks per role, or egress policies on the worker).
- **The per-run proxy bearer is visible to the worker.** The worker has to authenticate to its own proxy, so the per-run token (`secrets.token_urlsafe(32)`) is embedded in the worker's `http_proxy` env as basic-auth-as-bearer. A prompt-injected agent inside the worker could read `$http_proxy` and use the proxy directly. That's fine — the proxy bearer only authorizes requests *through the proxy*, which is exactly what the worker is allowed to do; the secrets that matter (`wiki-token`, etc.) still live only on the proxy side and only get injected on host-matched outbound requests.

---

### Stage 5 — Your agents need typed services

**Stage file:** `stage_5_typed_services.py`
**The pitch:** not every agent call deserves a shell. When the agent needs *structured* input AND a *structured* response — posting a webhook, looking up records, hitting an internal control plane — give it a typed service instead of asking it to drive `curl` and parse JSON. Stage 5 introduces `exec_service`: one tool that dispatches to a discriminated union of typed services. Tool description is built dynamically from the profile's `allowed_services`, so the LLM only sees what this agent can actually call.

This is also where the **two credential paths** distinction lands:

| Path | Where it runs | Creds resolved | Stage |
|---|---|---|---|
| `exec` (sandboxed) | inside the worker container | proxy injects matching `Authorization` headers | 4 |
| `exec_service` (typed) | in the host process | host resolves `kitaru.get_secret(...)` directly | 5 |

The proxy is **not** involved for `exec_service`. Each path is right for different shapes of work — `exec` for shell-shaped operations the agent reasons about as command output, `exec_service` for typed operations where the structured result matters more than the bytes.

**One-time setup (same `setup.sh` as earlier stages — also adds the `webhook-token` secret):**

```bash
bash setup.sh
```

If you ran `setup.sh` before stage 5 landed, re-run it once — `setup.sh` is idempotent and now also creates `webhook-token` and builds `agent-factory-sandbox` if it's missing.

**Run it:**

```bash
DISABLE_CACHE=1 python stage_5_typed_services.py
```

The agent follows a skill mixing both paths — sandboxed `exec` for OS info, host-side `exec_service` for the typed lookup + publish:

```
[mock-services]  Started container … (host=http://localhost:54321)
[proxy]          Started container … (injecting for hosts=['wiki.local'])
[sandbox]        Started container … (proxy-wired)
Kitaru: Checkpoint `default` started.
[sandbox] $ cat /etc/os-release
[sandbox]   → exit=0, stdout=286 chars, cwd=/workspace
[sandbox] $ uname -r
[sandbox]   → exit=0, stdout=17 chars, cwd=/workspace
[mock-services]  GET /snippets/durability (host=localhost:54321, auth=Bearer w…) → 200
[mock-services]  POST /webhooks/team-summaries (host=localhost:54321, auth=Bot we…) → 200
Kitaru: Checkpoint `default` finished in 26.5s.
[sandbox] Stopping container …
[proxy]   Stopping container …
[mock-services] Stopping container …

Published 72f5f72ad544 at 1777887594: Durable execution persists every
checkpoint output so flows can pause for hours, survive host reboot,
and resume from the last completed checkpoint.
```

Notice what's *not* in the log: there are no `[sandbox]` lines for the `lookup_wiki` or `publish_summary` calls because they run host-side, not in the worker container. The proxy stays idle for those — `[proxy]` only appears at startup/teardown. Only the two `exec` shell commands traverse the sandbox.

**What's in it:**

- `agent_factory/services/schemas.py` — Pydantic models for each service's args + result (`LookupWikiArgs`, `LookupWikiResult`, `PublishSummaryArgs`, `PublishSummaryResult`, `WikiSnippet`).
- `agent_factory/services/registry.py` — `ALL_SERVICES: dict[str, ServiceCall]` mapping service name → `(args_model, handler, summary)`. `build_service_description(allowed)` renders the dynamic tool description from a set of allowed services.
- `agent_factory/services/lookup_wiki.py` + `publish_summary.py` — the host-side handlers. Each one resolves its credential via `kitaru.get_secret(...)` and makes an HTTP call to the mock.
- `agent_factory/tools.py` — `exec_service` tool factory. Validates `args` against the right Pydantic model on every call (`call.args_model.model_validate(args)`), so a malformed arg dict surfaces a Pydantic `ValidationError` to the agent rather than blowing up inside the handler.
- `agent_factory/profile.py` — adds `allowed_services: set[str]` field. The `exec_service` tool's description is built from this set, so the LLM sees only the services this agent can dispatch to.
- `mocks/server.py` — adds `POST /webhooks/{webhook_id}` (Discord-shaped, returns `{message_id, posted_at}`). Auth via `Authorization: Bot webhook-token`.
- `mocks/runner.py` — publishes the mock on a free host port (Docker assigns it) and exports `AGENT_FACTORY_MOCK_BASE_URL` in the host process environment so service handlers can reach it without taking the runner instance as a dependency. (Docker network aliases like `wiki.local` only resolve inside the `agent_factory` network — host-process calls need `localhost:<port>`.)
- `skills/with-services/default-agent/SKILL.md` — stage 5's skill. Uses `exec` for the OS-info steps and `exec_service` for the lookup + publish.

**Architectural notes:**

- **Why a flat `(service_name, args)` shape, not a typed union directly?** Some LLM providers handle JSON-Schema `oneOf`/`anyOf` inconsistently. A flat `service_name: str` + `args: dict` parameter shape is reliable across providers; the body re-validates by constructing `<ArgsModel>(**args)` based on the chosen `service_name`. The dynamic tool description embeds each service's args schema as plain text so the LLM sees what it needs to emit.
- **Why host-side, not in the sandbox?** Two reasons. (1) The result is structured — the agent doesn't need to parse `curl` output, it just gets `WikiSnippet` objects back. (2) Some services (publishing webhooks, hitting internal control planes) shouldn't run from the worker container at all; their credentials never touch the sandbox network. The host-side path keeps that separation explicit. A real platform engineer's skill can mix both — `exec` to grep a logfile inside the worker, then `exec_service` to file a typed record in their internal ticketing system.
- **Adding a new service is three files.** A new `<name>.py` handler, a new args + result Pydantic pair in `schemas.py`, a new entry in `ALL_SERVICES`. The tool surface — and its dynamic description — updates automatically.
- **HTTP errors surface to the agent as typed results, not exceptions.** `lookup_wiki` and `publish_summary` catch `urllib.error.HTTPError` and return a result with an error-shaped field (`topic="<error 401: …>"` or `message_id="<error 401: …>"`) so the LLM can reason about the failure rather than crashing the turn.

**Env-var toggles:** same as earlier stages (`DISABLE_CACHE=1`).

---

### Stage 6 — Your agents need to ask humans things

**Stage file:** `stage_6_hitl.py`
**The pitch:** some questions only the operator can answer — sign-off on irreversible actions, subjective preferences, ambiguous next-steps. Stage 6 introduces `ask_question`, a freeform HITL tool that pauses the flow until a human responds. The agent calls it like any other tool; the kitaru pydantic-ai adapter routes the call through `kitaru.wait()`. From the agent's perspective: `ask_question("...")` returns a string after a brief pause. From kitaru's perspective: the flow paused, durably, until input arrived.

Two ways to drive the demo — pick one:

**Interactive (recommended for first run):**

```bash
DISABLE_CACHE=1 python stage_6_hitl.py
```

When the agent calls `ask_question`, kitaru's local runtime prompts on the same terminal. Type your answer, hit enter, the flow resumes.

**Non-interactive (production-shaped):**

```bash
DISABLE_CACHE=1 python stage_6_hitl.py </dev/null &
# in another terminal, after `Waiting on ask_question:...` shows up:
kitaru executions list                                          # find the waiting execution
kitaru executions input <execution_id> --value '"Verified by ops on call"'
```

That's exactly how it works in production: the flow runs on a server, the operator answers via the dashboard / CLI / REST API. The flow resumes from the same point and continues.

> **Local-stack caveat:** the local kitaru runtime polls for input until a 600s timeout, so the non-interactive path *does* wait for the CLI command above on this stack. If you skip the answer, the flow eventually times out rather than auto-resolving. Behavior on remote stacks is the same shape — the difference is just where the wait record lives.

The agent's skill (`skills/with-hitl/default-agent/SKILL.md`) does this:

1. Look up `lookup_wiki(topic="durability")` (host-side typed service from stage 5).
2. Draft a 1-2 sentence summary from the first snippet.
3. Call `ask_question("What suffix should I add before publishing?")` — **the flow pauses here.**
4. Append the operator's answer to the summary.
5. Publish via `publish_summary`. Returns `{message_id, posted_at}`.

So the agent goes: typed-service lookup → human pause → publish, all in one durable flow.

```
Kitaru: Checkpoint `default` started.
…  (skill list/read, exec_service lookup_wiki)
Kitaru: Waiting on `ask_question:What suffix should I add to the summary…` (type=external_input, timeout=600s, poll=5s)
                                                                            ↑ flow paused here
…  (operator runs `kitaru executions input … --value '"Verified by ops on call"'`)
Kitaru: HTTP Request: POST https://api.openai.com/v1/chat/completions   ← agent resumes with the answer
…  (exec_service publish_summary)
Kitaru: Checkpoint `default` finished in 1m12s.

Published 4f12a87bc394 at 1777892841: Durable execution persists every checkpoint output. Verified by ops on call.
```

**What's in it:**

- `agent_factory/tools.py` — adds the `ask_question` tool factory. Body calls `wait_for_input(question=question, name=...)` from the kitaru pydantic-ai adapter. The wait returns whatever the operator supplies; the tool coerces to `str` for the agent's tool-result contract.
- `agent_factory/profile.py` — `ToolName` already includes `ask_question` (since stage 1's literal-typed `allowed_tools` field was forward-looking). Stage 6 just turns it on in `allowed_tools={...}`.
- `skills/with-hitl/default-agent/SKILL.md` — stage 6's procedure. Same shape as stage 5 but with a human pause between draft and publish.

**Architectural notes:**

- **No checkpoint plumbing on the example side.** The `wait_for_input` call inside the tool body is the only HITL-specific code in this stage. The adapter handles the suspend/resume mechanics under the hood; the tool body looks like a normal function call that happens to take a while.
- **Wait-name design is replay-safe.** Each wait is identified by a `name` of the form `ask_question:<call_index>:<sha1(question)[:8]>`. The call index makes two same-text questions in one turn distinguishable (the runtime DB enforces a unique constraint on `(run_id, name)`); the question hash makes the name stable across replays as long as the agent emits the same question at the same call index. For an end-to-end replay tour against a flow with cached waits, see [`/guides/replay-and-overrides`](https://kitaru.ai/docs/guides/replay-and-overrides).
- **Why `wait_for_input(...)` from the body, not `@hitl_tool(...)` on the function?** The shipped `@hitl_tool(schema=...)` form persists the `schema` value (a Python `type`) into the wait's metadata, which doesn't round-trip cleanly on the local stack today. Calling `wait_for_input(...)` from inside the tool body has the same external behavior without that snag.
- **Schema is `None` (any-JSON), not `str`.** The operator can supply any JSON value (a string, a structured object, etc.). The tool body coerces to `str` for the freeform shape. If you want typed answers (e.g. an enum of approval choices), pass a Pydantic model as `schema=` to `wait_for_input(...)` and the wait will validate the operator's input against it.
- **Operator input flows downstream verbatim.** Whatever the operator answers becomes the tool's return value, then is appended to the summary, then is POSTed to the webhook. No escaping, no re-prompting. Forks should escape it themselves before handing it to anything that interprets the bytes (HTML renderer, shell, SQL).

**Env-var toggles:** same as earlier stages (`DISABLE_CACHE=1`).

---

That's the full tour. Replay isn't covered as a separate chapter — it's a general Kitaru primitive that applies uniformly to any flow, not a platform-engineering capability specific to agent factories. See [Replay and Overrides](https://kitaru.ai/docs/guides/replay-and-overrides) when you want it.

---

## Architecture overview

Highlights of the architecture choices behind this tour:

- **Single shared library; no per-stage code copies.** The `agent_factory/` library grows monotonically; per-stage progressive disclosure happens through the Profile, not through versioning the library.
- **Two credential paths.** `exec` (sandboxed shell, proxy-injected creds) vs. `exec_service` (host-side typed call, direct `kitaru.secrets` resolution). Both demoed.
- **Profile is the central factory artifact.** Adding a new agent = creating a new Profile.
- **Layer-A smoke tests in `tests/test_agent_factory_example.py`.** Hermetic (no Docker, no real OpenAI), import-surface drift + schema validation, ~4s under xdist. Layer-B integration tests (full Docker spin-up + real OpenAI calls) deferred as a follow-up — the smoke layer is the cheap proxy that gates every PR.

## Forking for your team

When this example is mature, the fork-this-for-your-team guide lands here. For now: clone, swap `DEFAULT_PROFILE`'s system prompt and `allowed_tools` for whatever your agents need, point `LocalSkillSource` at your team's playbook directory, and update the mock services to talk to your real wiki/webhook endpoints.

## Tips for production use

- **Model aliases.** This example hardcodes a provider string (`"openai:gpt-5-nano"`) in the Profile so chapter 1 has the smallest possible setup. In production, prefer `kitaru model register <alias> --provider openai --model <id> --api-key ...` and reference the alias in your Profile so credentials are managed centrally.
