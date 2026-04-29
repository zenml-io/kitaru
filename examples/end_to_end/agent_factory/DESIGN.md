# agent_factory — Design

**Status:** in progress (brainstorm captured; implementation plan to follow)
**Branch:** `example/agent-factory`
**Date:** 2026-04-28
**Authors:** Hamza Tahir, with Claude (brainstorming)

This document is the working spec for a new flagship example in `examples/end_to_end/agent_factory/`. It captures the decisions locked during brainstorming, the architecture, and the still-open work. It is intended to be edited as the implementation progresses.

---

## 1. Goal & non-goals

### Goal

Ship a hero example for kitaru that demonstrates a **durable, sandboxed, profile-gated PydanticAI agent with two-process credential isolation** — the architecture pioneered by `kami` (an internal ZenML project), simplified to be locally runnable with one `docker compose up` and zero external accounts.

The example is the canonical "fork this to build your own agent" template, supporting a 5-post blog series and serving as the most ambitious entry in `examples/end_to_end/`.

### Non-goals

- Re-shipping `compliance_review/` (the existing Claude Agent SDK example stays in place; this example sits alongside it).
- Modal or any cloud sandbox runtime in chapter 1. A later chapter teases the Modal/Docker-compose-prod ladder; the hero example runs locally.
- Skill cloning from a remote git repo in chapter 1. Skills are local files bind-mounted into the worker. A later chapter introduces `GitRepoSkillSource`.
- Real third-party services (Discord, Slack, Notion, etc.) in chapter 1. A mock-services container provides drop-in fakes; the "swap mocks for real" migration is a later chapter.

---

## 2. Background

### What kami is

`kami` is an internal agent framework at ZenML that wraps `pydantic-ai` with three architectural distinctives:

1. **Profile-driven tool gating.** A `Profile` declares which tools and services an agent can use; a `PermissionHandler` gates every tool call against the profile.
2. **Two-sandbox credential isolation.** Agent shell commands run in a Modal "worker" sandbox that has no credentials. A second Modal "proxy" sandbox runs `mitmdump` with an addon that injects `Authorization` headers based on the request host. The worker is wired to the proxy via `http_proxy` / `REQUESTS_CA_BUNDLE` env vars and a self-signed CA cert.
3. **Discriminated-union typed services.** A `ServiceCall` Pydantic union dispatches to handlers (`PostDiscordMessage`, `ReadAttio`, etc.). The `exec_service` tool's description is built dynamically from the profile so the LLM only sees services it is allowed to call.

It is orchestrated end-to-end by a kitaru `@flow`, with `KitaruAgent` wrapping the `pydantic_ai.Agent` for granular per-tool checkpoints, a `kitaru.wait()`-backed `ask_human` HITL tool, and `kitaru.secrets` for credential resolution.

### Why a hero example

Kitaru's `examples/` gallery, after the recent restructure, has three buckets: `features/` (single-concept demos), `integrations/` (adapter showcases), `end_to_end/` (flagship agents). The `end_to_end/` gallery currently has `coding_agent`, `news_scout`, and `compliance_review`. Surveying those:

- `coding_agent` doesn't use PydanticAI (raw provider SDK).
- `news_scout` has no HITL, no sandbox, no skills.
- `compliance_review` uses Claude Agent SDK, no tools, no sandbox.

**No example combines PydanticAI + granular per-tool checkpoints + memory + HITL `wait()` + tool diversity + named artifacts + sandboxed execution with credential isolation.** That is the hero gap kami fills naturally. With the Modal layer stripped out and replaced by Docker, kami becomes a local-first, education-first hero example.

---

## 3. Locked decisions

These were resolved during brainstorming and are the foundation for everything below.

| # | Decision | Choice | Why |
|---|---|---|---|
| C | Scope/shape | **C: Mini-kami with simplified sandbox** | Demonstrates an architecture nobody else publishes as a hero example; goes beyond minimal kitaru tutorial. |
| C1 | Sandbox runtime | **C1: Local subprocess + local mitmdump, both as Docker containers** | Real isolation (separate filesystems, separate network namespaces) without a Modal account. C2/C3 (Docker-compose prod, Modal) become later chapters. |
| D2/E2 | Agent task | **Compliance/policy reviewer** (document processing) | Naturally exercises proxy injection (multiple auth-gated doc sources), per-clause HITL, memory precedents. Builds on `compliance_review`'s domain heritage but with a PydanticAI + sandbox stack. |
| F2 | Local-first strategy | **Bundled fixtures + local mock-services container** | Zero external accounts. The mock server is a teaching artifact: readers can `tail -f` it to see the proxy injecting auth headers. |
| G2 | Profile complexity | **Lean Profile, grows one field per stage** | Each stage adds exactly one Profile field and one architectural concept. Reader's mental model expands monotonically. |
| H1 | Profile representation | ~~**`Profile` Python dataclass**~~ → **`Profile` Pydantic `BaseModel`** *(reversed 2026-04-29)* | The CLAUDE.md convention is "prefer Pydantic models for data structures," and kitaru's own public models are all Pydantic. Pydantic gives us free `__init__` validation, declarative cross-field checks via `@model_validator`, JSON/YAML parsing for `load_profile(...)`, and idiomatic kitaru style. Markdown+frontmatter à la Claude Agent SDK is still deferred. |
| I2 | HITL design | **Per-clause HITL via `@hitl_tool`-decorated agent tool, suspended via the adapter's `wait_for_input(...)` helper** | The LLM decides when to pause for human input by calling `request_severity_decision`. The flow body never calls `kitaru.wait()` directly; the tool body does, *through* the adapter's exported `wait_for_input(...)`. The decorator marks the tool for capture/permission/system-prompt purposes; the helper tags the wait with adapter metadata for dashboard/OTel correlation. Memory read + write are co-located in the same tool body so each clause is one self-contained "consult precedents → ask human → record" round-trip. |
| J2 | Memory design | **Flow-scope precedents + execution-scope findings** | Cross-execution learning: `request_severity_decision` reads precedents, suggests defaults, writes confirmed decisions back. After 2-3 runs, the agent visibly "gets smarter." |
| K3 | Tool inventory | **Five tools: `exec`, `fetch_document`, `request_severity_decision`, `publish_review`, `skill`** | Full kami parity. The `skill` tool is the architectural distinctive that separates "main loop" from "capabilities" — the agent's procedure lives in markdown, not Python. |
| L2 | Replay scenario | **"Policy change" replay (edit fixture file, replay from `check_clause_2`)** | The most realistic replay scenario for the compliance domain. No version metadata, no v1/v2 schema — replay overrides the `load_policy` checkpoint output: `overrides={"checkpoint.load_policy": <new policy text>}`. |
| L2-impl | Replay-input mechanism | **Checkpoint-output override on `load_policy`** | A `@checkpoint def load_policy(path) -> str` runs early in the flow; its output is the artifact replay swaps. Keeps `find_clauses` cached on its own input (the doc), so replay re-executes only from `check_clause_2` onward. Demos `overrides={"checkpoint.*": ...}` — a kitaru feature no other example covers. Accepted 2026-04-29. |
| H1-tools | `allowed_tools` typing | **`set[ToolName]` with `ToolName = Literal[...]` of the five tool names** | Closed set per K3, so a Literal alias gives readers IDE autocomplete and ty/ruff errors on typos at zero cost. Set (not list) expresses membership semantics and dedupes. Accepted 2026-04-29. |
| M3 + N2 | Location & name | **`examples/end_to_end/agent_factory/`** | New directory in the gallery; `compliance_review/` stays as the SDK flavor. Generic name signals "the canonical template," not a domain-specific demo. |

---

## 4. Runtime topology

```
┌─ host ────────────────────────────────────────────────────────────┐
│                                                                   │
│   kitaru @flow ──▶ KitaruAgent(pydantic_ai.Agent)                 │
│                          │                                        │
│                          │ tool: exec("curl wiki.local/...")     │
│                          ▼                                        │
│                docker exec worker_<exec_id>                       │
└────────────────────────────────────────┼──────────────────────────┘
                                         │
   ┌─ docker network "agent_factory" ────┼─────────────────────────┐
   │                                     ▼                         │
   │   ┌─ worker_<exec_id> ───┐  HTTP   ┌─ proxy ──────┐           │
   │   │ bash + curl + tools  │ via     │ mitmdump +   │           │
   │   │ trusts CA            │ proxy:  │ kami addon   │           │
   │   └──┬───────────┬───────┘ 8080    │ KAMI_CREDS={ │           │
   │      │           │ ───────────────▶│  wiki.local, │           │
   │      │           │                 │  policies.., │           │
   │      │           │                 │  docstore..} │           │
   │      │           │                 └──────┬───────┘           │
   │      │           │                        │                   │
   │      │ /workspace│   /skills              │ /certs            │
   │      ▼           ▼                        ▼                   │
   │  workspace_<id>  skills/ (host bind, RO)  ca_certs (vol)      │
   │  (named vol)                                                  │
   │                                                               │
   │                              ┌─ mock-services ──┐             │
   │                              │ FastAPI mocks    │             │
   │                              └──────────────────┘             │
   └───────────────────────────────────────────────────────────────┘
```

### Why this topology

- The kitaru flow stays on the host so `kitaru.wait()` resumption and the kitaru CLI feel native to the operator.
- The worker is a real container — separate filesystem, separate network namespace. It *cannot* read the proxy's `KAMI_CREDENTIALS` env var. That is the architectural distinctive that makes this example novel.
- The proxy is a real container running real `mitmdump` with the kami addon. Same code path as kami today, minus Modal.
- `mock-services` runs *inside the same Docker network*, so the proxy intercepts traffic to `*.local` without `/etc/hosts` hacks or DNS wrangling. Reader gets it on `docker compose up` without OS-specific setup.

### Volumes — durability story

| Volume | Type | Mounted in | Lifetime | Purpose |
|---|---|---|---|---|
| `workspace_<exec_id>` | Named, scoped to kitaru execution_id | worker → `/workspace` (rw) | One per flow run; cleaned up on flow completion (configurable retention) | The agent's working filesystem. Survives `kitaru.wait()` pauses and host reboots. If the worker container is recreated mid-flow, the new one mounts the same volume and the agent picks up where it left off. |
| `agent_factory_certs` | Named | proxy → `/certs/private` (rw); worker → `/usr/local/share/ca-certificates/` (ro) | Persistent across runs; generated once via init script | Self-signed CA: full PEM in proxy, public cert only in worker. Worker's `update-ca-certificates` runs at container start. |
| `./skills/` | Host bind mount | worker → `/skills` (ro) | Persistent; operator-edited | Operators edit `skills/compliance-reviewer/SKILL.md`; worker sees the changes on the next flow run. |

The per-execution workspace volume is what makes "your sandbox state is durable" demonstrable: a flow can pause for hours on a `request_severity_decision`, the host can reboot, and on resume the agent's `/workspace/extracted_clauses.txt` is exactly where it was.

---

## 5. File layout

```
examples/end_to_end/agent_factory/
├── README.md                          # Tour, run instructions, blog series links
├── DESIGN.md                          # This document
├── pyproject.toml                     # Extras pinning, follows kitaru convention
├── docker-compose.yml                 # worker, proxy, mock-services, network, volumes
│
├── stage_1_basic_agent.py             # Profile + KitaruAgent + 1 in-process exec tool
├── stage_2_sandboxed_exec.py          # Same agent, exec runs in Docker worker
├── stage_3_credential_proxy.py        # Adds sandbox_proxy_rules + KAMI_CREDENTIALS
├── stage_4_full_agent_factory.py      # Full mini-kami: HITL + memory + skills + services
├── stage_5_replay.py                  # Replay stage_4 with a tightened policy override
│
├── agent_factory/                     # The reusable library code
│   ├── __init__.py
│   ├── profile.py                     # Profile dataclass, ServicePermission, etc.
│   ├── agent.py                       # build_agent() — wires Profile -> KitaruAgent
│   ├── permissions.py                 # PermissionHandler
│   ├── tools.py                       # All 5 tool factories, gated by PermissionHandler
│   ├── sandbox/
│   │   ├── __init__.py
│   │   ├── worker.py                  # DockerWorker — analog of kami's ModalSandboxRuntime
│   │   ├── proxy.py                   # DockerProxy — analog of kami's ProxySandbox
│   │   ├── proxy_addon.py             # mitmproxy addon — host-pattern header injection
│   │   └── certs.py                   # ensure_certs() — local CA generation
│   ├── services/
│   │   ├── __init__.py
│   │   ├── schemas.py                 # ServiceCall discriminated union
│   │   ├── base.py                    # BaseService[T]
│   │   ├── registry.py                # ALL_SERVICES
│   │   ├── fetch_document.py
│   │   └── publish_review.py
│   ├── secrets.py                     # KitaruCredentialBroker, secret template parser
│   └── memory.py                      # flow-scope precedent helpers
│
├── skills/
│   └── compliance-reviewer/
│       └── SKILL.md                   # The agent's playbook (host-edited, bind-mounted)
│
├── fixtures/
│   ├── policy_v1.md                   # Default policy fixture
│   ├── policy_strict.md               # L2 replay fixture (the "tightened" policy)
│   └── docs/
│       └── sample_dpa.pdf             # Sample doc to review
│
├── mocks/
│   ├── Dockerfile
│   ├── server.py                      # FastAPI: wiki.local, policies.local, docstore.local, discord.local
│   └── fixtures.py                    # In-memory data
│
├── docker/
│   ├── worker.Dockerfile              # bash, curl, jq, pdftotext, CA install
│   └── proxy.Dockerfile               # mitmdump + addon
│
└── tests/
    ├── test_stage_1.py                # Smoke test for stage 1
    ├── test_proxy_injection.py        # Verify header injection works
    └── test_full_loop.py              # End-to-end happy path with mocks
```

### Reading order for the reader

1. `README.md`
2. `stage_1_basic_agent.py` (run it; understand the bare flow)
3. `stage_2_sandboxed_exec.py` (run it; see Docker worker isolating exec)
4. `stage_3_credential_proxy.py` (run it; watch the proxy inject headers)
5. `stage_4_full_agent_factory.py` (run it; the real thing)
6. `stage_5_replay.py` (run it; durability's payoff)

Each stage is one Python file that imports from the `agent_factory/` library. Stages don't duplicate logic — they progressively expose more of the library. The library is written once; stages compose it.

### Notable design choices

- **No `bootstrap.py`.** Kami's bootstrap mutates several module-level singletons; for an example we want to avoid teaching that pattern. Stage 4's flow body does the equivalent setup explicitly so readers can see it. Cleaner teaching, ~30 fewer lines of indirection.
- **Tests ship with the example.** Three small tests prove the architecture works — proxy injection, full loop with mocks, stage 1 smoke. Doubles as CI for the kitaru repo.

---

## 6. Profile schema (in progress)

The Profile grows one field per stage (decision G2). Resolved so far:

```python
ToolName = Literal[
    "exec",
    "fetch_document",
    "request_severity_decision",
    "publish_review",
    "skill",
]

class Profile(BaseModel):
    name: str
    system_prompt: str
    model: str                                    # raw pydantic-ai provider string
    allowed_tools: set[ToolName] = Field(default_factory=set)
    # Stage 3+:
    sandbox_proxy_rules: list[SandboxProxyRule] = Field(default_factory=list)
    # Stage 4+:
    service_configs: dict[str, ServiceConfig] = Field(default_factory=dict)
    skill_sources: list[SkillSource] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check_proxy_rules_have_services(self) -> "Profile":
        rule_hosts = {rule.host for rule in self.sandbox_proxy_rules}
        orphan = rule_hosts - set(self.service_configs)
        if orphan:
            raise ValueError(
                f"sandbox_proxy_rules reference hosts with no service_configs entry: {sorted(orphan)}"
            )
        return self
```

`load_profile(name)` deserializes a YAML/JSON file under `agent_factory/profiles/<name>.yaml` via `Profile.model_validate(...)` — Pydantic does the validation, no extra plumbing.

Per-stage growth:

| Stage | Profile fields |
|---|---|
| 1 | `name`, `system_prompt`, `model`, `allowed_tools` |
| 2 | + (no new field; `allowed_tools` includes `exec`, sandbox runtime injected via flow setup) |
| 3 | + `sandbox_proxy_rules: list[SandboxProxyRule]` |
| 4 | + `service_configs: dict[str, ServiceConfig]`, `skill_sources: list[SkillSource]` |
| 5 | (no new field; replay reuses stage 4's profile) |

### Model field

`model` is a raw pydantic-ai provider string (e.g. `"openai:gpt-4o-mini"`), not a kitaru model alias. Reasons:

- The chapter-1 hook is "durable agent in 60 lines" — adding a `kitaru model register ...` step before stage 1 runs adds friction on the wrong path.
- `"openai:gpt-4o-mini"` is the canonical pydantic-ai form; matching it makes the example read as a natural extension of pydantic-ai's own docs.
- This example has a single model call-site (the agent itself); the alias indirection has no payoff yet. The README's "Tips for production use" section mentions `kitaru model register` as the preferred path once there are multiple model call-sites or shared credentials.

### Validation

Handled by Pydantic per the reversed H1: built-in type checks at construction, plus `@model_validator(mode="after")` hooks for cross-field invariants (e.g. proxy rules reference declared services). `Profile.model_validate_json(...)` is the on-disk deserializer.

---

## 7. The five tools (TODO — section 4 of brainstorm)

All five are gated by `PermissionHandler.require_tool(name)` at execution time, even though `build_tools()` already filters by `allowed_tools`. Defense in depth — same pattern as kami.

The HITL tool integrates with PydanticAI through the kitaru adapter's two-piece seam:

- **`@hitl_tool(...)`** (marker decorator) — registration-time metadata: `name`, `schema`, static `question`. Read by `PermissionHandler`, capture policy, and the system-prompt builder so the LLM only sees the tool when it's allowed and capture/redaction rules know it's HITL.
- **`wait_for_input(...)`** (adapter-exported helper) — invocation-time pause. Calls `kitaru.wait()` underneath but tags the resulting wait record with `kitaru.adapter.id == "pydantic-ai"` and `source == "tool_body"` so the dashboard, OTel spans, and tool-event children correlate.

```python
from kitaru.adapters.pydantic_ai import hitl_tool, wait_for_input

@hitl_tool(
    name="request_severity_decision",
    schema=Severity,
    question="Approve or override this severity verdict.",
)
def request_severity_decision(
    ctx: RunContext[ReviewDeps],
    clause_id: str,
    clause_text: str,
    pattern: str,
) -> Severity:
    precedents = kitaru.memory.get("compliance-precedents", default=[])
    suggested = _suggest_from_precedents(pattern, precedents)

    decision: Severity = wait_for_input(
        schema=Severity,
        name=f"severity_decision_{clause_id}",
        question=(
            f"Clause {clause_id}: {clause_text!r}\n"
            f"Pattern: {pattern}\n"
            f"Suggested: {suggested or 'none'}"
        ),
        metadata={"clause_id": clause_id, "pattern": pattern, "suggested": suggested},
    )

    _record_precedent(precedents, pattern, decision)
    kitaru.memory.set("compliance-precedents", precedents)
    return decision
```

The `CallDeferred` / `ApprovalRequired` / `requires_approval=True` paths exist in pydantic-ai and bridge to `kitaru.wait()` automatically too, but they don't fit this tool's shape: per-clause severity needs a value back from the human (not a yes/no approval) *and* the memory write-back has to be co-located with the wait so each clause is a self-contained read-ask-record cycle. Raising `CallDeferred` would split that into two tools and rely on the LLM to call the second one — an architectural step backwards.

| Tool | Type | What it does | Stage introduced |
|---|---|---|---|
| `exec` | shell | Runs a bash command in the worker container; routes HTTP through proxy. | 1 (in-process), 2 (Docker worker) |
| `fetch_document` | typed `exec_service` | Discriminated-union service call to read a doc from a mock store. | 4 |
| `request_severity_decision` | `@hitl_tool` + `wait_for_input(...)` | Suspends the agent and asks the operator for a severity verdict on a clause. Schema: `Severity = Literal["sev-1", "sev-2", "sev-3", "n/a"]`. Tool body: read flow-scope precedents → suggest a default → call adapter's `wait_for_input(schema=Severity, ...)` to suspend → write the confirmed decision back to precedent memory → return it to the agent. | 4 |
| `publish_review` | typed `exec_service` | Posts the final review report to a mock Discord-like service. | 4 |
| `skill` | local-fs | `list` / `read` / `search` over `skills/` to load the agent's playbook. | 4 |

Exact tool descriptions, error handling, log redaction strategy: **TODO — to be answered when Section 4 of brainstorming resumes.**

---

## 8. Flow lifecycle (in progress)

Resolved so far:

- `policy_path` is the input to a `@checkpoint def load_policy(path) -> str`. The agent reads the policy text via the checkpoint's output, not from disk directly. L2 replay overrides `checkpoint.load_policy` (decision L2-impl).
- `find_clauses` is a separate `@checkpoint` whose only input is the doc/prompt, so the doc-extraction stays cached when the policy alone changes during replay.

### Sandbox lifecycle: eager + context-managed

Containers are started **eagerly** at flow entry, before the agent's first turn, and torn down via context managers on any exit path. Kami's lazy startup is reintroduced in the post-chapter "Going to production: Docker-compose to Modal," where Modal cold-start cost is real and billed.

Reasons for eager here:

- Chapter 2's job is to *show the worker booting* as a visible, sequential event — lazy hides it inside a tool call.
- "Docker daemon not running" is the most common fresh-clone error; eager surfaces it as a top-level `KitaruRuntimeError` before any LLM token is spent.
- Replay determinism: with eager startup the sandbox is up before `from_=...` re-executes, so "what gets reused vs. re-run" stays clear.
- Docker cold-start is 1–3 s and free locally; the "infrastructure costs nothing if unused" pitch is a Modal pitch, not a local pitch.

```python
@flow(image=ImageSettings(...))
def kami_main_flow(
    profile_name: str,
    prompt: str,
    policy_path: str = "fixtures/policy_v1.md",
) -> dict:
    profile = load_profile(profile_name)
    policy_text = load_policy(policy_path)            # @checkpoint
    extracted = find_clauses(prompt)                  # @checkpoint
    with (
        DockerProxy.start(credential_map=build_proxy_credential_map(profile)) as proxy,
        DockerWorker.start(
            execution_id=kitaru.runtime.execution_id(),
            proxy=proxy,
        ) as sandbox,
    ):
        agent = build_agent(
            profile,
            permission_handler=PermissionHandler(profile),
            sandbox=sandbox,
        )
        result = agent.run_sync(prompt, deps={"policy": policy_text, "clauses": extracted})
    return {"summary": result.output, "exec_id": kitaru.runtime.execution_id()}
```

Context managers also guarantee teardown on exception — a leaked `mitmdump` container holding a per-run bearer token on the network is the alternative footgun.

---

## 9. Memory & artifacts (TODO — section 6 of brainstorm)

### Flow-scope precedents

- Key: `compliance-precedents`
- Shape: `list[ClausePattern]` where `ClausePattern = {pattern: str, severity: Severity, count: int, last_seen: datetime}`
- Read by: `request_severity_decision` (to pre-fill suggested severity)
- Written by: `request_severity_decision` (after the operator confirms)

### Execution-scope findings

- Key: `findings`
- Shape: `list[Finding]` where `Finding = {clause_id: str, text: str, severity: Severity, rationale: str}`
- Written incrementally as the agent works through the doc

### Artifacts

- `final_review` (named artifact, type=`output`) — the published Markdown report
- `extracted_clauses` (named artifact, type=`context`) — intermediate output from `find_clauses` checkpoint, retained for replay

**Detailed shapes and exact memory access points: TODO.**

---

## 10. Sandbox & proxy (TODO — section 7 of brainstorm)

### Worker container

- Base image: `python:3.11-slim` + `bash`, `curl`, `jq`, `poppler-utils` (for `pdftotext`)
- CA cert installed via `update-ca-certificates`
- Runs `tail -f /dev/null` as PID 1 — the host opens a persistent bash via `docker exec -i worker_<id> bash` and reuses it across the flow's exec calls (matches kami's persistent-bash pattern)
- Env vars set at container start: `http_proxy`, `https_proxy`, `HTTP_PROXY`, `HTTPS_PROXY`, `no_proxy`, `REQUESTS_CA_BUNDLE`, `SSL_CERT_FILE`, `NODE_EXTRA_CA_CERTS`, `PIP_CERT`

### Proxy container

- Base image: `mitmproxy/mitmproxy:latest`
- Runs `mitmdump -s /opt/proxy_addon.py --listen-port 8080 --proxyauth ...`
- Env: `KAMI_CREDENTIALS={...}`, `KAMI_PROXY_TOKEN=<random-per-run>`
- The proxy URL embedded in worker env vars is `http://<token>:@proxy:8080` (basic-auth-as-bearer pattern, same as kami)

### Per-run bearer token

Generated at flow start via `secrets.token_urlsafe(32)`, passed to proxy via env, embedded in proxy URL. Proxy addon validates `Proxy-Authorization` header on every request and rejects unauthenticated traffic. This prevents other processes on the host from accidentally using the proxy.

### Marker-based command completion

The same b64 + completion-marker trick kami uses (`tools.py` in kami), allowing the host to read full stdout/stderr without shell buffering issues.

**Exact mitmproxy addon code: port from kami's `proxy_addon.py` with cosmetic adjustments. TODO.**

---

## 11. Mock services (TODO — section 7 of brainstorm)

A FastAPI server exposing four virtual hosts on the same Docker network:

| Host | Endpoints | Auth | Returns |
|---|---|---|---|
| `wiki.local` | `GET /precedents/{topic}` | `Authorization: Bearer wiki-token` | Sample prior-review snippets |
| `policies.local` | `GET /policy/{name}` | `Authorization: Bearer policy-token` | Markdown policy files |
| `docstore.local` | `GET /docs/{doc_id}` | `Authorization: Bearer docstore-token` | PDFs (or text equivalents for the mock) |
| `discord.local` | `POST /webhooks/{id}` | `Authorization: Bot discord-token` | 204 on success; logs payload to stdout for the reader to see |

Each mock host's auth is wired up in the Profile as a separate `SandboxProxyRule`. The reader can `docker compose logs -f mock-services` to see exactly which auth headers arrive at which endpoints — concrete demo of the proxy isolation.

**TODO — exact request/response schemas, fixture data.**

---

## 12. Replay scenario (L2)

`stage_5_replay.py` (~50 lines) runs `stage_4_full_agent_factory.kami_main_flow.run(...)` to seed an exec_id (or accepts one as a CLI arg if stage 4 was already run), then:

```python
new_policy = Path("fixtures/policy_strict.md").read_text()
flow_handle = kami_main_flow.replay(
    exec_id=stage_4_exec_id,
    from_="check_clause_2",
    overrides={"checkpoint.load_policy": new_policy},
)
result = flow_handle.wait()
print_diff(stage_4_findings, result.findings)
```

The earlier `find_clauses` and `load_policy` checkpoints are independent: `load_policy`'s output is overridden directly with the strict policy text, while `find_clauses` stays cached because its inputs (the doc) didn't change. From clause 2 onward the agent re-evaluates against the new policy; the precedent-memory from the prior run informs new severity suggestions; the operator only re-confirms the *changed* decisions.

The README has a prose section explaining what gets cached vs. re-executed and links to `examples/features/replay/replay_with_overrides.py` for a deeper dive on the replay primitive.

---

## 13. Blog series chapter map (TODO — section 8 of brainstorm)

Tentative:

| Chapter | Title | Stage file | Concepts introduced |
|---|---|---|---|
| 1 | Building a durable agent on kitaru | `stage_1_basic_agent.py` | `@flow`, `KitaruAgent`, `Profile`, `PermissionHandler`, granular checkpoints |
| 2 | Sandboxing your agent's shell | `stage_2_sandboxed_exec.py` | DockerWorker, persistent-bash, workspace volumes |
| 3 | The two-process credential isolation pattern | `stage_3_credential_proxy.py` | DockerProxy, mitmproxy addon, `sandbox_proxy_rules`, secret templates |
| 4 | HITL, memory, and the agent's playbook | `stage_4_full_agent_factory.py` | `@hitl_tool`, flow-scope memory, `skill` tool, discriminated-union services |
| 5 | When durability pays off: replay with overrides | `stage_5_replay.py` | `flow.replay()`, `from_=`, `overrides=`, what gets cached |
| (post) | Going to production: from Docker-compose to Modal | (no new stage) | C2/C3 ladder, deploying the same architecture remotely |

**Chapter outlines, opening hooks, code excerpts: TODO.**

---

## 14. Local development (TODO — section 7 of brainstorm)

```bash
cd examples/end_to_end/agent_factory
uv sync --extra local --extra pydantic-ai --extra agent-factory   # new extra
docker compose up -d                                              # worker, proxy, mock-services
python stage_1_basic_agent.py                                     # Run it!
```

Cleanup:

```bash
docker compose down -v   # drops named volumes, including workspace_<exec_id>
```

**The new `--extra agent-factory` adds `mitmproxy`, `cryptography`, `docker` (Python SDK), and dev-only deps. To be added to the root `pyproject.toml`. TODO.**

---

## 15. Tests & verification

Three tests ship with the example, run via `pytest examples/end_to_end/agent_factory/tests/`:

- `test_stage_1.py` — smoke test that stage 1 runs, completes, returns a non-empty result.
- `test_proxy_injection.py` — starts the proxy + worker + mocks, has the worker `curl` `wiki.local`, asserts the mock-services container received the expected `Authorization` header. *This is the central architectural test.*
- `test_full_loop.py` — end-to-end stage 4 happy path with a scripted operator response (a kitaru fixture that auto-resolves the HITL wait with predetermined severities).

Tests must be runnable in CI with no external accounts. They depend on Docker being available on the runner.

---

## 16. Open questions

Tracked here so they don't get lost. Several map to brainstorm sections we have not yet completed.

- **Section 3 — Profile schema details.** Resolved: Pydantic `BaseModel` (H1 reversed); `allowed_tools: set[ToolName]` with `ToolName = Literal[...]`; `model` is a raw pydantic-ai provider string; cross-field validation via `@model_validator`.
- **Section 4 — Tool details.** Exact descriptions, error handling, redaction.
- **Section 5 — Flow lifecycle.** Resolved: eager + context-managed sandbox/proxy startup; `policy_path` carried via `load_policy` checkpoint (decision L2-impl).
- **Section 6 — Memory & artifacts.** Exact data shapes, access points, retention policy.
- **Section 7 — Sandbox/proxy implementation.** Code-level details, addon port, mock-services schemas.
- **Section 8 — Blog series.** Chapter outlines, hooks, code excerpts.
- **Tool descriptions in profile system prompt.** Should the system prompt enumerate available skills, or rely on the agent calling `skill list` first?
- **CI strategy.** Does the existing kitaru CI runner have Docker? What's the right test-extra for `agent_factory`?
- **Naming check.** Confirm with maintainers that `example/agent-factory` and `examples/end_to_end/agent_factory/` are acceptable.

---

## 17. Implementation phasing (rough)

When we transition to implementation, the natural phases are:

1. **Skeleton.** `agent_factory/` library scaffolding (Profile, PermissionHandler, empty tool factories), `pyproject.toml`, `docker-compose.yml`, README stub.
2. **Stage 1.** `stage_1_basic_agent.py` runs end-to-end with one in-process exec tool.
3. **Sandbox layer.** DockerWorker, worker Dockerfile, persistent-bash mechanism. `stage_2_sandboxed_exec.py` runs.
4. **Proxy layer.** DockerProxy, proxy addon, certs module, credential broker. `stage_3_credential_proxy.py` runs and the proxy injection test passes.
5. **Full agent.** HITL tool, memory helpers, services, skill tool, all profile fields. `stage_4_full_agent_factory.py` runs end-to-end.
6. **Replay.** `stage_5_replay.py` runs against a stage-4 exec_id.
7. **Tests + docs.** Three tests pass, README is complete, blog post #1 drafted.

Each phase is a candidate for its own implementation plan via `superpowers:writing-plans`.

---

*This document will be updated as brainstorming sections 3–8 are resolved and as implementation reveals new questions.*
