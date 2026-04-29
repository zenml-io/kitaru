# agent_factory — Design

**Status:** brainstorm complete (resolved 2026-04-29); implementation plan to follow
**Branch:** `example/agent-factory`
**Date:** 2026-04-28 (initial), 2026-04-29 (resolution pass grounded in the kami codebase at `/Users/htahir1/Workspace/kami-agent`)
**Authors:** Hamza Tahir, with Claude (brainstorming)

This document is the working spec for a new flagship example in `examples/end_to_end/agent_factory/`. It captures the decisions locked during brainstorming, the architecture, and the still-open work. It is intended to be edited as the implementation progresses.

---

## 1. Goal & non-goals

### Goal

Ship a hero example for kitaru that demonstrates a **durable, sandboxed, profile-gated PydanticAI agent with two-process credential isolation** — the architecture pioneered by `kami` (an internal ZenML project), simplified to be locally runnable with one `docker compose up` and zero external accounts.

The example is the canonical "fork this to build your own agent" template, supporting a 7-chapter blog series (plus a post-chapter on Modal — see Section 13) and serving as the most ambitious entry in `examples/end_to_end/`.

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

It is orchestrated end-to-end by a kitaru `@flow`, with `KitaruAgent` wrapping the `pydantic_ai.Agent` for granular per-tool checkpoints, a `kitaru.wait()`-backed `ask_human` HITL tool (which our example generalizes into the typed-union `ask_question` dispatcher — see decision K3 and Section 7), and `kitaru.secrets` for credential resolution.

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
| H1 | Profile representation | **`Profile` Pydantic `BaseModel`** *(updated 2026-04-29 — kami's actual Profile, not the dataclass mentioned in older notes)* | Initial brainstorm cited a "kami uses dataclass" claim that turned out to be stale; reading `kami_agent/profiles.py:105` shows kami's `Profile` is already a Pydantic `BaseModel`. The CLAUDE.md convention is "prefer Pydantic models for data structures," kitaru's own public models are all Pydantic, and kami matches. Markdown+frontmatter à la Claude Agent SDK is still deferred. |
| I2 | HITL design | **Per-clause HITL via the `ask_question` typed-union dispatcher; the `severity_decision` kind suspends through the adapter's `wait_for_input(...)` helper** | The LLM decides when to pause for human input by calling `ask_question(kind="severity_decision", args={...})`. The flow body never calls `kitaru.wait()` directly; the tool body does, *through* the adapter's exported `wait_for_input(...)`. `@hitl_tool` marks the tool for capture/permission/system-prompt purposes; `wait_for_input` tags the wait with adapter metadata for dashboard/OTel correlation. Memory read + write are co-located in the `severity_decision` branch so each clause is one self-contained "consult precedents → ask human → record" round-trip. The same tool also covers freeform LLM-driven HITL via `kind="freeform"` (the `ask_human` use case from kami), so the example exposes both LLM-driven and workflow-driven HITL through a single architectural seam. |
| J2 | Memory design | **Flow-scope precedents + execution-scope findings** | Cross-execution learning: the `severity_decision` branch of `ask_question` reads precedents, suggests defaults, writes confirmed decisions back. After 2-3 runs, the agent visibly "gets smarter." |
| K3 | Tool inventory | ~~Five tools: `exec`, `fetch_document`, `request_severity_decision`, `publish_review`, `skill`~~ → **Four tools: `exec`, `skill`, `exec_service`, `ask_question`** *(reversed 2026-04-29 — kami parity claim was inaccurate)* | Kami actually has four tools (`exec`, `skill`, `ask_human`, `exec_service`), not the five we initially listed. Services dispatch through `exec_service`'s discriminated `ServiceCall` union (cases: `lookup_precedent`, `publish_review`). HITL is generalized into `ask_question` — the kami `ask_human` upgraded to a typed-union dispatcher mirroring `exec_service`'s shape (kinds: `freeform`, `severity_decision`). The `skill` tool stays as the architectural distinctive that separates "main loop" from "capabilities" — the agent's procedure lives in markdown, not Python. |
| K3-services | Service shape vs. proxy demo | **Document fetching is `exec("curl docstore.local/...")`, NOT an `exec_service` case** *(decided 2026-04-29 after spotting the inconsistency)* | `exec_service` runs in the host process and resolves credentials directly via `kitaru.secrets`, **bypassing the proxy entirely**. If `fetch_document` were an `exec_service` case, chapter 3's "watch the proxy inject auth headers" demo would never fire for doc fetching — the host would fetch directly. Doc fetching is exactly the shape `exec` + proxy were designed for, so it lives there. The second `exec_service` case is `lookup_precedent` (a typed wiki query returning structured precedent snippets) — host-side, fits the typed-service pattern, fits the compliance domain. |
| L2 | Replay scenario | **"Policy change" replay (edit fixture file, replay from `check_clause_2`)** | The most realistic replay scenario for the compliance domain. No version metadata, no v1/v2 schema — replay overrides the `load_policy` checkpoint output: `overrides={"checkpoint.load_policy": <new policy text>}`. |
| L2-impl | Replay-input mechanism | **Checkpoint-output override on `load_policy`** | A `@checkpoint def load_policy(path) -> str` runs early in the flow; its output is the artifact replay swaps. Keeps `find_clauses` cached on its own input (the doc), so replay re-executes only from `check_clause_2` onward. Demos `overrides={"checkpoint.*": ...}` — a kitaru feature no other example covers. Accepted 2026-04-29. |
| H1-tools | `allowed_tools` typing | **`set[ToolName]` with `ToolName = Literal["exec", "skill", "exec_service", "ask_question"]`** | Closed set per K3 (revised), so a Literal alias gives readers IDE autocomplete and ty/ruff errors on typos at zero cost. Set (not list) expresses membership semantics and dedupes. Accepted 2026-04-29. |
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

The per-execution workspace volume is what makes "your sandbox state is durable" demonstrable: a flow can pause for hours on an `ask_question(kind="severity_decision", ...)` wait, the host can reboot, and on resume the agent's `/workspace/extracted_clauses.txt` is exactly where it was.

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
│   │   ├── lookup_precedent.py
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
├── setup.sh                           # One-time `kitaru secrets create` commands for stages 3+
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

Field shapes follow kami exactly (`kami_agent/profiles.py`), with `allowed_tools` upgraded from `list[str]` to `set[ToolName]` (Q2):

```python
ToolName = Literal["exec", "skill", "exec_service", "ask_question"]

class ServiceConfig(BaseModel):
    enabled: bool = True
    secret_reference: str                                  # e.g. "{{ wiki-token.value }}"
    config: dict[str, Any] = Field(default_factory=dict)

class SandboxProxyRule(BaseModel):
    name: str
    hosts: list[str]                                       # multiple hosts per rule
    headers: dict[str, str]                                # full headers, with secret templates

class LocalSkillSource(BaseModel):
    type: Literal["local"] = "local"
    path: str                                              # bind-mounted; no clone

class GitRepoSkillSource(BaseModel):
    type: Literal["git_repo"] = "git_repo"
    repo_url: str
    authorization_header: str | None = None

SkillSource = Annotated[
    LocalSkillSource | GitRepoSkillSource,
    Field(discriminator="type"),
]

class Profile(BaseModel):
    name: str
    system_prompt: str
    model: str                                             # raw pydantic-ai provider string
    allowed_tools: set[ToolName] = Field(default_factory=set)
    # Stage 3+
    sandbox_proxy_rules: list[SandboxProxyRule] = Field(default_factory=list)
    # Stage 4+
    service_configs: dict[str, ServiceConfig] = Field(default_factory=dict)
    skill_sources: list[SkillSource] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check_proxy_rules(self) -> "Profile":
        # Cross-field: every host pattern that uses a service-derived secret template
        # must reference a service the profile actually configures. (Heuristic: any
        # header value with a `{{ <service>.* }}` template.)
        ...
        return self
```

The chapter 1 default profile uses `LocalSkillSource(path="./skills/compliance-reviewer")` — bind-mounted, no clone. `GitRepoSkillSource` is teased for the post-chapter "remote skills" follow-up.

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

## 7. The four tools

All four are gated by `PermissionHandler.require_tool(name)` at execution time, even though `build_tools(permission_handler)` already filters by `allowed_tools`. Defense in depth — same pattern as kami's `kami_agent/tools.py:428`.

### Permission and failure model (applies to all four tools)

Each tool has three structurally distinct failure paths. They surface to the agent differently:

| Failure | What happens | Why |
|---|---|---|
| **Permission denied** (tool not in `Profile.allowed_tools`) | `build_tools(profile.allowed_tools)` filters the tool out at construction time — the LLM never sees it. The `PermissionHandler.require_tool(name)` runtime check in the tool body is a **bug-tripwire**: if it ever fires (Profile mutated mid-flow), it raises `KitaruRuntimeError` and terminates the flow. | Permission denial leaking to the LLM as a "soft" exit code invites retries, prompt-injection probes, and hallucinated "the tool is broken" responses. Static filtering is the right gate; the runtime check is defense-in-depth, not a graceful fallback. |
| **Sandbox/proxy infrastructure failure** (worker container died, proxy unreachable, network partitioned) | Tool body catches the low-level error and re-raises `KitaruRuntimeError` with a clear cause. Flow terminates; operator sees it in the dashboard and can `kitaru executions retry` after fixing the infrastructure. | Asking the agent to recover from "Docker daemon went away" wastes tokens and pollutes the trace. Infrastructure problems are the operator's job. |
| **Application-level failure** (`grep` matched nothing → exit 1; `curl` returned 404; `pdftotext` complained about a corrupt PDF; mock service returned 500) | Tool returns its normal result type (`ExecResult`, `FetchDocumentResult`, etc.) with the non-zero exit code / 4xx / 5xx surfaced verbatim. | These are signals the agent should reason about — wrapping them as exceptions fights pydantic-ai's tool semantics. |

**Deviation from kami:** `kami_agent/sandbox/manager.py::execute_command` intentionally returns `"[sandbox-error] ..."` strings for both infrastructure and command failures and never raises. The example improves on this by separating the two — infrastructure problems become `KitaruRuntimeError` (operator's job, surfaces in the dashboard with a clear message and a `kitaru executions retry` path), application failures stay as structured tool results (agent's job to reason about). The "never raise from a tool body" pattern made sense for kami because everything in the early prototype was best-effort cloud sandbox work; this example uses kitaru's durable-execution semantics as the recovery layer instead.

### Logging and redaction

Every tool routes its argument and result logging through a shared helper (`agent_factory/tools.py::_log_tool_call` / `_log_tool_result` / `_log_tool_error`) that:

- Redacts dict keys matching the case-insensitive pattern `authorization|token|secret|password|key`. The replacement value is the literal string `"<redacted>"`.
- Truncates string values longer than 500 chars to `"<first 500 chars>… [truncated]"`.

Same defaults as kami's `tools.py`. Tool authors who need to log a secret deliberately bypass the helper — there's no `force=` flag.

The HITL tool integrates with PydanticAI through the kitaru adapter's two-piece seam:

- **`@hitl_tool(...)`** (marker decorator) — registration-time metadata. Read by `PermissionHandler`, capture policy, and the system-prompt builder so the LLM only sees the tool when it's allowed and capture/redaction rules know it's HITL.
- **`wait_for_input(...)`** (adapter-exported helper) — invocation-time pause. Calls `kitaru.wait()` underneath but tags the resulting wait record with `kitaru.adapter.id == "pydantic-ai"` and `source == "tool_body"` so the dashboard, OTel spans, and tool-event children correlate.

The tool exposes a flat parameter shape to the LLM (a `Literal` `kind` + a generic `args: dict`), mirroring kami's `exec_service(service_name, args)` pattern. This avoids `oneOf`/`anyOf` JSON schema fragments that some LLM providers handle inconsistently. The per-kind Pydantic models live in the tool's *description* (built dynamically) and validation happens inside the body.

```python
from typing import Annotated, Any, Literal
from pydantic import BaseModel, Field
from pydantic_ai import RunContext
from kitaru.adapters.pydantic_ai import hitl_tool, wait_for_input
import kitaru

Severity = Literal["sev-1", "sev-2", "sev-3", "n/a"]
QuestionKind = Literal["freeform", "severity_decision"]

class FreeformQuestion(BaseModel):
    question: str

class SeverityDecisionQuestion(BaseModel):
    clause_id: str
    clause_text: str
    pattern: str

ASK_QUESTION_KINDS: dict[str, type[BaseModel]] = {
    "freeform": FreeformQuestion,
    "severity_decision": SeverityDecisionQuestion,
}

@hitl_tool(name="ask_question")
def ask_question(
    ctx: RunContext[ReviewDeps],
    kind: QuestionKind,
    args: dict[str, Any],
) -> str | Severity:
    """Ask the human a question; routed to the right wait shape based on `kind`."""
    if kind == "freeform":
        payload = FreeformQuestion.model_validate(args)
        return wait_for_input(question=payload.question, schema=str)

    if kind == "severity_decision":
        payload = SeverityDecisionQuestion.model_validate(args)
        precedents = kitaru.memory.get("compliance-precedents", default=[])
        suggested = _suggest_from_precedents(payload.pattern, precedents)
        decision: Severity = wait_for_input(
            schema=Severity,
            name=f"severity_decision_{payload.clause_id}",
            question=(
                f"Clause {payload.clause_id}: {payload.clause_text!r}\n"
                f"Pattern: {payload.pattern}\n"
                f"Suggested: {suggested or 'none'}"
            ),
            metadata={
                "clause_id": payload.clause_id,
                "pattern": payload.pattern,
                "suggested": suggested,
            },
        )
        _record_precedent(precedents, payload.pattern, decision)
        kitaru.memory.set("compliance-precedents", precedents)
        return decision

    raise ValueError(f"unknown question kind: {kind!r}")
```

Adding a new HITL shape (e.g. `boolean_gate`) is the same shape as adding a new service to kami: a Pydantic model + an entry in `ASK_QUESTION_KINDS` + a branch + (optionally) a per-kind permission entry on the Profile.

The `CallDeferred` / `ApprovalRequired` / `requires_approval=True` paths exist in pydantic-ai and bridge to `kitaru.wait()` automatically too, but they don't fit this tool's shape: severity decisions need a value back from the human (not a yes/no approval) *and* the memory write-back has to be co-located with the wait so each clause is a self-contained read-ask-record cycle. Raising `CallDeferred` would split that across multiple tools and rely on the LLM to call them in order — an architectural step backwards.

| Tool | Type | What it does | Stage introduced |
|---|---|---|---|
| `exec` | shell | Runs a bash command in the worker container; routes HTTP through proxy. Returns truncated stdout/stderr + an exit code + the path to the full persisted output (see `ExecResult` below). | 1 (in-process), 2 (Docker worker) |
| `skill` | local-fs | `list` / `read` / `search` over `skills/`. Ported verbatim from `kami_agent/tools.py:135`: same actions, same `_resolve_skill_path` `.is_relative_to(skills_root)` escape check, same caps (`MAX_READ_BYTES=100_000`, `MAX_RESULTS_CAP=200`, default glob `**/SKILL.md`). For the local-only example, `LocalSkillSource(path=...)` resolves to a bind-mount; `GitRepoSkillSource` (deferred chapter) clones to a tempdir; both produce a `local_skills_directory` the tool reads from. | 4 |
| `exec_service` | typed-union service dispatcher (host-side) | One tool whose description is rebuilt from `profile.allowed_services_for_tooling()`. LLM sees flat parameters `service_name: Literal[...]` + `args: dict`; the body validates `{service_name, **args}` against the `ServiceCall` discriminated union and runs the handler **in the host process** — the proxy is not involved. Cases: `lookup_precedent` (typed wiki query returning structured precedent snippets), `publish_review` (post the final report to a Discord-shaped webhook). Doc fetching deliberately lives in `exec` (`exec("curl docstore.local/...")`) so the proxy demo covers it. | 4 |
| `ask_question` | typed-union HITL dispatcher | One tool with a dynamically built description listing each `kind` and its embedded JSON schema. LLM sees flat parameters `kind: Literal[...]` + `args: dict`; the body branches per kind, calling the adapter's `wait_for_input(schema=...)` with the right schema. Kinds: `freeform` (string answer) and `severity_decision` (typed `Severity` answer with precedent lookup + memory write-back co-located in the branch). | 4 |

### `exec` output policy

Raw command output can blow the agent's context window in a single call (a `pdftotext` of a 30-page DPA, an unfiltered `curl`). The tool truncates and persists:

```python
class ExecResult(BaseModel):
    exit_code: int
    stdout: str            # truncated; trailing "[N more lines: cat /workspace/.exec/<id>.out]" marker when clipped
    stderr: str            # truncated similarly
    full_output_path: str  # always set; full stdout+stderr written here by the worker
```

Defaults (constants in `agent_factory/sandbox/worker.py`):

| Constant | Default | Purpose |
|---|---|---|
| `MAX_STDOUT_LINES` | 200 | Lines before the "..." truncation marker. |
| `MAX_STDERR_LINES` | 50 | Same, for stderr. |
| `MAX_LINE_BYTES` | 2000 | Per-line cap so a binary blob in stdout doesn't explode the truncated output. |

The agent drills into the full output via subsequent `exec` calls (`head`, `tail`, `grep`, `jq`) against `full_output_path`. This composes with the workspace volume's durability — paused flows resume with the same `/workspace/.exec/<id>.out` files in place.

Auto-summarization via a hidden `kitaru.llm()` call was considered and rejected: a tool that silently spends LLM tokens conflicts with the "primitives first" pitch and corrupts structured output (`jq` results) the agent needs verbatim.

**Per-tool error handling and redaction details: TODO — answered as we walk the remaining four tools.**

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

## 9. Memory & artifacts (in progress)

### Flow-scope precedents

- **Key:** `compliance-precedents`
- **Scope:** flow (cross-execution; `kitaru.memory` defaults to flow scope when accessed from inside the agent loop).
- **Shape:**

  ```python
  class ClausePattern(BaseModel):
      pattern: str          # kebab-case topic slug picked by the LLM (e.g. "subprocessor-non-eea-transfer")
      severity: Severity
      count: int            # how many times this (pattern, severity) was confirmed
      last_seen: datetime
  ```

- **Read by:** `ask_question` tool body, `severity_decision` branch — calls `_suggest_from_precedents(pattern, precedents)` which exact-matches on the slug, ties broken by `(count, last_seen)`.
- **Written by:** same branch, after the operator confirms — calls `_record_precedent(precedents, pattern, severity)` which mutates in place if `(pattern, severity)` exists, else appends.
- **Why slug, not embeddings or verbatim phrase:** verbatim phrase is brittle (tiny rewordings miss, demo never fires precedent suggestion); embeddings are correct but bring in a model dependency, vector storage, and a similarity threshold for chapter 4 to defend. Slug-matching is the right educational midpoint — the system prompt says "use a kebab-case topic slug, 3-5 words" and the agent on run 2 sees existing slugs in the suggestion path so it's biased toward reuse. README's "going further" section points at vector indices for production use.
- **`_suggest_from_precedents` and `_record_precedent`** live in `agent_factory/memory.py` — pure functions, no kitaru imports, easy to unit-test.
- **Memory access point:** the tool body, *not* a `@checkpoint`. Kitaru forbids `kitaru.memory` inside `@checkpoint` — so `load_policy`/`find_clauses` cannot touch precedents, but the agent loop's tool body can. That matches our Q9 architecture exactly.

### Execution-scope findings

- **Key:** `findings`
- **Scope:** execution (per-run; `memory.configure(scope_type="execution")` for this key).
- **Shape:**

  ```python
  class Finding(BaseModel):
      clause_id: str
      text: str
      severity: Severity
      rationale: str
  ```

- Written incrementally inside the agent loop after each `severity_decision`. Read in `stage_5_replay.py` by `print_diff(stage_4_findings, result.findings)`.

### Artifacts

- **`final_review`** — named artifact (`type="output"`), the published Markdown report. `kitaru.save("final_review", report_md, type="output")` from the flow body once the agent's `result.output` is in.
- **`extracted_clauses`** — named artifact (`type="context"`), the `find_clauses` checkpoint's output. Retained for replay (Q1's locked decision means `find_clauses` is cached when only the policy changes).

### Retention

- Precedents memory: kept indefinitely. Real production deployments would compact via `kitaru.memory.compact(...)` after N versions; for the example, compaction is mentioned in README's "going further."
- Findings: per-execution, garbage-collected by kitaru's normal artifact retention.

---

## 10. Sandbox & proxy (TODO — section 7 of brainstorm)

### Worker container

- Base image: `python:3.11-slim` + `bash`, `curl`, `jq`, `poppler-utils` (for `pdftotext`)
- CA cert installed via `update-ca-certificates`
- Runs `tail -f /dev/null` as PID 1 — the host opens a persistent bash via `docker exec -i worker_<id> bash --noprofile --norc` and reuses it across the flow's exec calls (matches kami's persistent-bash pattern from `modal_runtime.py`)
- Env vars set at container start: `http_proxy`, `https_proxy`, `HTTP_PROXY`, `HTTPS_PROXY`, `no_proxy`, `NO_PROXY`, `REQUESTS_CA_BUNDLE`, `SSL_CERT_FILE`, `NODE_EXTRA_CA_CERTS`, `PIP_CERT` — all pointing at `/etc/ssl/certs/ca-certificates.crt`

### `DockerWorker` — port of kami's `ModalSandboxRuntime`

The persistent-shell + marker-based completion pattern from `kami_agent/sandbox/modal_runtime.py` ports near-verbatim. The Modal-specific 30 lines become subprocess calls; the 120 lines of stdout-reader-thread / queue / marker-parsing logic survive unchanged.

```python
# agent_factory/sandbox/worker.py
class DockerWorker:
    """Persistent-shell Docker worker; Docker port of kami's ModalSandboxRuntime."""

    def __init__(self, *, execution_id: str, proxy: DockerProxy) -> None:
        self._execution_id = execution_id
        self._proxy = proxy
        self._container_id: str | None = None
        self._shell_process: subprocess.Popen[bytes] | None = None
        self._shell_lock = threading.Lock()
        self._shell_stdout_queue: queue.Queue[str | None] | None = None
        self._shell_stdout_thread: threading.Thread | None = None

    def __enter__(self) -> "DockerWorker":
        self._start_container()       # docker run -d --network agent_factory + env + volumes
        self._start_shell_process()    # docker exec -i + stdout reader + `exec 2>&1`
        return self

    def __exit__(self, *exc: object) -> None:
        self._terminate_existing_shell()
        self._stop_container()         # docker stop + docker rm

    def run(self, command: str) -> ExecResult:
        # Kami's _build_command_payload → _write_stdin → _read_stdout_until_marker,
        # then truncation per Q7 (MAX_STDOUT_LINES / MAX_STDERR_LINES / MAX_LINE_BYTES)
        # plus full output written to /workspace/.exec/<call_id>.out.
        ...
```

Docker-specific seams:

- Container creation: `subprocess.run(["docker", "run", "-d", "--network", "agent_factory", "-v", f"workspace_{exec_id}:/workspace", "-v", f"agent_factory_certs:/usr/local/share/ca-certificates:ro", *env_args, "agent-factory-worker", "tail", "-f", "/dev/null"])`
- Persistent shell: `subprocess.Popen(["docker", "exec", "-i", container_id, "bash", "--noprofile", "--norc"], stdin=PIPE, stdout=PIPE, stderr=STDOUT, bufsize=0)`
- Cleanup: `docker stop --time=2 worker_<id>` + `docker rm worker_<id>`. Workspace volume retention is configurable (Section 4 — "cleaned up on flow completion (configurable retention)").

What the port does *not* carry over from kami:

- **`SandboxManager` wrapper** — kami's `manager.py` exists only to swap `ModalSandboxRuntime` for tests; the example uses `DockerWorker` directly.
- **Modal idle-timeout (`idle_timeout_seconds=86400`)** — Modal-specific auto-cleanup. Docker lifecycle is bounded by the Q5 context manager (worker dies when the flow exits).
- **`SandboxManager.execute_command`'s "never raise; wrap exceptions as exit_code=1"** — replaced by Q8's hybrid (raise `KitaruRuntimeError` on infrastructure failure, return structured `ExecResult` for application failures).

### Proxy container

- Base image: `mitmproxy/mitmproxy:latest`
- Runs `mitmdump --quiet --listen-host 0.0.0.0 --listen-port 8080 --set confdir=/mitmproxy_certs -s /opt/proxy_addon.py`
- Env: `AGENT_FACTORY_CREDENTIALS={...}`, `AGENT_FACTORY_PROXY_TOKEN=<random-per-run>` (renamed from kami's `KAMI_*` for namespace cleanliness)
- The proxy URL embedded in worker env vars is `http://<token>:@proxy:8080` — Docker network DNS reaches the proxy by container name; no port mapping or tunnel needed (replaces kami's Modal tunnel + `tunnels[8080].tcp_socket` lookup)
- Startup readiness: replaces kami's `time.sleep(2)` with a poll loop (`docker exec proxy nc -z localhost 8080` retried 50× / 100 ms) — same length, faster on the happy path, robust on slow runners

### Proxy port shape

| File | Ported from | Changes |
|---|---|---|
| `agent_factory/sandbox/proxy_addon.py` | `kami_agent/sandbox/proxy_addon.py` (129 LOC) | Verbatim except env var rename `KAMI_CREDENTIALS` → `AGENT_FACTORY_CREDENTIALS`, `KAMI_PROXY_TOKEN` → `AGENT_FACTORY_PROXY_TOKEN`, log prefix `[kami-proxy]` → `[agent-factory-proxy]`. All host-matching, per-connection auth set, `Proxy-Authorization` extraction + pop-after-validation, and `print(..., flush=True)` lines preserved (chapter 3 demos `docker logs proxy` to show injection in action). |
| `agent_factory/sandbox/certs.py` | `kami_agent/sandbox/proxy.py::ensure_certs` + `_generate_ca` (~45 LOC) | Verbatim — same `cryptography` RSA 2048 / 10-year / CN. Files relocated under `agent_factory/sandbox/proxy_certs/` |
| `agent_factory/sandbox/proxy.py` (`DockerProxy` class) | `kami_agent/sandbox/proxy.py::ProxySandbox` | Modal-specific seams (`modal.Sandbox.create`, `modal.tunnels()`, `time.sleep(2)`) replaced with `subprocess.run(["docker", "run", "-d", ...])`, Docker network DNS, and the readiness poll loop. `build_proxy_credential_map`, `_build_authenticated_proxy_url`, `secrets.token_urlsafe(32)` — verbatim. |

A header comment at the top of each ported file points at its kami origin so a reader diffing the two repos can verify parity without having to read kami first.

### Credential resolution: real `kitaru.secrets` even for mocks

Mock services use static tokens but the tokens still flow through the kami credential pattern. There are two paths (matching kami's architecture):

- **Proxy-injected (sandboxed `exec` calls):** `Profile.sandbox_proxy_rules[].headers` hold `{{ <secret-name>.<key> }}` templates; `build_proxy_credential_map(profile)` at flow start resolves them via `KitaruCredentialBroker → kitaru.secrets.get_secret(...)` and injects the resolved map into the proxy container's `AGENT_FACTORY_CREDENTIALS` env var. The worker never sees the resolved values.
- **Host-side (`exec_service` cases):** `Profile.service_configs[name].secret_reference` holds the same template form; the host's service handler resolves it directly via `KitaruCredentialBroker → kitaru.secrets.get_secret(...)` at call time and passes the cred to the HTTP request. The proxy is not involved.

Reader setup (one-time, before stage 3):

```bash
# agent_factory/setup.sh
kitaru secrets create wiki-token     --value=wiki-token
kitaru secrets create policies-token --value=policy-token
kitaru secrets create docstore-token --value=docstore-token
kitaru secrets create discord-token  --value=discord-token
```

Reasons for keeping real secrets even for mocks:

- **Teaches the right thing.** Chapter 3's hook is "the two-process credential isolation pattern." The secret has to *originate* somewhere the reader trusts (kitaru's secret store) and never reach the worker — putting hardcoded tokens in the profile teaches "configure tokens here," which is the wrong takeaway.
- **Swap mocks → real services with zero structural change.** Replacing the wiki mock with a real one is `kitaru secrets create wiki-token --value=<real_token>` plus a host-pattern tweak. Profile shape is identical.
- **Compose-seeded secrets considered and rejected** because they hide the chapter's central teaching artifact: the reader needs to see the secret-creation step explicitly so they know where to configure tokens for their own services later.

Stages 1 and 2 don't reference any secrets, so they run with zero setup.

### Per-run bearer token

Generated at flow start via `secrets.token_urlsafe(32)`, passed to proxy via env, embedded in proxy URL. Proxy addon validates `Proxy-Authorization` header on every request and rejects unauthenticated traffic. This prevents other processes on the host from accidentally using the proxy.

### Marker-based command completion

The same b64 + completion-marker trick kami uses (`tools.py` in kami), allowing the host to read full stdout/stderr without shell buffering issues.

**Exact mitmproxy addon code: port from kami's `proxy_addon.py` with cosmetic adjustments. TODO.**

---

## 11. Mock services

A single FastAPI app (`mocks/server.py`) running on a single port, multiplexed by `Host:` header to virtual hosts on the `agent_factory` Docker network. Reader runs `docker compose logs -f mock-services` for a live tail of which auth headers arrive at which endpoints — chapter 3's central demo.

Two credential paths reach the mocks (matching kami's two-path architecture):

- **Sandboxed + proxy-injected** (chapter 3 hero demo): the agent's `exec("curl <host>/...")` call originates inside the worker, hits the mitmdump proxy, the addon injects `Authorization` based on host match, the request lands at the mock with the right header. **`docstore.local` and `policies.local` use this path** (when fetched ad-hoc by the agent).
- **Host-side typed service** (`exec_service` cases): the host process resolves the credential directly via `kitaru.secrets.get_secret(...)`, the service handler makes the HTTP call from the host. Proxy is not involved. **`wiki.local` (via `lookup_precedent`) and `discord.local` (via `publish_review`) use this path.**

| Host | Endpoint | Required Authorization header | Consumer | Response |
|---|---|---|---|---|
| `wiki.local` | `GET /precedents/{topic}` | `Bearer wiki-token` | `exec_service("lookup_precedent", ...)` (host-side) | `{ "topic": str, "snippets": [{ "url": str, "excerpt": str }] }` — max 5, fixtures in `mocks/fixtures.py::WIKI_PRECEDENTS` |
| `policies.local` | `GET /policy/{name}` | `Bearer policy-token` | `load_policy` checkpoint (host-side) and ad-hoc agent `exec("curl ...")` (proxy-injected) | `text/markdown` body, served from `fixtures/policy_v1.md` (default) or `fixtures/policy_strict.md` (replay) |
| `docstore.local` | `GET /docs/{doc_id}` | `Bearer docstore-token` | Agent `exec("curl docstore.local/docs/<id>")` (proxy-injected) | `application/pdf` body served from `fixtures/docs/{doc_id}.pdf`, or `{ "error": "not_found" }` with HTTP 404 |
| `discord.local` | `POST /webhooks/{id}` | `Bot discord-token` | `exec_service("publish_review", ...)` (host-side) | HTTP 204; payload logged to stdout in the format `[mock-discord] webhook=<id> payload=<json>` so chapter 4's screenshot can show the published review arriving |

**Auth model:** every endpoint reads the inbound `Authorization` header. If missing or wrong, returns HTTP 401 with `{ "error": "unauthorized", "expected_prefix": "<Bearer|Bot>" }`. The agent's first `exec("curl docstore.local/...")` without proxy injection (chapter 2) gets a 401 — the agent has nothing useful to do. Chapter 3 enables `sandbox_proxy_rules`, the proxy injects, the next call returns 200. That's the visible chapter 3 payoff. (Host-side service calls always have credentials and never fail with 401 — they demo a different pattern in chapter 4.)

**Fixtures (`mocks/fixtures.py`):**

- `WIKI_PRECEDENTS: dict[str, list[dict]]` — keyed by topic slug, ~5 entries covering the most common compliance topics (`subprocessor-non-eea-transfer`, `data-retention-period`, `liability-cap`, `breach-notification`, `audit-rights`).
- `DOCSTORE_DOCS: dict[str, Path]` — keyed by doc_id, points at PDFs in `fixtures/docs/`.
- One fixture PDF: `fixtures/docs/sample_dpa.pdf` — a synthetic Data Processing Agreement (~30 pages). `pdftotext` extracts ~12 clauses; the agent reviews each.

**Logging:** every mock endpoint logs `[mock-<service>] <method> <path> auth=<redacted_first_8_chars> status=<status>` so the chapter 3 screenshot can show the full request flow without leaking full tokens.

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

## 13. Blog series chapter map

Chapters and stage files are numbered independently — chapter count is set by what's *consumable in one sitting*, not by the stage file count. Stage 4 alone supports three chapters (skills+services / HITL / memory) since each focuses on a different slice of the same `stage_4_full_agent_factory.py`. Length target: tight (~1500–2000 words per chapter, one hero screenshot, runnable code).

| Ch | Title | Stage | Key concepts | Hero artifact |
|---|---|---|---|---|
| 1 | Build a durable agent on kitaru in 60 lines | `stage_1_basic_agent.py` | `@flow`, `KitaruAgent`, `Profile`, in-process `exec` tool, `PermissionHandler` | The full Python file fits on one screen; dashboard run-detail view |
| 2 | Sandboxing your agent's shell | `stage_2_sandboxed_exec.py` | `DockerWorker`, persistent-bash + marker pattern, workspace volumes, `ExecResult` truncation, the `/workspace/.exec/<id>.out` durability trick | `docker ps` showing `worker_<exec_id>`; `docker exec` into the running worker mid-flow |
| 3 | The two-process credential isolation pattern | `stage_3_credential_proxy.py` | `DockerProxy`, mitmproxy addon, `sandbox_proxy_rules`, `kitaru.secrets` template resolution, per-run bearer token | `docker logs proxy` tail showing `[agent-factory-proxy] injected headers for wiki.local: ['Authorization']`; side-by-side: worker can't read `AGENT_FACTORY_CREDENTIALS`, proxy can |
| 4 | The agent's playbook: skills + typed services | `stage_4_full_agent_factory.py` (slice 1) | `skill` tool, `exec_service` typed-union dispatcher (cases: `lookup_precedent`, `publish_review`), profile-derived dynamic tool descriptions, the discriminated-`ServiceCall` pattern, the *two credential paths* distinction (sandboxed `exec` vs. host-side `exec_service`) | The SKILL.md procedure side-by-side with the Python file that uses it; the dynamically-generated `exec_service` description shown in the dashboard; `[mock-discord] webhook=... payload=...` log line showing the published review arrive host-side |
| 5 | Stateful HITL with `ask_question` | `stage_4_full_agent_factory.py` (slice 2) | `ask_question` typed-union HITL dispatcher, `wait_for_input`, the `freeform` kind, the kitaru/pydantic-ai integration seam (`@hitl_tool` + `wait_for_input`) | A flow that pauses; `kitaru executions list` showing `waiting`; the dashboard's wait-resume UI |
| 6 | Cross-execution learning: precedent memory | `stage_4_full_agent_factory.py` (slice 3) | `severity_decision` kind, slug-keyed precedents, `kitaru.memory` flow scope, the read-ask-record loop co-located in the tool body | Run 1 vs run 2 side-by-side: same clause, run 2 shows "Suggested: sev-2" pre-filled from precedent memory |
| 7 | When durability pays off: replay with overrides | `stage_5_replay.py` | `flow.replay()`, `from_=`, `overrides={"checkpoint.load_policy": ...}`, what gets cached vs re-executed, named artifacts as the diff surface | Diff between `findings_v1` and `findings_v2`; dashboard view showing `find_clauses` cached and `check_clause_2..N` re-executed |
| post | Going to production: from Docker-compose to Modal | (no new stage) | C2/C3 ladder, lazy sandbox startup (deferred from Q5), Modal port, deploying the same architecture remotely | Same agent, same profile, swap `DockerWorker` for `ModalSandboxRuntime` |

**Self-containment rule:** every chapter opens with a 1-paragraph recap of the prior chapter's setup (or "if you're starting here, run `docker compose up && bash setup.sh && python stage_<N-1>_<...>.py` to get to this point"), so a reader landing from search can run the chapter's stage without reading the others.

Chapter 1 and chapter 7 form the natural framing — "build a durable agent" → "what durability earns you." Chapters 4–6 are the body and can be sequenced flexibly if a reader cares more about HITL than skills, etc.

**Chapter outlines, opening hooks, code excerpts: separate writing pass after implementation lands.**

---

## 14. Local development (TODO — section 7 of brainstorm)

```bash
cd examples/end_to_end/agent_factory
uv sync --extra local --extra pydantic-ai --extra agent-factory   # new extra
docker compose up -d                                              # worker, proxy, mock-services
python stage_1_basic_agent.py                                     # Stage 1 needs no secrets
python stage_2_sandboxed_exec.py                                  # Stage 2 needs no secrets
bash setup.sh                                                     # Stage 3+ — creates kitaru secrets for the mocks
python stage_3_credential_proxy.py                                # Now the proxy can inject auth
```

Cleanup:

```bash
docker compose down -v   # drops named volumes, including workspace_<exec_id>
```

### `agent-factory` extra (added to root `pyproject.toml`)

```toml
[project.optional-dependencies]
agent-factory = [
    "mitmproxy>=10.0",            # proxy addon runtime
    "cryptography>=42.0",         # CA generation in certs.py
    "fastapi>=0.110",             # mock-services container
    "uvicorn>=0.30",              # mock-services entrypoint
    "pypdf>=4.0",                 # for fixtures/docs/sample_dpa.pdf parsing in tests
]
```

The `docker` Python SDK is *not* a dependency — `DockerWorker` and `DockerProxy` shell out via `subprocess.run(["docker", ...])` because (a) it matches what readers will inspect themselves with `docker ps`/`docker logs`, (b) it avoids a Python-API/CLI version drift surface, and (c) blog screenshots of the actual `docker` commands are clearer teaching artifacts than `containers.run(...)` calls.

### CI strategy

- The existing `ci.yml` runner is `ubuntu-latest`, which has Docker pre-installed.
- `tests/test_proxy_injection.py` and `tests/test_full_loop.py` need `docker compose up` against the example's compose file. Add a step to the CI `pytest` matrix: `docker compose -f examples/end_to_end/agent_factory/docker-compose.yml up -d --wait` before the example's pytest invocation, `down -v` after.
- `tests/test_stage_1.py` is a stage-1 smoke test — no Docker required. Stage 1 is in-process exec.
- The `agent-factory` extra is opted into a dedicated CI job (similar to the existing `kitaru[mcp]` test lane mentioned in CLAUDE.md), not the default `tests` job, so the Docker dependency doesn't slow down the base lane.

### Naming

Branch `example/agent-factory` and directory `examples/end_to_end/agent_factory/` are the chosen names. Confirmed against the existing `coding_agent`, `news_scout`, and `compliance_review` siblings — same naming convention. No external review needed; this is a regular example PR.

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
- **Section 6 — Memory & artifacts.** Resolved: slug-keyed `ClausePattern`, exact-match `_suggest_from_precedents`, flow-scope precedents + execution-scope findings, memory access lives in the `severity_decision` tool body (not in checkpoints).
- **Section 7 — Sandbox/proxy implementation.** Resolved: persistent-shell port (Q12); proxy port verbatim with namespace rename and readiness poll (Q13); mock services as FastAPI multiplexed by Host header with explicit fixtures and visible auth-failure logging.
- **Section 8 — Blog series.** Resolved: 7 main chapters + 1 post-chapter; chapter count is independent of stage-file count (stage 4 supports chapters 4, 5, 6 — different slices of the same file). Tight target (~1500–2000 words/chapter). Chapter outlines, hooks, code excerpts: separate writing pass after implementation lands.
- **Tool descriptions in profile system prompt.** Should the system prompt enumerate available skills, or rely on the agent calling `skill list` first? *(Defer to implementation; default is "agent calls `skill list` first" — kami pattern.)*
- **CI strategy.** Resolved (Section 14): `ubuntu-latest` runners have Docker; `agent-factory` gets its own test lane similar to `kitaru[mcp]`; `test_proxy_injection.py` + `test_full_loop.py` run against `docker compose up --wait`; `test_stage_1.py` is in-process with no Docker dependency.
- **Naming check.** Resolved (Section 14): `example/agent-factory` branch and `examples/end_to_end/agent_factory/` directory follow existing sibling conventions.

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
