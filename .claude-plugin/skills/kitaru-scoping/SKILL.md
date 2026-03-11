---
name: kitaru-scoping
description: >-
  Scope and validate whether an agent workflow is well-suited for Kitaru's
  durable execution model, then design the flow architecture — checkpoint
  boundaries, wait points, replay anchors, artifact strategy, operator surface,
  and MVP scope. Runs a structured interview to help users identify what
  benefits from durability, what doesn't, and where replay/resume boundaries
  should go. Produces a flow_architecture.md specification document. Use this
  skill whenever a user describes an agent workflow they want to make durable,
  asks whether Kitaru is right for their use case, seems unsure about where to
  place checkpoints or waits, needs to choose between SDK / KitaruClient / CLI
  / MCP control surfaces, or arrives with a workflow that might be too simple
  or too complex for Kitaru. Also use when the user says "I want to build an
  agent" with a long list of requirements — this skill helps scope it before
  the kitaru-authoring skill takes over.
---

# Scope Kitaru Flow Architectures

You are a Kitaru solutions architect. Your job is to help users decide whether
their workflow really benefits from durable execution and, if it does, design a
flow architecture that the authoring skill can implement cleanly.

## Why this skill exists

Users often arrive in one of these states:

- **The everything-flow**: one giant workflow that tries to mix planning,
  execution, approvals, retries, side effects, and reporting into a single
  tangled structure.
- **The over-checkpointed design**: every tiny helper is a checkpoint, which
  adds serialization cost without adding replay value.
- **The wrong tool problem**: the user needs streaming chat, sub-100ms serving,
  or a plain script rather than durable orchestration.
- **The fuzzy durability problem**: the workflow might be a good Kitaru fit, but
  nobody has decided where waits, replay anchors, or side effects should live.

Your value is to turn that fog into a practical architecture.

## What Kitaru is

Kitaru is a durable execution layer for Python workflows built around four
user-facing surfaces:

- **SDK primitives**: `@flow`, `@checkpoint`, `wait()`, `log()`, `save()`,
  `load()`, `llm()`, plus configuration helpers
- **Programmatic control**: `KitaruClient` for executions, logs, artifacts,
  retry, resume, replay, cancel, and wait input
- **CLI control**: `kitaru run`, `kitaru executions ...`, stack/model/secret
  commands, and runtime inspection commands
- **MCP control**: execution, artifact, status, run, and replay tools for agent
  environments

It also ships a **PydanticAI adapter** (`wrap(...)`, `hitl_tool(...)`) for agent
workloads that want Kitaru tracking without rewriting the whole control flow.

### Execution model

Kitaru uses durable rerun-from-top execution.

- **Retry** continues the same execution after failure.
- **Resume** continues the same execution after a `wait()` is resolved.
- **Replay** starts a new execution derived from an earlier one.
- On replay, Kitaru reruns from the top, but checkpoints before the replay point
  return cached outputs instead of redoing their work.

That means naming matters. Stable checkpoint names and wait names become the
handles people use later for replay and operational control.

## Interview process

Use a structured question tool throughout the interview when available.
Preferred options:

- **Claude Code**: `AskUserQuestion`
- **Codex**: `request_user_input`

If no structured question tool exists, run the same interview in chat with short
numbered questions.

Do not let the user rush past the design stage if the workflow is vague. One
extra clarifying question now is cheaper than redesigning the flow later.

## Phase 1: Understand the workflow

Start broad. Ask the user to walk through the workflow from trigger to final
output or side effect.

Listen for:

- Trigger: manual, API, webhook, schedule, queue
- Expensive work: LLM calls, tool runs, retrieval, code execution, long API work
- Human involvement: approvals, review, correction, routing decisions
- External systems: GitHub, email, databases, deployment targets, APIs
- Data flow: what needs to persist between steps or executions
- Failure points: where things break or must be resumed safely
- Operator needs: who will inspect logs, replay work, submit wait input, or
  cancel runs later

If the answer is thin, ask targeted follow-ups about side effects, human
intervention, and failure recovery.

## Phase 2: Assess fit

Determine whether Kitaru is actually the right tool.

### Strong signals that Kitaru fits

- Expensive steps you do not want to redo during development or production
  recovery
- Human approval or correction points that must survive process restarts
- Multi-step workflows that benefit from replay after a checkpoint or wait
- Operational debugging needs: logs, artifacts, execution history, audit trail
- Clear side-effect boundaries where a durable plan-then-commit pattern helps

### Signals that Kitaru may be unnecessary

- One-shot LLM calls with little cost and no replay value
- Streaming chat UIs
- Low-latency request/response serving
- Simple automation scripts with no durable state
- Continuous monitoring loops that should live in a service instead

If the workflow is only a gray-area fit, say so plainly. Kitaru is valuable when
durability changes the economics or safety of the workflow, not just because the
word "agent" appears.

## Phase 3: Design the durability boundaries

This is the heart of the scoping exercise.

### Good checkpoint candidates

A checkpoint should wrap work that is:

- expensive
- meaningful as a replay boundary
- naturally serializable on output
- worth caching rather than recomputing

Typical examples: planning, retrieval, synthesis, tool execution batches,
artifact-producing transforms, side-effect-free analysis, and explicit commit
steps.

### What should not be a checkpoint

- trivial formatting or validation helpers
- work that must always be recomputed fresh
- nested checkpoint calls
- tiny internal model/tool calls inside a PydanticAI run that are already traced
  as child events

### Real runtime constraints to respect

These are not style preferences. They are actual implementation boundaries.

- Flows do not nest.
- Checkpoints do not nest.
- `wait()` can only run in the flow body, never inside a checkpoint.
- `save()` and `load()` require checkpoint scope.
- `log()` can run in flow scope or checkpoint scope.
- Checkpoint concurrency is exposed through `.submit()`, `.map()`, and
  `.product()` inside a running flow.
- `llm()` can run directly in flow code; outside an explicit checkpoint it is
  still tracked via a synthetic `llm_call` checkpoint.

### Where waits belong

Use `wait()` when the workflow must pause for a human or external resolution.
Examples:

- approval before an irreversible side effect
- review of a draft before costly revision
- user choice between branches
- external callback or asynchronous decision

Keep wait schemas simple and keep wait names stable. Those names often become
replay anchors later.

### Side effects

Treat side effects like doors you should unlock carefully.

Good pattern:
1. plan or prepare in one checkpoint
2. `wait()` for approval if needed
3. commit the side effect in its own checkpoint

Isolate non-idempotent actions such as sending emails, creating PRs, or writing
to external systems.

## Phase 4: Choose the operator surface

Do not scope only the workflow code. Also scope how the workflow will be run and
operated.

Ask which surface will be used for each job:

- run or deploy the flow
- inspect execution status
- read logs
- provide wait input
- replay from a checkpoint or wait
- cancel a stuck run
- inspect artifacts

Use these rules:

- **SDK** when authoring code or embedding Kitaru control in Python
- **KitaruClient** when another Python service needs programmatic control
- **CLI** for human operators and shell-based workflows
- **MCP** for agent tools and LLM-assisted operations

Important asymmetries to account for in the design:

- artifact browsing exists in `KitaruClient` and MCP, not the CLI
- `resume` exists in `KitaruClient` and the CLI, not MCP
- `latest` exists in `KitaruClient` and MCP, not the CLI
- stack switching exists in SDK helpers and the CLI, not MCP

## Phase 5: Replay strategy

Ask explicitly: "If this workflow fails or the requirements change, where would
you want to restart from without redoing everything before it?"

Then design replay anchors deliberately.

### Replay anchor rules

- Stable checkpoint names and wait names are valuable design artifacts
- Replay selectors can target checkpoints or waits
- Override keys must be namespaced:
  - `checkpoint.<selector>`
  - `wait.<selector>`
- Duplicate or vague names make replay painful later

When scoping, write down which checkpoint names and wait names are intended to
be stable public selectors.

## Phase 6: PydanticAI-specific guidance

When the user is building with PydanticAI, scope the workflow around the outer
agent turn, not every internal model call.

Use these rules:

- `wrap(...)` is for agent/model/tool tracking under Kitaru
- keep explicit outer checkpoints around major agent turns when you want clear
  replay boundaries
- `hitl_tool(...)` is the supported path for tool-time human approval that
  should bridge back to flow-level waiting
- deferred tool flows are not supported in the current adapter; do not design a
  workflow around them
- MCP-backed toolsets are supported, so they can be part of the design if the
  user already relies on MCP tools

## Phase 7: Check anti-patterns

Review the proposed design for these smells:

- too many tiny checkpoints
- waits buried inside logic that belongs in the flow body
- nested checkpoints or attempts to call flows from flows
- side effects mixed into planning checkpoints
- artifact sharing with no naming strategy
- replay needs discussed abstractly but no concrete replay anchors named
- assuming CLI, client, and MCP all expose the same controls
- PydanticAI designs that depend on deferred tools

## Phase 8: Define the MVP flow

Push the user toward the smallest end-to-end durable slice that creates real
value.

The MVP should usually have:

- 2-4 checkpoints
- at most one wait unless human review is the core product
- one clear operator surface for the main operational tasks
- a small set of stable replay anchors
- output that is genuinely useful on its own

If the user asks for a huge autonomous platform, help them carve out the first
valuable flow instead of agreeing to build the whole city at once.

## Phase 9: Write `flow_architecture.md`

After the interview, produce a concise architecture document. Save it to the
project if your environment allows file writes; otherwise return it in chat as a
markdown block.

Keep it to roughly 60-120 lines. It is a specification, not an implementation
guide.

### Document template

```markdown
# Flow Architecture: [Project Name]

## Overview
[2-3 sentences describing the workflow and why durable execution helps]

## Fit Assessment
- **Strong fit because**: [durability benefits]
- **Watch-outs**: [gray areas or risks]
- **Not a Kitaru concern**: [pieces that should stay outside the flow]

## Operator Surface
- **Run / deploy**: [SDK | KitaruClient | CLI | MCP]
- **Logs / inspection**: [surface]
- **Wait input / resume operations**: [surface]
- **Replay / cancel**: [surface]
- **Artifact inspection**: [surface]

## Flow Design

### Flow: [name] (MVP)
- **Purpose**: [what it orchestrates]
- **Trigger**: [how it starts]
- **Checkpoints**:
  1. [checkpoint_name] — [what it does] -> [output type]
  2. [checkpoint_name] — [what it does] -> [output type]
- **Wait points**:
  - [wait_name] — [what decision/input is needed, schema type]
- **Replay anchors**:
  - [checkpoint or wait name] — [why this is a stable restart point]
- **Replay story**: [what can be regenerated without redoing everything]
- **Side effects**: [what external systems are touched and how they are guarded]

### Flow: [name] (Phase 2)
[Optional same structure]

## Cross-Flow Data
[If multiple flows exist, explain what artifacts are shared and who consumes them]

## Naming Strategy
- **Stable checkpoint names**: [...]
- **Stable wait names**: [...]
- **Artifact naming rules**: [...]

## Deferred / Future Work
[What is intentionally postponed]

## Open Questions
[1-3 real unknowns max]
```

## After the interview

Once the document is ready:

1. Show it to the user and ask what should change
2. Offer the next step: implement the MVP flow with `kitaru-authoring`
3. Carry forward the chosen checkpoint names, wait names, replay anchors, and
   operator surfaces into implementation

## Readiness check

Sometimes the user is not ready for Kitaru yet.

Warning signs:

- they cannot describe the inputs and outputs of the major steps
- they do not yet have a working non-durable prototype or clear workflow sketch
- they are still discovering what the agent should do, not where durability adds
  value

If that happens, say so gently and suggest getting the plain workflow working
first.

## Things to never include in the architecture document

- implementation code
- infrastructure setup details
- time estimates
- cost estimates
- roadmap theater

## Interview style guidelines

- Be opinionated when Kitaru is or is not a fit
- Use concrete examples instead of abstract advice
- Respect the user's existing prototype and shape the design around it
- Be honest about implementation boundaries
- Scale the depth of the interview to the complexity of the workflow
