# 1. Overview

**Kitaru** is ZenML's durable execution layer for Python agent workflows.

It lets you write normal Python orchestration, while making the important boundaries durable:

- **flows** define durable executions
- **checkpoints** persist replayable outcomes
- **waits** suspend and later resume execution (same execution continues)
- **LLM calls** can be tracked with minimal ceremony
- **artifacts and metadata** make replay, debugging, and dashboard rendering possible

Kitaru is built on **ZenML** — the hard durability machinery (retry, resume, replay, snapshots, divergence detection) is implemented in the ZenML backend. Kitaru provides a simpler developer-facing model on top:

- plain Python control flow
- explicit durable boundaries
- no graph DSL
- sync-first by default
- zero-config local development
- one-line path to connected/server-backed execution

**OSS vs Pro:** The semantic contracts are stable across both paths. The polished dashboard-triggered experience (resume after compute release, checkpoint visualization, snapshot execution) depends on Pro-backed server capabilities. Local-first OSS versions of these features exist but are more manual. See chapter 3 for details.

## What Kitaru is

Kitaru is a **durable orchestration runtime for Python agent workflows**.

It is designed for workflows that are:

- multi-step
- expensive
- long-running
- human-in-the-loop
- replayed locally during development
- audited later in a dashboard

Examples include:

- research and writing agents
- coding agents
- review-and-approve pipelines
- tool-heavy agents that need reproducibility
- workflows that pause for human or external input

## What Kitaru is not

Kitaru is **not**:

- exact Python stack or frame snapshotting
- a graph DSL
- a streaming chat runtime
- a low-latency request/response serving layer
- a guarantee that arbitrary external side effects are automatically idempotent

The execution model is durable **rerun-from-top**, not in-memory continuation.

## Three core operations

Kitaru's execution model distinguishes three operations:

- **Retry** — same execution recovers from failure (fixed code/config, no user overrides)
- **Resume** — same execution continues after `wait()` input arrives
- **Replay** — new execution derived from a previous one, optionally with changed code/config/inputs/overrides

Only replay creates a new execution. Retry and resume continue the same logical execution.

## Core philosophy

- **Primitives first, frameworks second.** Use a framework when it helps, but the durable model should not depend on one.
- **Python control flow for logic.** Use `if`, `for`, `while`, and `try/except` naturally.
- **Explicit durable boundaries.** Flows, checkpoints, waits, and artifacts make replay possible.
- **Sync-first.** Async may be supported where practical, but sync is the primary path.
- **Infrastructure separate from application logic.** Connection, stack selection, and model config are separate concerns.
