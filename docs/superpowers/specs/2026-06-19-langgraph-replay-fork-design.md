# LangGraph replay & fork from trace — design

**Date:** 2026-06-19
**Branch:** feat/replay-verify-reference-agent
**Scope:** SDK/backend. Reconstruct a node+call-level DAG from a recorded LangGraph trace, replay it deterministically, fork it with edits, and measure **reproduction drift** and **fork drift** for a single case end-to-end. LangGraph-only implementation, structured behind a `seed / checkpoints / fork / capabilities` adapter boundary. Cohort / experiment / iteration are explicitly deferred.

This is the v1 "spine" for the PRD [Replay & fork agent runs to test changes before shipping](https://app.notion.com/p/384f8dff2538815cb53ddc7c1db8207e). It delivers PRD tickets **1 Case · 2 Trace loader · 3 Adapter binding · 4 Replay · 5 Edit model · 6 Fork · 7 Comparison**; it defers **8 Cohort · 9 Experiment · 10 Iteration**.

## Problem

The PRD sells one unified experience: *import a recorded run → replay from a chosen point → fork one call → run forward (cached-before / live-after) → compare*. Today the repo has two partial, incompatible tracks:

- **Native checkpoint replay** (`src/kitaru/replay.py` + ZenML `Pipeline.replay`) — mature and public, but only works on a **Kitaru-native execution** (a `@flow` recorded as a ZenML `PipelineRunResponse`). It resolves a single `from_` checkpoint, walks the DAG (`upstream_steps` / `inputs_v2`), and computes `steps_to_skip`. This is exactly the PRD's `at=` semantics, plus `checkpoint.*` output overrides.
- **Imported-trace verify** (PR #412, `src/kitaru/_replay_verify_imported_*`) — imports external Langfuse traces into `ImportedReplayCase`, validates fidelity + safety, then **freshly re-executes** a baseline vs candidate across a cohort and emits ship/caution/hold verdicts. By its own contract it does **not** checkpoint-replay (`imported_input_fresh_execution_not_deterministic_checkpoint_replay`).

The gap: an external trace is not a Kitaru DAG, so there are no intermediate checkpoints to serve from cache — you can re-run the whole thing, but you cannot cut it mid-run or fork a single call. The headline UX is physically impossible on imported traces *unless* we reconstruct a provenant DAG from them.

## Decisions (from brainstorming)

- **Substrate:** reconstruct a full node+call DAG from the LangGraph agent + a recorded Langfuse trace, so an imported run becomes checkpoint-forkable (the deep "instrument-to-fork" option, LangGraph-first).
- **Granularity:** **node + call level**. Each graph node *and* each LLM/tool call is a Kitaru checkpoint. The runtime-variable tool-call fan-out inside `collect_evidence_with_tools` is wired with ZenML dynamic-pipeline **`.chunk(index=…)`** (not `.load()`), so each recorded call is a lineage-tracked step invocation. This is the provenance requirement that makes `replay.py`'s skip-set logic apply.
- **Replay semantics:** **cached-upstream / exec-tail**. Recorded calls are served from a trace-keyed cache, so replay is deterministic by construction. Live re-execution happens only at/after the cut.
- **Two drifts:**
  - **Reproduction drift** = re-execute the tail **live with no edits**, compared to the original trace → isolates LLM nondeterminism. (Serving cache instead ⇒ ≈ 0, which validates the reconstruction.)
  - **Fork drift** = re-execute the tail **with edits**, compared to the **no-edit replay** (not the trace) → isolates the edit's effect. Both forks share the same cached head.
- **Edit scope:** **both** whole-run variant edits (the existing `variants/*.yaml`: model, prompt_profile, tool_policy, max_tool_calls) **and** per-checkpoint edits (swap model on one node, override a single call's args/result). Precedence: `call > variant/global > recorded`.
- **MVP boundary:** single-case spine only (reconstruct → replay → fork → reproduction-drift + fork-drift). Cohort / experiment / iteration deferred to later pushes.
- **Generality:** LangGraph-only implementation, but structured behind a `seed / checkpoints / fork / capabilities` boundary so other adapters can slot in later. No second adapter built now.
- **Architecture:** **Approach 1 — generated mirror-pipeline + native ZenML replay** (below). The rejected alternative was an in-adapter bespoke replay executor, which forks replay logic away from `replay.py` and forfeits ZenML provenance.

## Entry contract

Deterministic node+call replay needs **both** the trace (cached call outputs + seed inputs) **and** the adapter-bound agent code (to execute the live tail). A trace alone, with no bound graph, can only do scan / fresh-reexec — not cut-and-fork.

```python
from kitaru.adapters.langgraph.replay import KitaruReplayAgent

agent = KitaruReplayAgent(graph)                      # bind the compiled LangGraph StateGraph
case  = agent.import_trace("langfuse:<trace_id>")     # → Case (reuses PR412 importer)
seed  = agent.reconstruct(case)                       # provenant Kitaru run; trace played from cache

# reproduction probe — live tail, no edits
repro = agent.replay(seed, from_="collect_evidence_with_tools")
repro_drift = repro.diff(case)                         # trace vs replay

# fork — same cut, with edits
fork = agent.fork(
    seed,
    from_="collect_evidence_with_tools",
    edits=[edit("model_call:decide_action", model="gpt-5-nano")],   # per-call
    variant="nano_trimmed_permissions",                              # whole-run
)
fork_drift = fork.diff(repro)                          # replay vs fork
```

Exact public names are illustrative and may be refined during planning; the boundary (`seed / checkpoints / fork / capabilities`) is binding.

## Architecture — Approach 1: generated mirror-pipeline + native ZenML replay

Reconstruction compiles `bound graph + trace` into a **provenant ZenML dynamic pipeline** that mirrors the graph. The first execution is a **seed/import run** that plays the trace back: every checkpoint returns its recorded output from a trace-keyed cache, producing a faithful native Kitaru `PipelineRunResponse` with full lineage and **zero live model calls**. Everything after that reuses the existing `client.executions.replay(from_=, overrides=)` machinery.

```
bind(graph)
   └─ import_trace(langfuse)  ──► Case (root_input, recorded_calls keyed by (node, call_index))
        └─ reconstruct(case)  ──► generated dynamic @flow mirroring the graph
              • node checkpoints: receive_request, collect_evidence_with_tools,
                summarize_evidence, decide_action, final_response
              • inside collect_evidence: .chunk(index=i) over recorded tool calls
                → one lineage-tracked checkpoint per tool/LLM call
              • each checkpoint body = cached-or-live (see below)
              ──► seed run: cached playback, native Kitaru execution, full provenance
                    ├─ replay(from_=cut)              → reproduction probe (live tail, no edits)
                    └─ replay(from_=cut, overrides=)  → fork (live tail, with edits)
                          └─ diff → drift report
```

New code is concentrated in: the **compiler**, the **cached-or-live checkpoint body**, the **`skip=` selector**, and the **drift diff**. `replay.py`, ZenML provenance/replay, and PR412's importer + `FieldComparison` are reused.

## Components

All LangGraph-specific code sits behind a `seed / checkpoints / fork / capabilities`-shaped boundary (a Protocol the adapter satisfies), so a future adapter implements the same contract.

1. **Trace importer** — extend PR #412's `cases_from_langfuse_observations` so `recorded_calls` are keyed by `(node, call_index)`, using the LangGraph callback's node tagging on each Langfuse observation. Produces the canonical `Case`. Input contract is the **rich per-observation Langfuse rows** (one row per span/observation), not the flattened `langfuse_export.jsonl`, because call-level reconstruction needs per-call I/O.

2. **DAG compiler** *(new — the heart of the work)* — introspect the compiled `StateGraph` (nodes/edges via `graph.get_graph()`) and emit a dynamic ZenML pipeline mirroring it. Fixed nodes become static checkpoint invocations; the variable tool-call fan-out inside `collect_evidence_with_tools` is wired with `.chunk(index=i)` per recorded call so provenance is preserved per invocation. The compiler binds each checkpoint to the corresponding real node/call callable from the bound graph.

3. **Cached-or-live checkpoint body** — given the current cut + resolved edits, each checkpoint either:
   - returns its **recorded output** from the trace-keyed cache (it is upstream of the cut and unedited), or
   - **executes live** by invoking the real node/call (it is at/after the cut, or an edit targets it or its upstream).
   Edits resolved with precedence `call > variant/global > recorded`.

4. **Replay / fork** — reuse `client.executions.replay(from_=, overrides=)`; **add a `skip=` selector** that freezes an explicit list of checkpoints by invocation id (the PRD's `skip=`, complementary to `from_`/`at=`). Fork = replay-from-cut **with** edits applied. `skip=` and `from_` are mutually exclusive selectors for the cut.

5. **Drift diff** — reuse/extend PR #412's `FieldComparison` + verdict model. Two report sections: reproduction drift and fork drift. Comparison metric = **semantic fields** (`policy_label`, `risk_status`, `required_action`, `tool_names`, `evidence_ids`, tool-call sequence), ignoring free-text `summary` wording.

6. **Capabilities** — `Caps(fork_granularity="call", native_checkpoints="reconstructed", resume="reconstruct")`, so the SDK (and later the UI) can report what a given binding actually supports rather than silently doing the wrong thing.

## Data shapes

- **Case** — extends PR #412's `ImportedReplayCase`: `case_id`, `source_ref`, `root_input`, `observed_output`, `recorded_calls: list[RecordedCall]` (now keyed by `(node, call_index)`), `trace_contract`, `runner_contract`, `labels`. The canonical object that both a Kitaru-native run and an imported trace resolve to.
- **RecordedCall** — `kind` (`llm | tool | retrieval | …`), `name`, `node`, `call_index`, `input_payload`, `output_payload`, `model`, `usage`, `metadata`, `observation_id`. Keyed so the compiler can attach each to the right checkpoint.
- **DriftReport** — `{ reproduction: list[FieldComparison], fork: list[FieldComparison], verdict }`.

## Drift semantics (precise)

- **reproduction drift** = `compare(trace, replay_no_edit)` where the tail re-executes **live** with no edits. With `from_=START` and cache off, this exercises full live re-exec → measures end-to-end nondeterminism; with a later cut, only the tail. Serving cache instead ⇒ ≈ 0, validating the reconstruction.
- **fork drift** = `compare(replay_no_edit, fork_with_edits)` — both from the same cut, sharing the cached head → isolates the edit's effect, free of nondeterminism noise from the head.
- Both use the semantic-field comparator (decision fields + tool-call sequence), not byte equality.

## Testing

- **Deterministic fake model** (record/replay) so the spine runs in CI without API keys; live runs are an opt-in path.
- **Fixtures** = the reference agent (`examples/end_to_end/replay_verify_reference_agent/`) and its `variants/*.yaml`.
- **Assertions:**
  - Reconstruction faithful: seed run reproduces the trace's node/call outputs exactly under cache-serving.
  - Reproduction drift ≈ 0 when the tail is served from cache; bounded/explainable when re-executed live under the fake model.
  - **Fork drift surfaces the planted regression**: the `nano_trimmed_permissions` variant drives permission-scope drift on the permission cases (mirroring PR #412's calibrated cohort, at single-case granularity).
  - `skip=` freezes exactly the named invocation ids; provenance edges from the compiler match `replay.py`'s expectations (skip-set computed correctly).

## Out of scope (deferred to PRD tickets 8–10)

- Cohort grouping and aggregate drift reporting (`kt.cohort`).
- Experiment across a cohort with BYO metrics, repeats, and `regressions()` (`cohort.experiment`).
- Iteration / re-run-only-what-changed.
- A second framework adapter (PydanticAI / OpenAI / Claude). The Protocol boundary is built; no second implementation.
- UI / dashboard. Reports remain code artifacts (structured + reusing PR #412's HTML).
- Mock / side-effect policy beyond what PR #412's safety validation already provides; tools are assumed replay-safe per the recorded `side_effect_status`.

## Affected files (anticipated)

- `src/kitaru/adapters/langgraph/replay.py` *(new)* — `KitaruReplayAgent`, the `seed/checkpoints/fork/capabilities` boundary, the DAG compiler, cached-or-live body.
- `src/kitaru/replay.py` — add the `skip=` selector alongside `from_`.
- `src/kitaru/_replay_verify_imported_sources/langfuse.py` — extend importer to key `recorded_calls` by `(node, call_index)`.
- `src/kitaru/_replay_verify_imported_validation.py` / `_reporting.py` — reuse `FieldComparison`; add the two-section `DriftReport`.
- `tests/` — spine tests with the fake model + reference-agent fixtures.
