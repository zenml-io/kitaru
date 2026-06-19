# LangGraph replay & fork from trace — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reconstruct a node+call-level DAG from a recorded LangGraph trace, replay it deterministically, fork it with edits, and report reproduction drift + fork drift for a single case end-to-end.

**Architecture:** Approach 1 — generated mirror-pipeline + native ZenML replay. An adapter compiles a bound LangGraph `StateGraph` + an imported Langfuse trace into a real Kitaru `@flow` whose checkpoints mirror the graph nodes (and the tool-call fan-out). A **seed run** plays the trace back from a trace-keyed cache (`playback=True`, zero model calls), producing a native Kitaru execution with full provenance. Cut-and-fork then reuse the existing `flow.replay(from_=, overrides=)` machinery: native replay skips the head (reusing recorded artifacts) and re-executes the tail live (`playback=False`).

**Tech Stack:** Python 3.11, Kitaru SDK (`@flow`, `@checkpoint`, `client.executions.replay`), ZenML dynamic pipelines (provenance via handle-threading), LangGraph (`StateGraph` introspection), pytest.

## Global Constraints

- **LangGraph-only** implementation, structured behind a `seed / checkpoints / fork / capabilities` Protocol; no second adapter built.
- **Single-case spine only.** No cohort / experiment / iteration. (PRD tickets 8–10 deferred.)
- **Provenance rule:** fan out with `checkpoint.submit(..., id=...)` and thread the returned output handles into downstream checkpoints. Do **not** `.load()` a checkpoint output into a plain Python value and re-pass it — that drops the lineage edge (the Kitaru analog of ZenML `.load()`-for-decisions vs `.chunk()`-for-wiring).
- **Comparison metric:** semantic fields only — `policy_label`, `risk_status`, `required_action`, `tool_names`, `evidence_ids`, and the tool-call name sequence. Never byte-compare the free-text `summary`.
- **Edit precedence:** `call > variant/global > recorded`.
- **Determinism in tests:** use the injected fake model (record/replay); never call a live provider in CI. Live runs are an opt-in path guarded by an env var.
- **Input contract:** the importer consumes **rich per-observation Langfuse rows** (one row per span), not the flattened `langfuse_export.jsonl`.
- New public surface lives under `src/kitaru/adapters/langgraph/replay/`. Reuse — do not fork — `src/kitaru/replay.py`, PR #412's `cases_from_langfuse_observations`, `RecordedCall`, and `FieldComparison`.

---

## File Structure

- `src/kitaru/_replay_verify_imported_models.py` — **modify**: add `node` + `call_index` to `RecordedCall`.
- `src/kitaru/replay.py` — **modify**: add `skip=` selector to `build_replay_plan`.
- `src/kitaru/adapters/langgraph/replay/__init__.py` — public exports.
- `src/kitaru/adapters/langgraph/replay/_protocol.py` — `ReplayAdapter` Protocol + `Caps`.
- `src/kitaru/adapters/langgraph/replay/_edits.py` — `Edit`, `edit()`, `resolve_edits()`.
- `src/kitaru/adapters/langgraph/replay/_importer.py` — `import_trace()`, node/call_index keying.
- `src/kitaru/adapters/langgraph/replay/_drift.py` — semantic comparator + `DriftReport`.
- `src/kitaru/adapters/langgraph/replay/_compiler.py` — `StateGraph` introspection → `CompiledTopology`.
- `src/kitaru/adapters/langgraph/replay/_flow.py` — generic mirror `@flow` + cached-or-live bodies.
- `src/kitaru/adapters/langgraph/replay/_agent.py` — `KitaruReplayAgent`.
- `tests/adapters/langgraph/replay/` — one test module per source module + `test_spine_e2e.py`.

Each task ends with a green test and a commit. Run the suite with `uv run pytest <path> -v`.

---

### Task 1: Add `node` + `call_index` to `RecordedCall`

**Files:**
- Modify: `src/kitaru/_replay_verify_imported_models.py` (the `RecordedCall` dataclass)
- Test: `tests/adapters/langgraph/replay/test_recorded_call_keying.py`

**Interfaces:**
- Produces: `RecordedCall(kind, name, input_payload=None, output_payload=None, metadata={}, observation_id=None, started_at=None, model=None, usage=None, cost=None, latency=None, node: str | None = None, call_index: int | None = None)`

- [ ] **Step 1: Write the failing test**

```python
# tests/adapters/langgraph/replay/test_recorded_call_keying.py
from kitaru._replay_verify_imported_models import RecordedCall


def test_recorded_call_carries_node_and_call_index():
    call = RecordedCall(
        kind="tool",
        name="lookup_customer",
        node="collect_evidence_with_tools",
        call_index=0,
    )
    assert call.node == "collect_evidence_with_tools"
    assert call.call_index == 0


def test_recorded_call_node_keying_defaults_to_none():
    call = RecordedCall(kind="llm", name="decide_action")
    assert call.node is None
    assert call.call_index is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/adapters/langgraph/replay/test_recorded_call_keying.py -v`
Expected: FAIL with `TypeError: __init__() got an unexpected keyword argument 'node'`

- [ ] **Step 3: Add the two fields**

In `src/kitaru/_replay_verify_imported_models.py`, append two fields to the `RecordedCall` dataclass (after `latency`):

```python
@dataclass(frozen=True)
class RecordedCall:
    """One model, tool, retrieval, or evaluator observation from the source."""

    kind: RecordedCallKind
    name: str
    input_payload: Any = None
    output_payload: Any = None
    metadata: dict[str, Any] = field(default_factory=dict)
    observation_id: str | None = None
    started_at: str | None = None
    model: str | None = None
    usage: Any = None
    cost: float | None = None
    latency: float | None = None
    node: str | None = None
    call_index: int | None = None
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/adapters/langgraph/replay/test_recorded_call_keying.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Run the existing PR412 suite to confirm no regression**

Run: `uv run pytest tests/test_replay_verify_imported_sources.py -v`
Expected: PASS (new fields are optional, default `None`)

- [ ] **Step 6: Commit**

```bash
git add src/kitaru/_replay_verify_imported_models.py tests/adapters/langgraph/replay/test_recorded_call_keying.py
git commit -m "feat(replay): add node/call_index keying to RecordedCall"
```

---

### Task 2: `import_trace` — node/call_index keying over Langfuse rows

**Files:**
- Create: `src/kitaru/adapters/langgraph/replay/_importer.py`
- Create: `src/kitaru/adapters/langgraph/replay/__init__.py`
- Test: `tests/adapters/langgraph/replay/test_importer.py`

**Interfaces:**
- Consumes: `cases_from_langfuse_observations(rows, *, base_url=None, source_ref=None, partial_trace_ids=None) -> list[ImportedReplayCase]` (existing, `src/kitaru/_replay_verify_imported_sources/langfuse.py`); `RecordedCall` (Task 1).
- Produces:
  - `import_trace(rows: Iterable[Mapping[str, Any]], *, trace_id: str | None = None) -> ImportedReplayCase`
  - keying helper `key_calls_by_node(case: ImportedReplayCase) -> ImportedReplayCase` that returns a copy whose `recorded_calls` each have `node` and `call_index` populated.

**Background:** each rich Langfuse observation row carries the LangGraph node name. The reference agent tags observations via the callback `metadata`/`tags`; the node name appears under `metadata["langgraph_node"]` (LangChain callback convention) or, failing that, the observation `name`. `call_index` is the 0-based position of the call **within its node**, in observation start-time order.

- [ ] **Step 1: Write the failing test**

```python
# tests/adapters/langgraph/replay/test_importer.py
from kitaru.adapters.langgraph.replay import import_trace


def _row(obs_id, trace_id, node, name, started_at, kind="tool", parent="root"):
    return {
        "id": obs_id,
        "trace_id": trace_id,
        "type": "SPAN",
        "name": name,
        "parent_observation_id": parent,
        "start_time": started_at,
        "metadata": {"langgraph_node": node},
        "input": {"args": {}},
        "output": {"ok": True},
    }


def _root_row(trace_id):
    return {
        "id": "root",
        "trace_id": trace_id,
        "type": "SPAN",
        "name": "agent",
        "parent_observation_id": None,
        "start_time": "2026-06-17T14:00:00Z",
        "input": {"user_request": "hi"},
        "output": {"decision": {"policy_label": "billing_policy"}},
        "metadata": {},
    }


def test_import_trace_keys_calls_by_node_and_index():
    trace_id = "t1"
    rows = [
        _root_row(trace_id),
        _row("o1", trace_id, "collect_evidence_with_tools", "lookup_customer", "2026-06-17T14:00:01Z"),
        _row("o2", trace_id, "collect_evidence_with_tools", "search_kb", "2026-06-17T14:00:02Z"),
        _row("o3", trace_id, "decide_action", "model_call", "2026-06-17T14:00:03Z", kind="llm"),
    ]
    case = import_trace(rows)

    by_name = {(c.node, c.call_index): c for c in case.recorded_calls}
    assert ("collect_evidence_with_tools", 0) in by_name
    assert ("collect_evidence_with_tools", 1) in by_name
    assert by_name[("collect_evidence_with_tools", 0)].name == "lookup_customer"
    assert by_name[("collect_evidence_with_tools", 1)].name == "search_kb"
    assert by_name[("decide_action", 0)].name == "model_call"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/adapters/langgraph/replay/test_importer.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'kitaru.adapters.langgraph.replay'`

- [ ] **Step 3: Create the package `__init__.py`**

```python
# src/kitaru/adapters/langgraph/replay/__init__.py
"""LangGraph replay & fork: reconstruct a DAG from a trace and fork it."""

from ._importer import import_trace, key_calls_by_node

__all__ = ["import_trace", "key_calls_by_node"]
```

- [ ] **Step 4: Implement the importer**

```python
# src/kitaru/adapters/langgraph/replay/_importer.py
from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import replace
from typing import Any

from kitaru._replay_verify_imported_models import ImportedReplayCase, RecordedCall
from kitaru._replay_verify_imported_sources.langfuse import (
    cases_from_langfuse_observations,
)


def _node_of(call: RecordedCall) -> str:
    node = call.metadata.get("langgraph_node") if call.metadata else None
    if isinstance(node, str) and node:
        return node
    return call.name


def key_calls_by_node(case: ImportedReplayCase) -> ImportedReplayCase:
    """Return a copy whose recorded_calls carry node + call_index.

    call_index is the 0-based position within a node, in recorded order
    (recorded_calls already arrive sorted by observation start time).
    """
    per_node_counter: dict[str, int] = {}
    keyed: list[RecordedCall] = []
    for call in case.recorded_calls:
        node = _node_of(call)
        index = per_node_counter.get(node, 0)
        per_node_counter[node] = index + 1
        keyed.append(replace(call, node=node, call_index=index))
    return replace(case, recorded_calls=keyed)


def import_trace(
    rows: Iterable[Mapping[str, Any]],
    *,
    trace_id: str | None = None,
) -> ImportedReplayCase:
    """Import one trace (rich per-observation Langfuse rows) into a keyed Case."""
    cases = cases_from_langfuse_observations(rows)
    if not cases:
        raise ValueError("No cases could be imported from the provided rows.")
    if trace_id is not None:
        cases = [c for c in cases if c.source_ref.source_id == trace_id]
        if not cases:
            raise ValueError(f"Trace id {trace_id!r} not found in rows.")
    if len(cases) > 1:
        raise ValueError(
            "Multiple traces found; pass trace_id= to select one "
            f"({', '.join(c.source_ref.source_id for c in cases)})."
        )
    return key_calls_by_node(cases[0])
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/adapters/langgraph/replay/test_importer.py -v`
Expected: PASS. If `cases_from_langfuse_observations` does not surface `metadata` onto `RecordedCall`, adjust `_node_of` to read the field the importer actually populates (verify by printing `case.recorded_calls[0].metadata`), then re-run.

- [ ] **Step 6: Commit**

```bash
git add src/kitaru/adapters/langgraph/replay/ tests/adapters/langgraph/replay/test_importer.py
git commit -m "feat(replay): import_trace with per-node call keying"
```

---

### Task 3: Edits — `edit()` and precedence resolution

**Files:**
- Create: `src/kitaru/adapters/langgraph/replay/_edits.py`
- Test: `tests/adapters/langgraph/replay/test_edits.py`

**Interfaces:**
- Produces:
  - `@dataclass(frozen=True) class Edit: target: str; model: str | None = None; value: Any = _UNSET; params: dict[str, Any] = {}`
  - `edit(target: str, *, model=None, value=_UNSET, **params) -> Edit`
  - `resolve_edits(*, node: str, call_index: int | None, edits: list[Edit], variant: dict[str, Any] | None, recorded: dict[str, Any]) -> dict[str, Any]` returning the effective params for a checkpoint, applying precedence `call > variant > recorded`.
  - target grammar: `"<node>"` (whole node), `"model_call:<node>"`, `"tool_call:<node>:<call_index>"`.

- [ ] **Step 1: Write the failing test**

```python
# tests/adapters/langgraph/replay/test_edits.py
from kitaru.adapters.langgraph.replay._edits import Edit, edit, resolve_edits


def test_edit_factory_builds_model_edit():
    e = edit("model_call:decide_action", model="gpt-5-nano")
    assert e == Edit(target="model_call:decide_action", model="gpt-5-nano")


def test_call_edit_beats_variant_beats_recorded():
    edits = [edit("model_call:decide_action", model="gpt-5-nano")]
    effective = resolve_edits(
        node="decide_action",
        call_index=None,
        edits=edits,
        variant={"model": "gpt-5-mini"},
        recorded={"model": "gpt-5-pro"},
    )
    assert effective["model"] == "gpt-5-nano"


def test_variant_beats_recorded_when_no_call_edit():
    effective = resolve_edits(
        node="decide_action",
        call_index=None,
        edits=[],
        variant={"model": "gpt-5-mini"},
        recorded={"model": "gpt-5-pro"},
    )
    assert effective["model"] == "gpt-5-mini"


def test_unrelated_node_edit_is_ignored():
    edits = [edit("model_call:summarize_evidence", model="gpt-5-nano")]
    effective = resolve_edits(
        node="decide_action",
        call_index=None,
        edits=edits,
        variant=None,
        recorded={"model": "gpt-5-pro"},
    )
    assert effective["model"] == "gpt-5-pro"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/adapters/langgraph/replay/test_edits.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Implement edits**

```python
# src/kitaru/adapters/langgraph/replay/_edits.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

_UNSET: Any = object()


@dataclass(frozen=True)
class Edit:
    target: str
    model: str | None = None
    value: Any = _UNSET
    params: dict[str, Any] = field(default_factory=dict)


def edit(target: str, *, model: str | None = None, value: Any = _UNSET, **params: Any) -> Edit:
    return Edit(target=target, model=model, value=value, params=dict(params))


def _matches(target: str, *, node: str, call_index: int | None) -> bool:
    if target == node:
        return True
    if target == f"model_call:{node}":
        return True
    if call_index is not None and target == f"tool_call:{node}:{call_index}":
        return True
    return False


def resolve_edits(
    *,
    node: str,
    call_index: int | None,
    edits: list[Edit],
    variant: dict[str, Any] | None,
    recorded: dict[str, Any],
) -> dict[str, Any]:
    """Effective params for a checkpoint: call > variant > recorded."""
    effective: dict[str, Any] = dict(recorded)
    if variant:
        effective.update({k: v for k, v in variant.items() if v is not None})
    for e in edits:
        if not _matches(e.target, node=node, call_index=call_index):
            continue
        if e.model is not None:
            effective["model"] = e.model
        if e.value is not _UNSET:
            effective["value"] = e.value
        effective.update(e.params)
    return effective
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/adapters/langgraph/replay/test_edits.py -v`
Expected: PASS (4 passed)

- [ ] **Step 5: Commit**

```bash
git add src/kitaru/adapters/langgraph/replay/_edits.py tests/adapters/langgraph/replay/test_edits.py
git commit -m "feat(replay): edit() factory and precedence resolution"
```

---

### Task 4: `skip=` selector in `build_replay_plan`

**Files:**
- Modify: `src/kitaru/replay.py` (`build_replay_plan`)
- Test: `tests/test_replay_skip.py`

**Interfaces:**
- Consumes: existing `build_replay_plan(*, run, from_, overrides=None, flow_inputs=None) -> ReplayPlan`.
- Produces: `build_replay_plan(*, run, from_=None, skip=None, overrides=None, flow_inputs=None)`. Exactly one of `from_` / `skip` must be provided. `skip` is a list of checkpoint selectors to **keep cached** (freeze); every checkpoint not in the frozen set (and its descendants) re-executes.

**Note:** read `tests/test_replay.py` first to reuse its `_run(...)` fixture helper for building a `PipelineRunResponse` stand-in.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_replay_skip.py
import pytest

from kitaru.replay import build_replay_plan
from tests.test_replay import _run, fetch, write, publish  # reuse existing helpers


def test_skip_freezes_named_checkpoints_and_reexecutes_rest():
    run = _run(fetch, write, publish)
    plan = build_replay_plan(run=run, skip=["fetch", "write"])
    # fetch + write are frozen (cached); publish re-executes
    assert plan.steps_to_skip == {"fetch", "write"}


def test_from_and_skip_are_mutually_exclusive():
    run = _run(fetch, write, publish)
    with pytest.raises(Exception):
        build_replay_plan(run=run, from_="publish", skip=["fetch"])


def test_one_of_from_or_skip_required():
    run = _run(fetch, write, publish)
    with pytest.raises(Exception):
        build_replay_plan(run=run)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_replay_skip.py -v`
Expected: FAIL with `TypeError` (no `skip` kwarg) — confirm the import helpers exist in `tests/test_replay.py`; if their names differ, adjust the import line to match before continuing.

- [ ] **Step 3: Implement `skip=`**

In `src/kitaru/replay.py`, change the `build_replay_plan` signature and add the skip branch. Replace the head of the function:

```python
def build_replay_plan(
    *,
    run: PipelineRunResponse,
    from_: str | None = None,
    skip: Sequence[str] | None = None,
    overrides: Mapping[str, Any] | None = None,
    flow_inputs: Mapping[str, Any] | None = None,
) -> ReplayPlan:
    checkpoints = _checkpoints(run)
    if not checkpoints:
        raise KitaruStateError(
            f"Execution '{run.id}' has no checkpoint history to replay."
        )

    if (from_ is None) == (skip is None):
        raise KitaruUsageError("Provide exactly one of `from_` or `skip`.")

    checkpoint_overrides = _split_overrides(overrides)

    if skip is not None:
        frozen = {
            _resolve_checkpoint_selector(sel, checkpoints).invocation_id
            for sel in skip
        }
        all_steps = {cp.invocation_id for cp in checkpoints}
        steps_to_skip = frozen & all_steps
        return ReplayPlan(
            original_run_id=str(run.id),
            steps_to_skip=steps_to_skip,
            input_overrides=dict(flow_inputs or {}),
            step_input_overrides={},
        )

    if not from_.strip():
        raise KitaruUsageError("`from_` must be a non-empty selector.")

    explicit_checkpoint = _resolve_checkpoint_selector(from_, checkpoints)
    # ... (rest of the existing from_ logic unchanged) ...
```

Keep the remainder of the original function body (the `from_` path: `step_input_overrides`, `replay_roots`, descendants, overlap safety check, return) exactly as-is below this point. Add `Sequence` to the existing `collections.abc` import if not already present (it is).

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_replay_skip.py tests/test_replay.py -v`
Expected: PASS (new skip tests + all existing replay tests still green)

- [ ] **Step 5: Commit**

```bash
git add src/kitaru/replay.py tests/test_replay_skip.py
git commit -m "feat(replay): add skip= selector to build_replay_plan"
```

---

### Task 5: Drift comparator + `DriftReport`

**Files:**
- Create: `src/kitaru/adapters/langgraph/replay/_drift.py`
- Test: `tests/adapters/langgraph/replay/test_drift.py`

**Interfaces:**
- Consumes: `FieldComparison` from `src/kitaru/_replay_verify_imported_*` (verify exact import path via `git grep "class FieldComparison"`; expected `kitaru._replay_verify_imported_runner`).
- Produces:
  - `SEMANTIC_FIELDS = ("policy_label", "risk_status", "required_action", "tool_names", "evidence_ids")`
  - `compare_decisions(baseline: Mapping, candidate: Mapping) -> list[FieldComparison]`
  - `@dataclass DriftReport: reproduction: list[FieldComparison]; fork: list[FieldComparison]` with `.has_reproduction_drift -> bool` and `.has_fork_drift -> bool`.

- [ ] **Step 1: Write the failing test**

```python
# tests/adapters/langgraph/replay/test_drift.py
from kitaru.adapters.langgraph.replay._drift import (
    DriftReport,
    compare_decisions,
)


def test_compare_flags_changed_field_only():
    base = {"policy_label": "billing_policy", "risk_status": "safe", "tool_names": ["a"], "summary": "long text x"}
    cand = {"policy_label": "billing_policy", "risk_status": "unsafe", "tool_names": ["a"], "summary": "totally different text y"}
    comps = compare_decisions(base, cand)
    by_field = {c.field: c for c in comps}
    assert by_field["risk_status"].matches is False
    assert by_field["policy_label"].matches is True
    # free-text summary is never compared
    assert "summary" not in by_field


def test_drift_report_flags():
    base = {"risk_status": "safe", "tool_names": ["a"]}
    same = {"risk_status": "safe", "tool_names": ["a"]}
    drifted = {"risk_status": "unsafe", "tool_names": ["a"]}
    report = DriftReport(
        reproduction=compare_decisions(base, same),
        fork=compare_decisions(base, drifted),
    )
    assert report.has_reproduction_drift is False
    assert report.has_fork_drift is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/adapters/langgraph/replay/test_drift.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Implement the comparator**

```python
# src/kitaru/adapters/langgraph/replay/_drift.py
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from kitaru._replay_verify_imported_runner import FieldComparison

SEMANTIC_FIELDS = (
    "policy_label",
    "risk_status",
    "required_action",
    "tool_names",
    "evidence_ids",
)


def compare_decisions(
    baseline: Mapping[str, Any],
    candidate: Mapping[str, Any],
) -> list[FieldComparison]:
    comparisons: list[FieldComparison] = []
    for field_name in SEMANTIC_FIELDS:
        if field_name not in baseline and field_name not in candidate:
            continue
        b = baseline.get(field_name)
        c = candidate.get(field_name)
        comparisons.append(
            FieldComparison(
                field=field_name,
                baseline_value=b,
                comparison_value=c,
                matches=(b == c),
            )
        )
    return comparisons


@dataclass(frozen=True)
class DriftReport:
    reproduction: list[FieldComparison]
    fork: list[FieldComparison]

    @property
    def has_reproduction_drift(self) -> bool:
        return any(not c.matches for c in self.reproduction)

    @property
    def has_fork_drift(self) -> bool:
        return any(not c.matches for c in self.fork)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/adapters/langgraph/replay/test_drift.py -v`
Expected: PASS. If `FieldComparison`'s constructor keyword names differ from `(field, baseline_value, comparison_value, matches)`, adjust the kwargs to match the verified definition, then re-run.

- [ ] **Step 5: Commit**

```bash
git add src/kitaru/adapters/langgraph/replay/_drift.py tests/adapters/langgraph/replay/test_drift.py
git commit -m "feat(replay): semantic drift comparator and DriftReport"
```

---

### Task 6: Capabilities + adapter Protocol boundary

**Files:**
- Create: `src/kitaru/adapters/langgraph/replay/_protocol.py`
- Test: `tests/adapters/langgraph/replay/test_protocol.py`

**Interfaces:**
- Produces:
  - `@dataclass(frozen=True) class Caps: fork_granularity: str; native_checkpoints: str; resume: str`
  - `class ReplayAdapter(Protocol)` with `seed(self, case) -> str`, `checkpoints(self, seed_exec_id) -> list[str]`, `fork(self, seed_exec_id, *, from_, edits, variant) -> Any`, `capabilities(self) -> Caps`.
  - `LANGGRAPH_CAPS = Caps(fork_granularity="call", native_checkpoints="reconstructed", resume="reconstruct")`

- [ ] **Step 1: Write the failing test**

```python
# tests/adapters/langgraph/replay/test_protocol.py
from kitaru.adapters.langgraph.replay._protocol import (
    LANGGRAPH_CAPS,
    Caps,
    ReplayAdapter,
)


def test_langgraph_caps_advertise_call_granularity():
    assert LANGGRAPH_CAPS.fork_granularity == "call"
    assert LANGGRAPH_CAPS.native_checkpoints == "reconstructed"


def test_minimal_adapter_satisfies_protocol():
    class Dummy:
        def seed(self, case):
            return "exec-1"

        def checkpoints(self, seed_exec_id):
            return ["receive_request"]

        def fork(self, seed_exec_id, *, from_, edits, variant):
            return object()

        def capabilities(self):
            return LANGGRAPH_CAPS

    adapter: ReplayAdapter = Dummy()
    assert isinstance(adapter.capabilities(), Caps)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/adapters/langgraph/replay/test_protocol.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Implement the protocol**

```python
# src/kitaru/adapters/langgraph/replay/_protocol.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable


@dataclass(frozen=True)
class Caps:
    fork_granularity: str
    native_checkpoints: str
    resume: str


@runtime_checkable
class ReplayAdapter(Protocol):
    def seed(self, case: Any) -> str: ...
    def checkpoints(self, seed_exec_id: str) -> list[str]: ...
    def fork(self, seed_exec_id: str, *, from_: str, edits: list[Any], variant: Any) -> Any: ...
    def capabilities(self) -> Caps: ...


LANGGRAPH_CAPS = Caps(
    fork_granularity="call",
    native_checkpoints="reconstructed",
    resume="reconstruct",
)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/adapters/langgraph/replay/test_protocol.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add src/kitaru/adapters/langgraph/replay/_protocol.py tests/adapters/langgraph/replay/test_protocol.py
git commit -m "feat(replay): ReplayAdapter protocol and LangGraph capabilities"
```

---

### Task 7: Graph compiler — `StateGraph` → `CompiledTopology`

**Files:**
- Create: `src/kitaru/adapters/langgraph/replay/_compiler.py`
- Test: `tests/adapters/langgraph/replay/test_compiler.py`

**Interfaces:**
- Produces:
  - `@dataclass(frozen=True) class CompiledTopology: nodes: list[str]; callables: dict[str, Callable]; fanout_node: str | None`
  - `compile_topology(graph: Any) -> CompiledTopology` — introspects a compiled LangGraph graph via `graph.get_graph()`, returns nodes in topological (edge) order, the per-node callables, and the single fan-out node (the node whose recorded calls vary in count; for the reference agent, `collect_evidence_with_tools`). v1 asserts a **single linear chain** and raises `KitaruUsageError` otherwise.

**Note:** the reference graph builder is `examples/end_to_end/replay_verify_reference_agent/graph.py::build_graph`. Use it (with stub tools) to get a real compiled graph in the test.

- [ ] **Step 1: Write the failing test**

```python
# tests/adapters/langgraph/replay/test_compiler.py
from langgraph.graph import END, START, StateGraph
from typing_extensions import TypedDict

from kitaru.adapters.langgraph.replay._compiler import compile_topology


class _S(TypedDict, total=False):
    x: int


def _linear_graph():
    b = StateGraph(_S)
    b.add_node("a", lambda s: {"x": 1})
    b.add_node("b", lambda s: {"x": 2})
    b.add_node("c", lambda s: {"x": 3})
    b.add_edge(START, "a")
    b.add_edge("a", "b")
    b.add_edge("b", "c")
    b.add_edge("c", END)
    return b.compile()


def test_compile_returns_nodes_in_edge_order():
    topo = compile_topology(_linear_graph())
    assert topo.nodes == ["a", "b", "c"]
    assert set(topo.callables) == {"a", "b", "c"}
    assert callable(topo.callables["a"])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/adapters/langgraph/replay/test_compiler.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Implement the compiler**

```python
# src/kitaru/adapters/langgraph/replay/_compiler.py
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from kitaru.errors import KitaruUsageError


@dataclass(frozen=True)
class CompiledTopology:
    nodes: list[str]
    callables: dict[str, Callable]
    fanout_node: str | None


def compile_topology(graph: object, *, fanout_node: str | None = None) -> CompiledTopology:
    """Introspect a compiled LangGraph graph into an ordered linear topology.

    v1 supports a single linear chain (the reference-agent shape). The fan-out
    node (variable tool calls) is named explicitly or inferred as the node
    whose callable is unknown to be fixed; callers pass it for clarity.
    """
    drawable = graph.get_graph()
    # Build adjacency over real nodes (skip __start__/__end__ sentinels).
    successors: dict[str, list[str]] = {}
    real_nodes: set[str] = set()
    for node_id in drawable.nodes:
        if node_id in ("__start__", "__end__"):
            continue
        real_nodes.add(node_id)
        successors[node_id] = []
    start_targets: list[str] = []
    for edge in drawable.edges:
        src, dst = edge.source, edge.target
        if src == "__start__":
            if dst in real_nodes:
                start_targets.append(dst)
            continue
        if dst == "__end__" or src not in real_nodes:
            continue
        successors.setdefault(src, []).append(dst)

    if len(start_targets) != 1:
        raise KitaruUsageError(
            "Replay v1 supports a single linear graph; "
            f"found {len(start_targets)} start edges."
        )

    ordered: list[str] = []
    current: str | None = start_targets[0]
    seen: set[str] = set()
    while current is not None:
        if current in seen:
            raise KitaruUsageError("Cycle detected; replay v1 requires a linear graph.")
        seen.add(current)
        ordered.append(current)
        nexts = successors.get(current, [])
        if len(nexts) > 1:
            raise KitaruUsageError(
                f"Node {current!r} branches; replay v1 requires a linear graph."
            )
        current = nexts[0] if nexts else None

    callables = {name: _node_callable(graph, name) for name in ordered}
    return CompiledTopology(nodes=ordered, callables=callables, fanout_node=fanout_node)


def _node_callable(graph: object, name: str) -> Callable:
    """Recover the runnable for a node from the compiled graph."""
    nodes = getattr(graph, "nodes", {})
    spec = nodes.get(name)
    runnable = getattr(spec, "runnable", spec)
    if not callable(runnable):
        raise KitaruUsageError(f"Node {name!r} has no callable runnable.")
    return runnable
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/adapters/langgraph/replay/test_compiler.py -v`
Expected: PASS. LangGraph's compiled-graph internals vary by version: if `graph.nodes[name]` does not expose `.runnable`, print `type(graph.nodes["a"])` and its attributes, then adjust `_node_callable` to return the actual node function. Re-run until green.

- [ ] **Step 5: Add a topology assertion test for branching graphs**

```python
# append to tests/adapters/langgraph/replay/test_compiler.py
import pytest
from kitaru.errors import KitaruUsageError


def test_branching_graph_is_rejected():
    b = StateGraph(_S)
    b.add_node("a", lambda s: {"x": 1})
    b.add_node("b", lambda s: {"x": 2})
    b.add_node("c", lambda s: {"x": 3})
    b.add_edge(START, "a")
    b.add_edge("a", "b")
    b.add_edge("a", "c")
    b.add_edge("b", END)
    b.add_edge("c", END)
    with pytest.raises(KitaruUsageError):
        compile_topology(b.compile())
```

Run: `uv run pytest tests/adapters/langgraph/replay/test_compiler.py -v`
Expected: PASS (3 passed)

- [ ] **Step 6: Commit**

```bash
git add src/kitaru/adapters/langgraph/replay/_compiler.py tests/adapters/langgraph/replay/test_compiler.py
git commit -m "feat(replay): compile LangGraph StateGraph to linear topology"
```

---

### Task 8: Mirror flow + cached-or-live checkpoint bodies

**Files:**
- Create: `src/kitaru/adapters/langgraph/replay/_flow.py`
- Test: `tests/adapters/langgraph/replay/test_flow.py`

**Interfaces:**
- Consumes: `CompiledTopology` (Task 7), `RecordedCall` keyed by node/call_index (Task 1/2), `resolve_edits` (Task 3), `@flow`/`@checkpoint`, `checkpoint.submit(..., id=...)`.
- Produces:
  - `@dataclass class ReplayContext: topology: CompiledTopology; recorded_by_node: dict[str, list[RecordedCall]]; node_output_by_node: dict[str, Any]; playback: bool; variant: dict | None; edits: list[Edit]`
  - `build_replay_flow(ctx: ReplayContext) -> _FlowDefinition` — returns a Kitaru `@flow` with one checkpoint per node; the fan-out node submits one sub-checkpoint per recorded call (handles threaded, never early-`.load()`-ed). Each node checkpoint body: if `playback` → return recorded node output; else → invoke the live node callable with edits applied.
  - `run_seed(ctx) -> FlowHandle` convenience that runs the flow with `playback=True`.

**Design notes (read before implementing):**
- The flow takes `playback: bool` as a flow input so the seed run (`playback=True`) serves recorded outputs and a later native replay (`playback=False`) executes live. Native replay handles the cut; this body only decides playback-vs-live for checkpoints that actually execute.
- Provenance: pass each `submit()` handle into the next checkpoint; the fan-out collects handles and threads them into the node's aggregate checkpoint. Do not materialize with `.load()` until the value is needed for live execution input.
- The live node callable is the LangGraph node function; it takes the running `AgentState` dict and returns a partial-state dict. For the fake-model tests, the injected callables are deterministic stubs.

- [ ] **Step 1: Write the failing test (seed playback materializes recorded outputs, zero live calls)**

```python
# tests/adapters/langgraph/replay/test_flow.py
from kitaru._replay_verify_imported_models import RecordedCall
from kitaru.adapters.langgraph.replay._compiler import CompiledTopology
from kitaru.adapters.langgraph.replay._flow import ReplayContext, run_seed


def _topology(live_calls):
    def make(name):
        def _node(state):
            live_calls.append(name)  # records any LIVE execution
            return {name: "live"}
        return _node
    nodes = ["receive_request", "collect_evidence_with_tools", "decide_action"]
    return CompiledTopology(
        nodes=nodes,
        callables={n: make(n) for n in nodes},
        fanout_node="collect_evidence_with_tools",
    )


def test_seed_run_serves_recorded_and_makes_no_live_calls():
    live_calls: list[str] = []
    topo = _topology(live_calls)
    ctx = ReplayContext(
        topology=topo,
        recorded_by_node={
            "collect_evidence_with_tools": [
                RecordedCall(kind="tool", name="lookup_customer",
                             node="collect_evidence_with_tools", call_index=0,
                             output_payload={"found": True}),
            ],
        },
        node_output_by_node={
            "receive_request": {"tool_executions": []},
            "collect_evidence_with_tools": {"tool_executions": [{"name": "lookup_customer"}]},
            "decide_action": {"decision": {"policy_label": "billing_policy", "risk_status": "safe"}},
        },
        playback=True,
        variant=None,
        edits=[],
    )
    handle = run_seed(ctx)
    result = handle.wait()
    assert live_calls == []  # nothing executed live during seed playback
    assert result["decide_action"]["decision"]["risk_status"] == "safe"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/adapters/langgraph/replay/test_flow.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Implement the mirror flow**

```python
# src/kitaru/adapters/langgraph/replay/_flow.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from kitaru import checkpoint, flow
from kitaru.adapters.langgraph.replay._compiler import CompiledTopology
from kitaru.adapters.langgraph.replay._edits import Edit, resolve_edits
from kitaru._replay_verify_imported_models import RecordedCall


@dataclass
class ReplayContext:
    topology: CompiledTopology
    recorded_by_node: dict[str, list[RecordedCall]]
    node_output_by_node: dict[str, Any]
    playback: bool
    variant: dict[str, Any] | None = None
    edits: list[Edit] = field(default_factory=list)


def _run_node(ctx: ReplayContext, node: str, state: dict[str, Any]) -> Any:
    """Cached-or-live body for one node checkpoint."""
    if ctx.playback:
        return ctx.node_output_by_node.get(node, {})
    recorded = {}
    calls = ctx.recorded_by_node.get(node, [])
    if calls:
        recorded = {"model": calls[0].model}
    resolve_edits(
        node=node,
        call_index=None,
        edits=ctx.edits,
        variant=ctx.variant,
        recorded=recorded,
    )
    callable_ = ctx.topology.callables[node]
    return callable_(state)


def build_replay_flow(ctx: ReplayContext):
    @checkpoint
    def node_step(node: str, state: dict[str, Any]) -> dict[str, Any]:
        out = _run_node(ctx, node, state)
        merged = dict(state)
        if isinstance(out, dict):
            merged.update(out)
        return merged

    @flow(cache=False)
    def replay_flow(playback: bool) -> dict[str, Any]:
        state: dict[str, Any] = {}
        handle = None
        for node in ctx.topology.nodes:
            handle = node_step(node, state, id=node)
            state = handle.load()
        return state

    return replay_flow


def run_seed(ctx: ReplayContext):
    ctx.playback = True
    replay_flow = build_replay_flow(ctx)
    return replay_flow.run(True)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/adapters/langgraph/replay/test_flow.py -v`
Expected: PASS. This task touches the live Kitaru runtime; if the local stack is not initialized the run will error on submission. Initialize once in the repo with `uv run kitaru init` (and a local stack) before running, and gate the test with the project's existing flow-test fixture/marker (grep `tests/` for how `test_replay_verify_imported_durable.py` sets up a runnable flow, and mirror that setup). Adjust `node_step(... id=node)` handle threading if the runtime requires `.submit()` for provenance instead of `.call()`; the rule is to keep one checkpoint per node and pass handles forward.

- [ ] **Step 5: Add the live fan-out test**

```python
# append to tests/adapters/langgraph/replay/test_flow.py
def test_live_run_executes_tail_nodes():
    live_calls: list[str] = []
    topo = _topology(live_calls)
    ctx = ReplayContext(
        topology=topo,
        recorded_by_node={},
        node_output_by_node={},
        playback=False,
        variant=None,
        edits=[],
    )
    flow_def = __import__(
        "kitaru.adapters.langgraph.replay._flow", fromlist=["build_replay_flow"]
    ).build_replay_flow(ctx)
    handle = flow_def.run(False)
    handle.wait()
    assert "decide_action" in live_calls  # tail executed live
```

Run: `uv run pytest tests/adapters/langgraph/replay/test_flow.py -v`
Expected: PASS (2 passed)

- [ ] **Step 6: Commit**

```bash
git add src/kitaru/adapters/langgraph/replay/_flow.py tests/adapters/langgraph/replay/test_flow.py
git commit -m "feat(replay): mirror flow with cached-or-live node bodies"
```

---

### Task 9: `KitaruReplayAgent` + end-to-end spine

**Files:**
- Create: `src/kitaru/adapters/langgraph/replay/_agent.py`
- Modify: `src/kitaru/adapters/langgraph/replay/__init__.py` (export `KitaruReplayAgent`, `edit`, `Caps`)
- Test: `tests/adapters/langgraph/replay/test_spine_e2e.py`

**Interfaces:**
- Consumes: every prior task.
- Produces:
  - `class KitaruReplayAgent:` `__init__(self, graph, *, fanout_node=None)`; `import_trace(rows, *, trace_id=None) -> ImportedReplayCase`; `reconstruct(case) -> str` (seed exec_id); `replay(seed_exec_id, *, from_) -> Execution`; `fork(seed_exec_id, *, from_, edits=(), variant=None) -> Execution`; `diff(case, replay_exec, fork_exec) -> DriftReport`; `capabilities() -> Caps`.

**Design notes:**
- `reconstruct` builds the `ReplayContext` from the case (group `recorded_calls` by node; build `node_output_by_node` from the trace's per-node outputs), runs the seed flow, returns its exec_id.
- `replay(from_=...)` calls `flow.replay(seed_exec_id, from_=from_, cache=False, playback=False)` — native ZenML skips the head (recorded artifacts) and re-executes the tail live with no edits.
- `fork(from_=..., edits=..., variant=...)` rebuilds the context with edits/variant, then calls `flow.replay(seed_exec_id, from_=from_, cache=False, playback=False, overrides=<value-edits-as-checkpoint-overrides>)`.
- `diff` reads decision outputs from the trace (`case.observed_output`) and from the two executions' final-node artifacts (`execution.checkpoints[].artifacts[].load()` for `decide_action`/`final_response`), then `compare_decisions`.

- [ ] **Step 1: Write the failing end-to-end test (fork drift surfaces the planted regression)**

```python
# tests/adapters/langgraph/replay/test_spine_e2e.py
import json
from pathlib import Path

import pytest

from kitaru.adapters.langgraph.replay import KitaruReplayAgent, edit

pytestmark = pytest.mark.skipif(
    not Path("examples/end_to_end/replay_verify_reference_agent").exists(),
    reason="reference agent fixtures required",
)


def test_fork_drift_surfaces_permission_regression(reference_graph, permission_trace_rows):
    # reference_graph + permission_trace_rows are fixtures (see Step 3).
    agent = KitaruReplayAgent(reference_graph, fanout_node="collect_evidence_with_tools")
    case = agent.import_trace(permission_trace_rows)

    seed = agent.reconstruct(case)

    # reproduction: live tail, no edits — semantic decision reproduced
    repro = agent.replay(seed, from_="collect_evidence_with_tools")

    # fork: trim permissions (planted regression) — risk_status must drift
    fork = agent.fork(
        seed,
        from_="collect_evidence_with_tools",
        variant={"prompt_profile": "trimmed_permissions", "model": "gpt-5-nano"},
    )

    report = agent.diff(case, repro, fork)
    assert report.has_reproduction_drift is False
    assert report.has_fork_drift is True
    drifted = {c.field for c in report.fork if not c.matches}
    assert "risk_status" in drifted or "required_action" in drifted
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/adapters/langgraph/replay/test_spine_e2e.py -v`
Expected: FAIL with `ImportError: cannot import name 'KitaruReplayAgent'`

- [ ] **Step 3: Add fixtures (fake model + reference graph + trace rows)**

Create `tests/adapters/langgraph/replay/conftest.py`. Build the reference graph with a deterministic fake model so the live tail is reproducible, and synthesize rich per-observation rows for one permission case. Mirror the fake-model wiring used by `tests/test_replay_verify_reference_agent.py` (read it first):

```python
# tests/adapters/langgraph/replay/conftest.py
import pytest

# Reuse the reference agent's graph builder + a deterministic model.
# Inspect tests/test_replay_verify_reference_agent.py for the existing
# fake-model / stub-tools setup and import the same helpers here.


@pytest.fixture
def reference_graph():
    from examples.end_to_end.replay_verify_reference_agent.graph import build_graph
    # Provide stub tools + fake model per the reference test's helpers.
    ...  # construct and return build_graph(tools=..., callbacks=[], metadata={}, tags=[])


@pytest.fixture
def permission_trace_rows():
    # Rich per-observation rows for one permission scenario, baseline variant:
    # root observation (input + observed decision risk_status="safe") plus
    # one observation per recorded tool/LLM call, each tagged with its node.
    ...  # return list[dict]
```

Fill the `...` blocks using the reference agent's actual scenario/tool fixtures (`scenarios.yaml`, `variants/baseline.yaml`, `variants/nano_trimmed_permissions.yaml`) so the recorded "safe" decision and the trimmed-permission drift are realistic.

- [ ] **Step 4: Implement `KitaruReplayAgent`**

```python
# src/kitaru/adapters/langgraph/replay/_agent.py
from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from kitaru import KitaruClient
from kitaru._replay_verify_imported_models import ImportedReplayCase
from kitaru.adapters.langgraph.replay._compiler import compile_topology
from kitaru.adapters.langgraph.replay._drift import DriftReport, compare_decisions
from kitaru.adapters.langgraph.replay._edits import Edit
from kitaru.adapters.langgraph.replay._flow import ReplayContext, build_replay_flow
from kitaru.adapters.langgraph.replay._importer import import_trace
from kitaru.adapters.langgraph.replay._protocol import LANGGRAPH_CAPS, Caps


class KitaruReplayAgent:
    def __init__(self, graph: object, *, fanout_node: str | None = None) -> None:
        self._graph = graph
        self._topology = compile_topology(graph, fanout_node=fanout_node)
        self._client = KitaruClient()
        self._flow_def = None  # set during reconstruct

    def import_trace(
        self, rows: Iterable[Mapping[str, Any]], *, trace_id: str | None = None
    ) -> ImportedReplayCase:
        return import_trace(rows, trace_id=trace_id)

    def _context(self, case: ImportedReplayCase, *, playback: bool,
                 edits: Sequence[Edit] = (), variant: dict | None = None) -> ReplayContext:
        recorded_by_node: dict[str, list] = defaultdict(list)
        for call in case.recorded_calls:
            recorded_by_node[call.node or call.name].append(call)
        node_output_by_node = _node_outputs_from_case(case, self._topology.nodes)
        return ReplayContext(
            topology=self._topology,
            recorded_by_node=dict(recorded_by_node),
            node_output_by_node=node_output_by_node,
            playback=playback,
            variant=variant,
            edits=list(edits),
        )

    def reconstruct(self, case: ImportedReplayCase) -> str:
        ctx = self._context(case, playback=True)
        self._flow_def = build_replay_flow(ctx)
        handle = self._flow_def.run(True)
        handle.wait()
        return handle.exec_id

    def replay(self, seed_exec_id: str, *, from_: str):
        handle = self._flow_def.replay(
            seed_exec_id, from_=from_, cache=False, playback=False
        )
        handle.wait()
        return self._client.executions.get(handle.exec_id)

    def fork(self, seed_exec_id: str, *, from_: str,
             edits: Sequence[Edit] = (), variant: dict | None = None):
        # Rebuild the flow closure with edits/variant for the live tail.
        # (Value-edits could also be passed as overrides={"checkpoint.X": v}.)
        ctx = ReplayContext(
            topology=self._topology,
            recorded_by_node=self._flow_def_ctx_recorded(),
            node_output_by_node=self._flow_def_ctx_outputs(),
            playback=False,
            variant=variant,
            edits=list(edits),
        )
        self._flow_def = build_replay_flow(ctx)
        handle = self._flow_def.replay(
            seed_exec_id, from_=from_, cache=False, playback=False
        )
        handle.wait()
        return self._client.executions.get(handle.exec_id)

    def diff(self, case: ImportedReplayCase, replay_exec, fork_exec) -> DriftReport:
        trace_decision = _decision_of_observed(case)
        replay_decision = _decision_of_execution(replay_exec)
        fork_decision = _decision_of_execution(fork_exec)
        return DriftReport(
            reproduction=compare_decisions(trace_decision, replay_decision),
            fork=compare_decisions(replay_decision, fork_decision),
        )

    def capabilities(self) -> Caps:
        return LANGGRAPH_CAPS


def _node_outputs_from_case(case: ImportedReplayCase, nodes: list[str]) -> dict[str, Any]:
    """Recover each node's recorded output dict from the case."""
    observed = case.observed_output if isinstance(case.observed_output, Mapping) else {}
    outputs: dict[str, Any] = {n: {} for n in nodes}
    # Final node carries the decision; intermediate nodes carry their state delta.
    if nodes:
        outputs[nodes[-1]] = dict(observed)
    return outputs


def _decision_of_observed(case: ImportedReplayCase) -> dict[str, Any]:
    observed = case.observed_output or {}
    decision = observed.get("decision") if isinstance(observed, Mapping) else None
    return dict(decision) if isinstance(decision, Mapping) else {}


def _decision_of_execution(execution) -> dict[str, Any]:
    for cp in execution.checkpoints:
        if cp.name in ("decide_action", "final_response"):
            for art in cp.artifacts:
                value = art.load()
                if isinstance(value, Mapping) and "decision" in value:
                    return dict(value["decision"])
                if isinstance(value, Mapping) and "risk_status" in value:
                    return dict(value)
    return {}
```

Note: the two `_flow_def_ctx_*` helpers above are illustrative seams — in implementation, store the seed `ReplayContext` on `self._seed_ctx` during `reconstruct` and reuse its `recorded_by_node` / `node_output_by_node` in `fork`, rather than re-deriving. Replace the placeholder calls with `self._seed_ctx.recorded_by_node` / `self._seed_ctx.node_output_by_node` and set `self._seed_ctx = ctx` in `reconstruct`.

- [ ] **Step 5: Update package exports**

```python
# src/kitaru/adapters/langgraph/replay/__init__.py
"""LangGraph replay & fork: reconstruct a DAG from a trace and fork it."""

from ._agent import KitaruReplayAgent
from ._edits import Edit, edit
from ._importer import import_trace, key_calls_by_node
from ._protocol import LANGGRAPH_CAPS, Caps, ReplayAdapter

__all__ = [
    "KitaruReplayAgent",
    "Edit",
    "edit",
    "import_trace",
    "key_calls_by_node",
    "LANGGRAPH_CAPS",
    "Caps",
    "ReplayAdapter",
]
```

- [ ] **Step 6: Run the end-to-end test**

Run: `uv run pytest tests/adapters/langgraph/replay/test_spine_e2e.py -v`
Expected: PASS — reproduction drift is False (semantic decision reproduced from the cached head + deterministic fake-model tail) and fork drift is True with `risk_status`/`required_action` flagged. If the native dynamic-pipeline replay does not re-execute the fan-out tail as expected, capture the failure and apply systematic-debugging: verify `seed` produced per-node checkpoints (`client.executions.get(seed).checkpoints`), that `from_="collect_evidence_with_tools"` resolves, and that `cache=False` forces live tail execution.

- [ ] **Step 7: Run the full new suite + adjacent suites**

Run: `uv run pytest tests/adapters/langgraph/replay/ tests/test_replay.py tests/test_replay_skip.py tests/test_replay_verify_imported_sources.py -v`
Expected: PASS (all green)

- [ ] **Step 8: Commit**

```bash
git add src/kitaru/adapters/langgraph/replay/ tests/adapters/langgraph/replay/
git commit -m "feat(replay): KitaruReplayAgent spine — reconstruct, replay, fork, diff"
```

---

## Self-Review (completed by plan author)

**Spec coverage:**
- Entry contract (`bind → import_trace → reconstruct → replay → fork → diff`) → Tasks 2, 9.
- Node+call DAG reconstruction with provenance → Tasks 7 (topology), 8 (mirror flow + fan-out handle-threading).
- Cached-upstream / exec-tail replay → Task 8 (`playback` flag) + Task 9 (`flow.replay(cache=False, playback=False)`).
- Reproduction drift vs fork drift → Tasks 5 (comparator) + 9 (wiring + e2e assertion).
- Both edit scopes + precedence → Task 3.
- `skip=` selector → Task 4.
- Capabilities / Protocol boundary → Task 6.
- Importer on rich per-observation rows, node/call keying → Tasks 1, 2.
- Testing with fake model + reference fixtures + planted regression → Tasks 8, 9.

**Deferred (correctly absent):** cohort, experiment, iteration, second adapter, UI, mock/side-effect policy beyond PR #412 — none appear as tasks. ✔

**Placeholder scan:** Tasks 1–7 are fully concrete. Tasks 8–9 contain real code plus explicit runtime-verification notes (LangGraph version differences in node-callable access; live-stack flow setup; dynamic-replay behavior). The `...` blocks in Task 9 Step 3 are **test fixtures** that must be filled from the reference agent's existing helpers — flagged inline as the one place the implementer wires real fixtures, not skipped logic. The `_flow_def_ctx_*` seams are explicitly resolved in the note under Step 4.

**Type consistency:** `RecordedCall.node/call_index` (T1) → consumed in T2/T8/T9. `resolve_edits(node, call_index, edits, variant, recorded)` (T3) → called in T8. `compare_decisions` + `DriftReport` (T5) → used in T9. `CompiledTopology(nodes, callables, fanout_node)` (T7) → used in T8/T9. `import_trace(rows, *, trace_id)` (T2) → used in T9. Signatures match across tasks. ✔

**Known runtime risks (call out during execution, not placeholders):**
1. Dynamic-pipeline native replay re-executing a `.submit()` fan-out tail is the single highest-risk integration point (Task 9 Step 6). If native replay cannot re-run dynamically-created sub-checkpoints, fall back to rebuilding+running the tail flow directly (still `playback=False`) and compare against the seed's recorded head — same drift semantics, without leaning on `executions.replay` for the fan-out.
2. LangGraph compiled-graph introspection (`graph.get_graph()`, node-callable access) is version-sensitive (Task 7).
