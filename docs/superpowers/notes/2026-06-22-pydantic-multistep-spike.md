# PydanticAI Multi-Step Replay Spike

**Date:** 2026-06-22
**Branch:** feat/langgraph-replay-fork
**Spike scripts:** `/tmp/pa_multistep_spike.py`, `/tmp/pa_multistep_spike2.py`, `/tmp/pa_override_verify.py`
**Locking test:** `tests/test_pydantic_replay_fork.py::test_multistep_replay_from_intermediate_step`

---

## 1. Multi-step structure: which approach works

### Attempt (a) — ONE agent with 2 model calls (tool + model), calls-strategy

Checkpoint graph produced (tool `gather_info`, single agent `two_call_agent`):

```
two_call_agent_model_request   (initial turn)
gather_info_tool               (tool call)
two_call_agent_model_request_2 (second model turn)
```

All three are **terminal sibling leaves** in the DAG — none is downstream of another.
`_MultipleTerminalStepsOutputError` fires on `.wait()`.

**Verdict:** NOT viable. Sibling checkpoints, never a chain. Cannot select one as CUT with a clean head-cached tail.

### Attempt (b) — TWO sequential `KitaruAgent.run_sync` calls in one `@flow`

```python
@flow(cache=False)
def two_step(prompt: str) -> dict:
    gather_out = gather_wrapped.run_sync(prompt).output
    decide_out = decide_wrapped.run_sync(f"decide based on: {gather_out.triage}").output
    return {...}
```

Checkpoint names produced: `['gather_agent_model_request', 'decide_agent_model_request']`

Both are again **terminal sibling leaves**. The `decide_agent_model_request` checkpoint does NOT receive `gather_agent_model_request`'s output as an upstream input — each `KitaruAgent.run_sync` call creates its own self-contained checkpoint, and the intermediate Python variable (`gather_out`) never crosses a checkpoint boundary. Same `_MultipleTerminalStepsOutputError`.

**Verdict:** NOT viable for replay-from-intermediate. Appears chained from names but is NOT in the DAG.

### Attempt (b2) — TWO explicit `@checkpoint` functions in one `@flow` ✓

```python
@checkpoint
def gather_step(prompt: str) -> dict:
    return raw_gather_agent.run_sync(prompt).output.model_dump()

@checkpoint
def decide_step(triage_result: dict) -> dict:   # ← receives gather_step output
    return raw_decide_agent.run_sync(f"triage={triage_result['triage']}").output.model_dump()

@flow(cache=False)
def b2_flow(prompt: str) -> dict:
    gathered = gather_step(prompt)      # ← result is a ZenML artifact
    decided  = decide_step(gathered)    # ← gather artifact flows into decide input
    return decided
```

Checkpoint names: `['gather_step', 'decide_step']`
DAG: `gather_step` → `decide_step` (chained; decide_step depends on gather_step output artifact)
Single terminal: `decide_step`. Clean `.wait()` returns `{'verdict': 'escalate'}`.

**Verdict: VIABLE — this is the chosen structure.**

Note: Inside each `@checkpoint`, use a plain `pydantic_ai.Agent` directly (not `KitaruAgent`). `KitaruAgent` becomes a passthrough inside an explicit `@checkpoint` anyway, so the raw agent is correct. This is also the "minimal changes around my pydantic agent" win: wrap each agent call in a named `@checkpoint`, wire them in a `@flow`.

---

## 2. Replay from the intermediate step

**CUT selector:** `"decide_step"` (the name of the second checkpoint — the terminal step).

```python
fork_handle = fork_flow.replay(base_exec_id, from_="decide_step", cache=False)
```

Observed behavior:
- `Kitaru: Skipping checkpoint gather_step.` — gather served from cache.
- `Kitaru: Checkpoint decide_step started.` — decide re-ran under the fork flow's agent.
- `fork_result = {'verdict': 'escalate'}` — fork agent's output, not base's `'approved'`.

**Lineage assertion confirmed:**
```python
fork_run = client.executions.get(fork_exec_id)
assert fork_run.original_exec_id == base_exec_id  # GREEN
```

---

## 3. Reconfiguring the FIRST INVOCATION of the CUT step + global config change

### (i) Per-step-invocation reconfiguration via `overrides={"checkpoint.<name>": value}`

`overrides` injects a new output value for a checkpoint, which is then fed into its
downstream consumers as their input. This is NOT the same as re-running that step — the
step is frozen (cached), but its consumers receive the new injected value instead of the
original artifact.

```python
fork_flow.replay(
    base_exec_id,
    from_="decide_step_ov",          # CUT: re-run decide_step_ov
    cache=False,
    overrides={"checkpoint.gather_step_ov": {"triage": "critical"}},  # inject new gather output
)
```

What happens:
1. `gather_step_ov` is skipped (frozen).
2. `decide_step_ov` receives `{"triage": "critical"}` as its `triage_result` input (not the base `"medium"`).
3. `decide_step_ov` re-runs under the fork flow's agent (which has `verdict="reject"`).
4. Result: `{'verdict': 'reject', 'received_triage': 'critical'}` — both overrides apply.

**Verified assertions (all GREEN):**
- `override_result["received_triage"] == "critical"` — injected value reached decide.
- `override_result["verdict"] == "reject"` — fork's global agent model drove the re-run.
- `fork_run.original_exec_id == base_exec_id` — lineage intact.

### (ii) Global config change

"Global config change" = building the fork flow with a different agent (different `TestModel`
or different model name/prompt). The fork flow is a **separate Python object** (different
`decide_inner = Agent(TestModel(custom_output_args={"verdict": "reject"}), ...)`).

When `replay(from_="decide_step")` is called on the fork flow, the fork flow's closure
is what runs — so the fork agent is automatically the "global config change". No special
Kitaru API is needed; swapping the Python agent object is sufficient.

### Combined: per-first-invocation reconfig + global config change

These two mechanisms compose cleanly:
- `from_=CUT` + `cache=False` → re-runs the tail (decides which checkpoints re-execute).
- `overrides={"checkpoint.gather_step": new_value}` → injects a new value for an upstream
  checkpoint's output, feeding it to the re-running step as its input.
- The fork flow's own agent closure → implements the "global config change" (new model, new
  prompt, etc.).

Single combined call:

```python
fork_result = fork_flow.replay(
    base_exec_id,
    from_="decide_step",
    cache=False,
    overrides={"checkpoint.gather_step": {"triage": "critical"}},
).wait()
```

---

## 4. Per-invocation selector

In a linear flow where each `@checkpoint` is called exactly once, the ZenML mapping is:

```
checkpoint.name == checkpoint.invocation_id == function name
checkpoint.call_id == step.id (UUID, e.g. 'a7936f64-bfd7-4571-ac3b-b7e7c7af87dc')
```

All three forms (`name`, `invocation_id`, `call_id`) are valid selectors in `replay(from_=...)` and `overrides={"checkpoint.<selector>": ...}`.

**When a checkpoint is called multiple times in the same flow** (rare but possible — e.g. a loop calling the same `@checkpoint` function twice), `name` and `invocation_id` would be ambiguous. The unambiguous selector in that case is the `call_id` UUID, retrieved via:

```python
from zenml.client import Client as ZenMLClient
from kitaru.replay import _checkpoints as _get_checkpoints

run_zenml = ZenMLClient().get_pipeline_run(exec_id)
for cp in _get_checkpoints(run_zenml):
    print(cp.name, cp.invocation_id, cp.call_id)  # call_id is the stable UUID
```

**First-invocation selector form:** `call_id` UUID string (e.g. `"a7936f64-bfd7-4571-ac3b-b7e7c7af87dc"`). In linear flows, the step `name` is sufficient and human-readable.

---

## Walls hit / caveats

1. **`KitaruAgent` inside `@checkpoint` becomes a passthrough.** Use raw `pydantic_ai.Agent` inside `@checkpoint` bodies — `KitaruAgent` adds no value there. This is documented behavior.

2. **Attempt (b) — two `KitaruAgent.run_sync` in one `@flow` — looks chained but isn't.** The Python variable flowing between them does NOT create a ZenML DAG edge. Both checkpoints are siblings. This is a silent footgun: it works (both checkpoints are recorded) but replay-from-intermediate is not possible.

3. **`overrides={"checkpoint.X": value}` requires a downstream consumer.** If `X` has no steps that consume its output, Kitaru raises `KitaruStateError: Checkpoint override has no downstream consumer`. Always pair an override with `from_=` pointing to a consumer of X.

4. **`wait()` on multi-terminal flows raises `_MultipleTerminalStepsOutputError`.** Approaches (a) and (b) produce multi-terminal graphs. The workaround is explicit `@checkpoint` functions (b2), which ZenML sees as proper DAG edges.

5. **Override injects a value; it does NOT re-run the overridden step.** `overrides={"checkpoint.gather_step": new_value}` freezes `gather_step` and injects `new_value` as its artifact — `gather_step` does NOT re-execute. Only the downstream steps (starting at `from_=`) re-run.

---

## Chosen structure for downstream tasks

```python
from pydantic_ai import Agent
from pydantic_ai.models.test import TestModel
from kitaru import flow
from kitaru.checkpoint import checkpoint

gather_agent = Agent(TestModel(custom_output_args={"triage": "medium"}), ...)
decide_agent = Agent(TestModel(custom_output_args={"verdict": "approved"}), ...)

@checkpoint
def gather_step(prompt: str) -> dict:
    return gather_agent.run_sync(prompt).output.model_dump()

@checkpoint
def decide_step(triage_result: dict) -> dict:
    return decide_agent.run_sync(f"triage={triage_result['triage']}").output.model_dump()

@flow(cache=False)
def analysis_flow(prompt: str) -> dict:
    gathered = gather_step(prompt)
    decided  = decide_step(gathered)
    return decided
```

CUT = `"decide_step"`. Replay + override call:

```python
fork_result = fork_flow.replay(
    base_exec_id,
    from_="decide_step",
    cache=False,
    overrides={"checkpoint.gather_step": {"triage": "critical"}},
).wait()
```
