# PydanticAI Fork-Spike Findings

**Date:** 2026-06-22
**Branch:** feat/langgraph-replay-fork
**Spike script:** `/tmp/pa_spike.py`
**Locking test:** `tests/test_pydantic_replay_fork.py::test_fork_by_replay_reexecutes_tail_under_new_agent`

---

## CUT checkpoint selector

The decision checkpoint name for a `KitaruAgent(agent, checkpoint_strategy="calls")` run follows the pattern:

```
{agent_name}_model_request
```

For an agent named `"spike_agent"`, the CUT is `"spike_agent_model_request"`.

This is the single checkpoint created per model call by `KitaruModel.request()` in
`kitaru/adapters/pydantic_ai/_model.py` (line ~589):

```python
checkpoint_name = f"{self._agent_name}_model_request"
response = await run_async_in_checkpoint(
    config=with_default_type(self._checkpoint_config, "llm_call"),
    step_name=checkpoint_name,
    ...
)
```

**Observed checkpoint names (spike run):**

```
['spike_agent_model_request']
```

Only one checkpoint for a simple prompt-only turn (no tools, single model call).
With tools (`call_tools=["add"]`), additional `{agent_name}_{tool_name}` checkpoints would appear.

---

## A/B Verdict: **A**

**Mechanism A works.**

`flow.replay(baseline_exec_id, from_=CUT, cache=False)` with a separately-constructed
fork flow (wrapping a different `TestModel`) **re-ran the tail under the fork agent**
and produced the fork agent's output (`risk_status="safe"`) instead of the baseline's
(`risk_status="needs_review"`).

```
FORK-BY-REPLAY WORKED: ModelResponse(parts=[ToolCallPart(tool_name='final_result',
  args={'risk_status': 'safe', 'required_action': 'x'}, ...)])
VERDICT: A
```

---

## Result shape note

`fork_flow.replay(exec_id, from_=CUT, cache=False).wait()` returns a `ModelResponse`
(the raw PydanticAI model output stored by the calls-strategy checkpoint), **not**
the `dict` returned by the `@flow` body. This is because with `checkpoint_strategy="calls"`,
the `{agent_name}_model_request` checkpoint is ZenML's terminal step and its artifact
is what `_extract_flow_result` returns.

**Downstream tasks should recover the decision by inspecting `ModelResponse.parts`
args or by reading checkpoint artifacts** rather than relying on the flow's body
return value.

---

## Files changed

- `/tmp/pa_spike.py` — scratch spike script (not committed)
- `tests/test_pydantic_replay_fork.py` — locking test (GREEN)
- `docs/superpowers/notes/2026-06-22-pydantic-fork-spike.md` — this file

---

## Concerns for downstream tasks

1. **Flow body return value is shadowed by calls-strategy.** The `@flow` body's
   `return wrapped.run_sync(prompt).output.model_dump()` is never the terminal
   step output; the `_model_request` checkpoint is. Tasks 3+ should either:
   - Wrap the `KitaruAgent` call inside an explicit `@kitaru.checkpoint` so the flow
     has a single terminal step with the dict output, OR
   - Read back the decision from `KitaruClient().executions.get(exec_id)` checkpoint
     artifacts.

2. **For real agents with tools:** additional `{agent_name}_{tool_name}` checkpoints
   appear between turns. The CUT must be the **last** checkpoint (the final model call
   that produces the structured output), resolved per-execution as `run.checkpoints[-1].name`.

3. **Agent name collision across test runs:** stable names like `"forktest_agent"` should
   use `uuid` suffixes in production to avoid cross-test ZenML source-alias collisions.
   The locking test uses a stable name intentionally (to prove the selector pattern).

---

## Lineage assertion (2026-06-22 follow-up)

**Lineage field:** `Execution.original_exec_id` (`src/kitaru/_client/_models.py:493`)

After the fork-by-replay completes, the replay `Execution` object has `original_exec_id`
set to the base execution's ID. This links a replay to its source at the metadata level
and is set unconditionally by the ZenML pipeline-run mapper
(`src/kitaru/_client/_mappers.py:617-640`).

**The locking test now asserts:**
```python
fork_exec = client.executions.get(fork_exec_id)
assert fork_exec.original_exec_id == base_exec_id
```

This assertion is NON-VACUOUS: if the fork degraded to a fresh run (no replay lineage),
`original_exec_id` would be `None` and the test would fail.

**RED evidence:** With `== "bogus-id"`, pytest reported:
```
FAILED — AssertionError: Replay lineage broken: expected original_exec_id='42f8843b-...', got original_exec_id='42f8843b-...'.
assert '42f8843b-3ea4-49d2-bfd3-bcafc8662ad9' == 'bogus-id'
```
The assertion correctly failed; restored to `== base_exec_id` → GREEN (1 passed, 10.66s).
