---
description: Replay executions from checkpoints with flow and checkpoint overrides
icon: rotate-left
---

# Replay and Overrides

Replay creates a **new execution** from a previous one.

Use replay when you want to keep earlier durable work and rerun only parts of your workflow.

## SDK replay

```python
import kitaru

client = kitaru.KitaruClient()

replayed = client.executions.replay(
    "kr-a8f3c2",
    at="write_draft",
    topic="New topic",
    output={"research": "Edited notes"},
)

print(replayed.exec_id)
print(replayed.original_exec_id)  # points to source execution
```

## Flow-object replay

```python
handle = content_pipeline.replay(
    "kr-a8f3c2",
    at="write_draft",
    topic="New topic",
    output={"research": "Edited notes"},
)

result = handle.wait()
```

## Batch replay (`replay_many`)

Replay many parent executions with the same cut and overrides:

```python
result = content_pipeline.replay_many(
    ["kr-a", "kr-b", "kr-c"],
    at="write_draft",
    model="openai:gpt-5-nano",
    wait=True,
    on_error="collect",
)

for exec_id, handle in result.successes:
    print(exec_id, handle.exec_id)

for exec_id, reason in result.skipped:
    print("skipped", exec_id, reason)
```

Parents whose checkpoint history does not contain `at` land in `ReplayManyResult.skipped`. Load or replay errors are collected in `failures` unless `on_error="fail"`.

`KitaruClient().executions.replay_many(...)` resolves the source flow from the first parent execution and delegates to the same planner.

## CLI replay

```bash
kitaru executions replay kr-a8f3c2 \
  --at write_draft \
  --args '{"topic":"New topic"}' \
  --mock-output '{"research":"Edited notes"}' \
  --skip lookup_policy_tool,write_draft
```

## Cut point (`at`)

`at` selects the replay cut. Checkpoints before `at` are skipped and return recorded outputs. `at` and its downstream descendants re-execute unless mocked via `output=` or listed in `skip=`.

`at` can target:

- checkpoint name (for example `write_draft`)
- checkpoint invocation ID
- checkpoint call ID

If a selector is ambiguous, replay raises `KitaruStateError`.

## Override parameters

| Parameter | Purpose |
|---|---|
| `**flow_inputs` | Override `@flow` parameters (for example `topic=`, `model=`) |
| `input=` | Override checkpoint inputs and force those checkpoints to re-execute |
| `output=` | Mock checkpoint outputs without running tool/LLM code |
| `tool=` | Swap a tool implementation by import path for the replay run |
| `llm_model=` | Override model alias for native `kitaru.llm()` checkpoints in the live tail |
| `skip=` | Force playback for checkpoints that would otherwise re-execute in the live tail |

### Explicit skip (`skip=`)

Use `skip=["checkpoint_name"]` when the replay cut would re-execute a checkpoint but you want its recorded output instead. Downstream checkpoints in the live tail still re-execute and consume the cached output.

You cannot combine `skip=` and `input=` for the same checkpoint.

### Output mocks

- `output={"lookup_policy": mock_value}` mocks all invocations of that tool (strict when ambiguous).
- `at=<tool_checkpoint>, output={"lookup_policy": mock_value}` mocks only the invocation at the cut when the tool key matches.

### Tool swap (`tool=`)

Swap a tool implementation without changing flow code:

```python
support_copilot_flow.replay(
    exec_id,
    at="lookup_policy_tool",
    tool={"lookup_policy": "mocks.lookup_policy"},
)
```

The import path must resolve to a callable with the same signature as the original tool.

### Migration from the previous replay API

| Old | New |
|---|---|
| `from_="write_draft"` | `at="write_draft"` |
| `overrides={"checkpoint.research": "Edited notes"}` | `output={"research": "Edited notes"}` |
| `overrides={"checkpoint.fetch": {...}}` | `input={"fetch": {...}}` |
| Flow parameter kwargs (unchanged) | Still `**flow_inputs` (for example `topic=`) |
| `overrides={"wait.approve": ...}` | Not supported — resolve waits through the normal input flow |

## Diff

Compare an original execution against one or more replays:

```python
execution_diff = kitaru.diff("kr-original")
execution_diff = kitaru.diff("kr-original", "kr-replay-a", "kr-replay-b")
```

When replay executions are omitted, `kitaru.diff` discovers all runs whose `original_exec_id` matches the source.

Cohort diff across many originals:

```python
matrix = kitaru.diff_cohort(["kr-original-a", "kr-original-b"])
for row in matrix.rows:
    print(row.original_exec_id, row.urls)
```

CLI:

```bash
kitaru executions diff kr-original kr-replay-a -o json
kitaru executions diff-cohort kr-a kr-b kr-c -o json
```

`ExecutionDiff.urls` links to the Kitaru UI compare view — one URL listing the original and every compared replay (auto-discovered or explicitly passed):

```text
{server}/flows/{flow_id}/v/{deployment_version}/compare?executions={id1},{id2},{id3}
```

When deployment metadata is absent on the execution, the version segment defaults to `local`.

Use `kitaru.build_compare_url_for_executions(...)`, `kitaru.compare_url_for_executions(...)`, or `kitaru.build_compare_url(...)` when constructing links yourself.

## Waits during replay

Replay does not support overriding or pre-populating wait results. If a replayed execution reaches a `wait()` during normal execution, resolve it through the normal wait input flow:

- SDK: `client.executions.input(exec_id, wait="name", value=...)`
- CLI: `kitaru executions input <exec_id> --value ...`

## Divergence

Replay can raise `KitaruDivergenceError` when the backend detects that durable call sequence compatibility is broken.

## Example in this repository

```bash
uv sync --extra local
uv run python examples/features/replay/replay_with_overrides.py
uv run python examples/end_to_end/pydantic_replay_fork/demo.py run-all
uv run pytest tests/test_phase16_replay_example.py
```

For the broader catalog, see [Examples](../getting-started/examples.md).

## Related blog posts

- [Agents need more than traces](https://kitaru.ai/blog/agents-need-more-than-traces)
- [Why agents need durable execution](https://kitaru.ai/blog/why-agents-need-durable-execution)
