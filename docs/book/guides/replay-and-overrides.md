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

## CLI replay

```bash
kitaru executions replay kr-a8f3c2 \
  --at write_draft \
  --args '{"topic":"New topic"}' \
  --mock-output '{"research":"Edited notes"}'
```

## Cut point (`at`)

`at` selects the replay cut. Checkpoints before `at` are skipped and return recorded outputs. `at` and its downstream descendants re-execute unless mocked via `output=`.

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
| `llm_model=` | Override model alias for `llm_call` checkpoints in the live tail |

### Output mocks

- `output={"lookup_policy": mock_value}` mocks all invocations of that tool (strict when ambiguous).
- `at=<tool_checkpoint>, output={"lookup_policy": mock_value}` mocks only the invocation at the cut when the tool key matches.

### Diff

```python
diff = kitaru.diff("kr-original")
diff = kitaru.diff("kr-original", "kr-replay-a", "kr-replay-b")
```

When replay executions are omitted, `kitaru.diff` discovers all runs whose `original_exec_id` matches the source.

`ExecutionDiff.urls` links to the Kitaru UI compare view — one URL per original-vs-replay pair (the UI compares two executions at a time):

```text
{server}/flows/{flow_id}/v/local/compare?executions={original},{replay}
```

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
uv run pytest tests/test_phase16_replay_example.py
```

For the broader catalog, see [Examples](../getting-started/examples.md).

## Related blog posts

- [Agents need more than traces](https://kitaru.ai/blog/agents-need-more-than-traces)
- [Why agents need durable execution](https://kitaru.ai/blog/why-agents-need-durable-execution)
