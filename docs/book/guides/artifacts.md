---
description: Persist named values in checkpoints and reuse them across executions
icon: box-archive
---

# Save and Load Artifacts

Kitaru offers two ways to work with artifacts, depending on where your code runs.

{% hint style="info" %}
Use artifacts when you want values tied to a specific execution or checkpoint.
If you need shared application state across executions, keep it in your own
database or service and pass the relevant values into the flow explicitly.
{% endhint %}

## Inside checkpoints

Use `kitaru.save()` and `kitaru.load()` for inline artifact handling:

- `kitaru.save(name, value, ...)` stores a named value in the current checkpoint.
- `kitaru.load(exec_id, name)` reads a named value from another execution's checkpoint.

Both are checkpoint-only. Calling them outside `@checkpoint` raises an error.

## In flow bodies

Checkpoint calls inside a flow body produce durable checkpoint outputs. Flow-body
`kitaru.llm()` calls behave similarly because they run as durable call
boundaries. When you pass one output directly into another checkpoint, Kitaru
keeps the data flow between those checkpoints intact:

```python
summary = research(topic)
return write_report(summary)
```

If the flow body itself needs the actual Python value — for example to build a
human-facing wait question, print a preview, use an f-string, or branch on the
value — materialize it with `.load()` first:

```python
summary = research(topic)
summary_text = summary.load()

approved = kitaru.wait(
    name="approve_summary",
    question=f"Approve this summary?\n\n{summary_text}",
    schema=bool,
)

return write_report(summary)
```

Use the loaded value for flow-body text or control flow. Keep passing the
original checkpoint output to downstream checkpoints unless you intentionally
want to create a new value in the flow body.

## Outside checkpoints

Use the client APIs for browsing artifacts from completed executions:

- `KitaruClient().executions.get(exec_id).artifacts` — browse all artifacts as `ArtifactRef` objects.
- `KitaruClient().artifacts.get(artifact_id)` — get a reference to a specific artifact.

Call `.load()` on any `ArtifactRef` to materialize the actual value:

```python
artifact = KitaruClient().artifacts.get(artifact_id)
value = artifact.load()
```

## Artifact types

Kitaru validates these artifact types:

- `prompt`
- `response`
- `context`
- `input`
- `output`
- `blob`

## Example

```python
from kitaru import checkpoint
import kitaru

@checkpoint
def research(topic: str) -> str:
    notes = f"Research notes about {topic}."
    kitaru.save("research_context", {"topic": topic, "notes": notes}, type="context")
    return notes

@checkpoint
def follow_up(prev_exec_id: str) -> str:
    # Load checkpoint output by checkpoint name.
    previous_notes = kitaru.load(prev_exec_id, "research")
    # Load explicit named artifact.
    context = kitaru.load(prev_exec_id, "research_context")
    return f"{previous_notes} [topic={context['topic']}]"
```

## Name lookup behavior

When loading from an execution:

- checkpoint outputs are addressable by checkpoint name
- manual saved artifacts are addressable by artifact name

If no match is found, `kitaru.load()` raises an error.
If multiple matches are found, `kitaru.load()` raises an ambiguity error so you can choose a unique name.

For custom invocation IDs or heavily dynamic checkpoint naming, prefer explicit
`kitaru.save()` names for stable lookup.

Behavior for artifacts from failed or incomplete source executions follows what
is persisted in runtime storage and may evolve in later releases.

## Try it locally

```bash
uv sync --extra local
uv run examples/features/basic_flow/flow_with_artifacts.py
uv run pytest tests/test_phase8_artifacts_example.py
```

For the broader catalog, see [Examples](../getting-started/examples.md).
