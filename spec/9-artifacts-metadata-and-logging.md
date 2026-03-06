# 9. Artifacts, Metadata, and Logging

Artifacts and metadata are the substrate that make Kitaru useful for replay, debugging, audit, and dashboard rendering.

They are related, but not the same thing.

- **artifacts** are persisted values
- **metadata** is structured information attached to executions or checkpoints
- **logs** are the mechanism for attaching that metadata

## Artifacts

An **artifact** is a persisted value produced by a durable call.

Artifacts are created implicitly by Kitaru in the normal case, and can also be created explicitly when needed.

### Common artifact producers

| Producer | Artifacts created |
| --- | --- |
| `@kitaru.checkpoint` return value | output artifact |
| `kitaru.llm()` | prompt artifact + response artifact |
| `kitaru.wait()` | input artifact / recorded wait value |
| `kitaru.save()` | explicit named artifact |

## Why artifacts exist

Artifacts are what make these things possible:

- replay
- inspection in the dashboard
- local debugging
- lineage and auditability
- override-based replay
- rich rendering of prompts, responses, JSON, and blobs

Artifacts are not a user burden in the happy path. Most are created automatically.

## Artifact naming

Every artifact gets a name.

By default, checkpoint output artifacts are named after the producing function.

```python
@kitaru.checkpoint
def research(topic: str) -> str:
    ...
```

A successful call produces an output artifact named `research`.

Repeated calls may be auto-indexed for display:

```python
@kitaru.flow
def multi_research(topics: list[str]) -> list[str]:
    results = []
    for topic in topics:
        results.append(research(topic))
    return results
```

In a UI, these may appear as `research`, `research_2`, `research_3`, and so on.

For exact targeting, APIs should use stable IDs rather than relying on display names alone.

## Artifact types

Artifacts should carry a type that helps the dashboard render them appropriately.

For MVP, the useful artifact types are:

| Type | Produced by | What it contains | Dashboard display |
| --- | --- | --- | --- |
| `prompt` | `kitaru.llm()` input | string prompt or messages list | message / prompt view |
| `response` | `kitaru.llm()` output | model text response | markdown / text view |
| `context` | explicit `kitaru.save(..., type="context")` | intermediate data | expandable JSON/text |
| `input` | `kitaru.wait()` | provided input value | structured input view |
| `output` | `@kitaru.checkpoint` return value | checkpoint output | type-aware text / JSON view |
| `blob` | explicit `kitaru.save(..., type="blob")` | binary data | preview or download |

## `kitaru.save()` — explicit artifact creation

Most workflows only need implicit artifacts.

Use `kitaru.save()` when you want to persist additional named values that are not just the return value of a checkpoint.

### Signature

```python
kitaru.save(name: str, value: object, *, type: str = "output", tags: list[str] | None = None)
```

### Rule

`kitaru.save()` should only be valid inside a checkpoint.

That keeps artifact production anchored to a durable work boundary.

### Example

```python
@kitaru.checkpoint
def research(topic: str) -> str:
    raw = kitaru.llm(f"Deep research on {topic}", model="smart")
    kitaru.save("raw_notes", raw, type="context", tags=["debug"])
    summary = kitaru.llm(f"Summarize:\n{raw}", model="fast")
    return summary
```

## `kitaru.load()` — reading artifacts from another execution

`kitaru.load()` lets a checkpoint read an artifact from a previous execution.

### Signature

```python
kitaru.load(exec_id: str, name: str) -> object
```

### Rule

`kitaru.load()` should only be valid inside a checkpoint.

This keeps cross-execution reads visible as lineage edges in durable execution history.

### Example

```python
@kitaru.checkpoint
def build_on_previous(prev_exec_id: str) -> str:
    previous_research = kitaru.load(prev_exec_id, "research")
    return kitaru.llm(
        f"Build on this research:\n{previous_research}\n\nWrite a follow-up analysis.",
        model="smart",
    )
```

## Artifact lineage

Artifacts should retain lineage information such as:

- which checkpoint or call produced them
- when they were produced
- what execution they belong to
- which prior artifacts or inputs fed into that producing call

This is useful for:

- audit trails
- debugging
- replay inspection
- cross-execution dependency tracking

## Artifacts vs call records

This distinction matters.

A successful checkpoint produces both:

- a **call record** saying the durable call completed with a value
- an **artifact** containing that value

A failed checkpoint still produces a call record, even if it does not produce an output artifact.

So:

- artifacts are persisted values
- call records are execution history

Kitaru needs both.

## Execution timeline and call records

A single execution timeline may contain richer detail than just success/failure:

- **failed attempt segments** — a checkpoint or flow attempt that failed before retry
- **retry attempt segments** — the subsequent retry that succeeded or failed again
- **wait entered** — the point where execution suspended
- **wait active timeout reached** — compute/resources released while still waiting
- **resumed continuation** — execution picked up again after input arrived

This is important because retry and resume are same-execution operations. The timeline should show these phases as part of one execution, not as separate executions.

Wait input recorded as an artifact belongs to the **same execution** — it does not imply a new execution was created.

## `kitaru.log()` — metadata logging

`kitaru.log()` attaches structured metadata to the current execution context.

### Signature

```python
kitaru.log(**kwargs)
```

### Context-sensitive behavior

`kitaru.log()` should be valid in two contexts:

- **inside a checkpoint** → metadata attaches to the current checkpoint call
- **inside a flow but outside a checkpoint** → metadata attaches to the execution itself

This keeps the API simple while making examples like flow-level logging well-defined.

## Examples

**Logging inside a checkpoint:**

```python
@kitaru.checkpoint
def generate_summary(text: str) -> str:
    summary = kitaru.llm(f"Summarize:\n{text}", model="fast")
    kitaru.log(quality_score=0.92, source_count=4)
    return summary
```

**Logging at flow level:**

```python
@kitaru.flow
def my_agent(prompt: str) -> str:
    draft = write_draft(prompt)
    kitaru.log(topic=prompt, stage="draft_complete")
    return draft
```

## Metadata types

Some metadata keys are especially useful because the dashboard can aggregate or render them specially.

| Type | Example keys | Typical source |
| --- | --- | --- |
| Cost | `cost_usd`, `cost_input`, `cost_output` | `kitaru.llm()` |
| Usage | `tokens`, `tokens_input`, `tokens_output`, `model` | `kitaru.llm()` |
| Timing | `latency_ms`, `queue_time_ms` | runtime or `kitaru.llm()` |
| Quality | `quality_score`, `confidence`, `relevance` | user code |
| Debug | `error`, `retry_reason`, `fallback_model` | runtime or user code |
| Custom | anything else | user code |

Unknown keys should still be allowed as custom metadata.

## Metadata behavior

- values should be JSON-serializable
- multiple `log()` calls should merge metadata rather than replace earlier entries
- known keys may receive richer dashboard treatment
- unknown keys should still remain visible and queryable

## Dashboard rendering

In the dashboard, artifacts and metadata should work together:

- checkpoints show their artifacts and metadata
- waits show pending or provided input
- LLM calls show prompt/response plus usage and cost
- execution-level metadata can summarize the run
- artifact types influence how values are rendered inline
- failed attempts and retry segments should be visible in the timeline

## Open MVP question: failed-attempt artifacts

One subtle point that should be decided explicitly is what happens to `kitaru.save()` data from a checkpoint attempt that later fails.

Possible behaviors include:

- discard failed-attempt artifacts
- persist them as failed-attempt debug artifacts
- persist them but mark them as not part of the final successful checkpoint output set

The MVP does not need the most sophisticated answer, but it should define one clearly.
