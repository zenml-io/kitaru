---
description: Pause flows for human or agent input, then resume from where they left off.
icon: hourglass-half
---

# Wait, Input, and Resume

`kitaru.wait()` suspends a running flow until a human, another agent, or an
external system provides input. It exists because durable runs let a flow stop
and resume without losing state: when execution hits a wait, the server holds the
run's checkpoints and the runner can release compute. The execution resumes
seconds, hours, or months later when input lands, picking up at the exact wait
point with the same state and artifacts. In non-interactive runs the runner polls
for input up to its `timeout` (default 600 seconds), then exits.

## The wait/resume timeline

<figure><img src="https://assets.kitaru.ai/docs/diagrams/wait-resume.png" alt="The server holds durable state while compute is idle, then the runner resumes at the exact wait point when input lands."><figcaption></figcaption></figure>

The server holds the run's durable state while compute is idle. When input lands,
the runner picks up at the exact point the wait left off, with the same checkpoint
state and artifacts.

## Full example

```python
import kitaru
from kitaru import checkpoint, flow

@checkpoint
def research(topic: str) -> str:
    return kitaru.llm(f"Summarize the latest developments in {topic}.")

@checkpoint
def write_report(summary: str) -> str:
    return kitaru.llm(f"Write a short report based on:\n\n{summary}")

@flow
def research_agent(topic: str) -> str:
    summary = research(topic)
    summary_text = summary.load()

    approved = kitaru.wait(
        name="approve_summary",
        question=f"The agent produced this summary:\n\n{summary_text}\n\nApprove?",
        schema=bool,
    )

    if not approved:
        return "Report rejected by reviewer."

    return write_report(summary)

if __name__ == "__main__":
    research_agent.run(topic="durable execution for AI agents")
```

The flow calls `.load()` only for the human-facing wait question. The original
`summary` checkpoint output is still passed to `write_report(summary)` so Kitaru
keeps the durable data flow between checkpoints intact. See
[In flow bodies](../guides/artifacts.md#in-flow-bodies) for more on when to
materialize checkpoint outputs in orchestration code.

When execution reaches `kitaru.wait()`:

1. The flow suspends and the execution moves to `waiting` status
2. The question, schema, and metadata are recorded on the server
3. The runner polls for input up to its `timeout`. If input arrives first, the
   flow continues in the same process. If the timeout elapses first, the runner
   exits and compute is released; input can still land later, and
   `kitaru executions resume` picks up from exactly this point.

## Providing input

### From the CLI

```bash
# Provide the answer directly (auto-detects the pending wait)
kitaru executions input <exec-id> --value true

# Interactive mode — shows the question and schema, prompts for input
kitaru executions input <exec-id> --interactive

# Sweep all waiting executions interactively
kitaru executions input --interactive
```

### From Python

```python
client = kitaru.KitaruClient()
client.executions.input(
    "<exec-id>",
    wait="approve_summary",
    value=True,
)
```

### From the UI

The UI shows all executions in `waiting` status with the question and expected
schema. You can provide input directly from the UI.

<!-- TODO: add screenshot of UI wait input -->

## Before timeout vs. after timeout

This is an important distinction. The `timeout` parameter (default: 600 seconds)
controls how long the **runner process** keeps polling for input before it exits:

### Input arrives before timeout

The runner is still alive and polling. When you provide input, the flow continues
immediately in the same process. No extra step needed.

### Input arrives after timeout

The runner has already exited and released compute. Your input is recorded on the
server, but there is no running process to pick it up. You need to explicitly
resume the execution:

{% tabs %}
{% tab title="CLI" %}
```bash
# Step 1: provide the input (auto-detects the pending wait)
kitaru executions input <exec-id> --value true

# Step 2: resume the execution (starts a new runner)
kitaru executions resume <exec-id>
```
{% endtab %}

{% tab title="Python" %}
```python
client = kitaru.KitaruClient()

# Step 1: provide the input
client.executions.input("<exec-id>", wait="approve_summary", value=True)

# Step 2: resume the execution
client.executions.resume("<exec-id>")
```
{% endtab %}
{% endtabs %}

{% hint style="info" %}
The `timeout` is not a deadline on the wait itself — the wait never expires. It
only controls how long the runner process stays alive polling for a response.
After timeout, the input can still be provided at any time.
{% endhint %}

## Wait parameters

| Parameter | Default | What it does |
|---|---|---|
| `name` | Auto-generated | Identifier for this wait point (used when providing input) |
| `question` | `None` | Human-readable prompt shown in the CLI, UI, and MCP |
| `schema` | `None` | Expected type of the input. When `None`, the wait acts as a continue/abort gate returning `None`. Pass a type (e.g. `bool`, a Pydantic model) to validate input against it. |
| `timeout` | `600` | Seconds the runner polls before exiting (not a wait expiration) |
| `metadata` | `None` | Additional key-value data attached to the wait record |

## Next steps

* [Wait, Input, and Resume guide](../guides/wait-and-resume.md) — detailed
  patterns including local interactive mode and abort
* [Execution Management](../guides/execution-management.md) — inspect, replay,
  and manage executions
