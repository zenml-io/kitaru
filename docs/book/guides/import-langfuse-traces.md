---
description: Import a Langfuse observations JSONL export as inspectable Kitaru executions, with a read-only preview and explicit data-storage consent.
icon: file-import
---

# Import Langfuse Traces

Kitaru can import a Langfuse **observations JSONL export** as historical
executions. Each source trace becomes one execution, and each observation in
that trace becomes a checkpoint call. Different traces can have different
checkpoint graphs, so dynamic agent runs do not need to share one fixed shape.

Importing does not call a model, invoke a tool, or run source application code.
It records the exported history so you can inspect it through the Kitaru UI,
SDK, and execution CLI.

{% hint style="warning" %}
Imported executions are historical synthetic records. They are not executable
flow snapshots and cannot currently be replayed, resumed, or retried. Import is
the inspection foundation for external traces, not yet a reconstruction or
re-execution mechanism.
{% endhint %}

## What Kitaru stores

For every selected trace, Kitaru stores:

- the source identity: Langfuse project ID and trace ID;
- the observation graph, including parent relationships;
- observation names, kinds, timestamps, and success/error state;
- full observation inputs and outputs;
- compatible model, token-usage, latency, and USD cost information; and
- an integrity classification describing gaps in the exported graph.

The `agent_name` you provide groups the imported executions under one Kitaru
flow name. It does not need to correspond to executable Python code.

{% hint style="danger" %}
An actual import persists full input and output payloads in your configured
Kitaru storage. These values can contain prompts, customer data, tool
arguments, tool results, or secrets. Check access controls, retention, and
deletion requirements before writing an export. Kitaru does not automatically
redact the export.
{% endhint %}

## Preview an export

The CLI is read-only unless you pass `--write`:

```bash
kitaru import langfuse langfuse-observations.jsonl \
  --source-project-id <langfuse-project-id> \
  --agent-name support-agent
```

The preview reads and normalizes the export, checks each selected trace against
the current Kitaru project, and reports what a write would do. It does not
create executions or store payloads.

Use JSON output for automation:

```bash
kitaru import langfuse langfuse-observations.jsonl \
  --source-project-id <langfuse-project-id> \
  --agent-name support-agent \
  --output json
```

The result contains aggregate `counts` plus one outcome per selected trace.

## Select a small cohort

Repeat `--trace-id` to select exact traces. Kitaru preserves that order and
applies `--limit` after selection:

```bash
kitaru import langfuse langfuse-observations.jsonl \
  --source-project-id <langfuse-project-id> \
  --agent-name support-agent \
  --trace-id trace-a \
  --trace-id trace-b \
  --limit 2
```

Without `--trace-id`, every trace in the export is selected. Start with a small
cohort before importing a large production export.

## Write the selected traces

Writing requires both mutation intent and explicit acknowledgement that full
payloads will be stored:

```bash
kitaru import langfuse langfuse-observations.jsonl \
  --source-project-id <langfuse-project-id> \
  --agent-name support-agent \
  --trace-id trace-a \
  --write \
  --confirm-data-storage
```

The default is one trace operation at a time. You can increase bounded
concurrency with `--max-workers`, from 1 to 8, after validating the workflow
against your storage and server:

```bash
kitaru import langfuse langfuse-observations.jsonl \
  --source-project-id <langfuse-project-id> \
  --agent-name support-agent \
  --limit 20 \
  --max-workers 4 \
  --write \
  --confirm-data-storage
```

## Import through the SDK

The SDK exposes the same dry-run-first contract:

```python
from pathlib import Path

from kitaru import KitaruClient

client = KitaruClient()
export = Path("langfuse-observations.jsonl")

preview = client.imports.langfuse(
    export,
    source_project_id="<langfuse-project-id>",
    agent_name="support-agent",
    trace_ids=["trace-a", "trace-b"],
)
print(preview.counts)

result = client.imports.langfuse(
    export,
    source_project_id="<langfuse-project-id>",
    agent_name="support-agent",
    trace_ids=["trace-a", "trace-b"],
    dry_run=False,
    confirm_data_storage=True,
)

for outcome in result.outcomes:
    print(outcome.trace_id, outcome.status, outcome.execution_id)
```

## Understand integrity classifications

Langfuse exports can omit observations, especially the root observation. Kitaru
classifies each normalized trace before writing it:

| Integrity | Meaning | Default behavior |
|---|---|---|
| `complete` | The exported observations form one connected graph with no missing parent. | Accepted. |
| `root_omitted` | One parent is missing, which commonly means the export omitted the trace root. | Accepted, with the gap recorded. |
| `fragmented` | The export has multiple disconnected components or more than one missing parent. | Rejected unless you pass `--allow-fragmented` or `allow_fragmented=True`. |
| `invalid` | The observation graph contains a cycle. | Always rejected. |

Allowing a fragmented trace does not repair or infer its missing relationships.
It records the available components and their limitations.

## Interpret outcomes

| Outcome | Meaning |
|---|---|
| `would_create` | Preview found no execution with this source identity. |
| `would_resume` | Preview found an interrupted import that a write can continue. |
| `created` | A new synthetic execution was stored. |
| `resumed` | An interrupted import was completed. |
| `unchanged` | The same source identity and content already has a finished import. Nothing was rewritten. |
| `conflict` | The source identity already exists with different content or a different agent name. |
| `rejected` | The trace or request violates an import safety rule. |
| `failed` | A backend operation failed for that trace. |

The stable identity is the combination of provider, source project ID, and
trace ID. Re-running an identical import is a no-op. Kitaru reports a conflict
instead of silently replacing historical evidence when the same identity has
different normalized content.

The CLI exits non-zero if any selected trace is `conflict`, `rejected`, or
`failed`, after printing all per-trace outcomes. This lets an automation keep
the successful results while still detecting a partial failure.

## Inspect imported executions

Use the normal execution interfaces after import:

```bash
kitaru executions get <execution-id>
kitaru executions list --flow support-agent
```

Or through Python:

```python
execution = client.executions.get("<execution-id>")
print(execution.status)
for checkpoint in execution.checkpoints:
    print(checkpoint.name, checkpoint.status)
```

An error on an imported root or agent observation marks the execution as
failed. A failed child tool observation remains visible as a failed checkpoint
without necessarily changing the overall execution status.

For executable Kitaru-native recordings, see
[Replay and Overrides](replay-and-overrides.md).
