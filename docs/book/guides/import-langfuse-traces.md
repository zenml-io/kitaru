---
description: Import Langfuse traces from JSONL or the observations API with source attribution, replay evidence, and explicit storage consent.
icon: file-import
---

# Import Langfuse Traces

Kitaru imports Langfuse traces as historical, inspectable executions. You can
read an observations JSONL export or fetch one trace with a
`langfuse://trace/<id>` URI. Each trace becomes one execution, and each
observation becomes a checkpoint call.

Every import declares the exact Kitaru Agent and AgentVersion that produced the
trace. Kitaru verifies that declaration before it writes anything. Importing
does not call a model, invoke a tool, run the source application, or execute a
replay.

{% hint style="warning" %}
Imported executions remain read-only historical records. Kitaru refuses to
resume, retry, cancel, or run them through native checkpoint replay. A registered
PydanticAI Agent can use their immutable evidence to create a separate candidate
experiment, as described below. The candidate run never changes the imported
record.
{% endhint %}

## Choose an import source

Use JSONL when you have an export or want to import several traces in a fixed
order:

```bash
kitaru import langfuse langfuse-observations.jsonl \
  --source-project-id <langfuse-project-id> \
  --agent support-agent \
  --agent-version prod
```

A JSONL import requires `--source-project-id` because the export is not trusted
to identify its project.

Use a trace URI to fetch one trace through Langfuse's observations API:

```bash
export LANGFUSE_PUBLIC_KEY=pk-lf-...
export LANGFUSE_SECRET_KEY=sk-lf-...

kitaru import langfuse "langfuse://trace/<trace-id>" \
  --agent support-agent \
  --agent-version prod
```

Install the optional SDK with `kitaru[langfuse]`. Kitaru supports
`langfuse>=4.7.0,<5` for this path. It reads `LANGFUSE_BASE_URL`, then falls
back to `LANGFUSE_HOST`, and otherwise uses Langfuse Cloud. If both URL
variables are set, they must match. A self-hosted URL must use HTTP or HTTPS
and cannot include credentials, a query, or a fragment.

Kitaru fetches all observation pages for the selected trace, derives the
project ID from the response, and rejects empty results, mixed trace IDs, mixed
project IDs, duplicate observations, and traces that exceed bounded page,
observation-count, per-observation canonical-byte, or cumulative canonical-byte
limits. You may pass
`--source-project-id` with a URI as an extra check; it must match the fetched
project. The fetched rows then enter the same normalization, attribution,
integrity, storage, and idempotency path as JSONL rows.

Both commands above are previews. Add `--write --confirm-data-storage` only
after reviewing the destination and evidence summary.

## What Kitaru stores

For every selected trace, Kitaru stores:

- the Langfuse project ID and trace ID;
- the declared source Agent and immutable AgentVersion;
- the observation graph, including parent relationships and source order;
- observation names, kinds, timestamps, and success or error state;
- imported inputs, outputs, message parts, tool parts, model, usage, cost, and
  timing fields when the export contains them;
- the exact selected JSONL rows as raw imported evidence;
- a normalized, versioned replay-evidence bundle; and
- compact source-attribution, integrity, and replay-readiness summaries.

The raw rows and normalized bundle are separate immutable artifacts. Their IDs,
schema versions, and SHA-256 hashes are available through the import result and
`Execution.import_info`. The compact summaries do not contain prompts, tool
arguments, tool results, or other imported payloads.

When Langfuse reports a USD cost, Kitaru records it as historical actual cost.
When cost is absent but model and token counts are available, Kitaru can estimate
cost with the installed `genai-prices` catalog. The estimate is labeled and may
not match the price that applied when the trace originally ran.

{% hint style="danger" %}
An actual import stores the selected raw rows and normalized replay evidence in
your configured Kitaru storage. These values can contain prompts, customer data,
tool arguments, tool results, or secrets. Check access controls, retention, and
deletion requirements before writing an export. Kitaru does not automatically
redact the export.
{% endhint %}

## Source attribution

Kitaru accepts one Agent and one AgentVersion per import request. The AgentVersion
selector must be an exact immutable ID or an exact registered label. There is no
default version.

For each selected trace, Kitaru reports one attribution status:

| Status | Meaning |
|---|---|
| `source_verified` | A supported Langfuse version field exactly matches the declared AgentVersion's full Git SHA or one of its labels, and no supported field conflicts. |
| `caller_attributed` | The export contains no supported version field. Kitaru records the caller's exact AgentVersion declaration without claiming provider verification. |
| `conflict` | A supported version field contradicts the declared AgentVersion, or supported fields disagree with each other. |

Kitaru preserves supported provider fields in the result. It does not accept Git
SHA prefixes, guess labels, or infer an AgentVersion from a trace. If any selected
trace has a source-version conflict, Kitaru writes none of the selected traces.
The conflicting outcomes explain which declaration or export field to correct.

## Replay-readiness meanings

Readiness is reported separately for each capability because one exported trace
can contain a usable root input but lack ordered messages or complete tool
results.

| Capability | `ready` means | Other results |
|---|---|---|
| Root-input candidate rerun | The export contains one unredacted, unambiguous root input. | `unsupported` includes a bounded diagnostic such as `root_input_missing`. |
| Model-message reconstruction | Not reported by the importer. | `unknown` means the necessary fields appear present but have not been validated for execution. `unsupported` means required evidence is absent, redacted, or ambiguous. |
| Tool-result-boundary reconstruction | Not reported by the importer. | `unknown` or `unsupported`, with bounded diagnostics such as `tool_call_without_result`. |
| Recorded-response matching | Not reported by the importer. | `unknown` or `unsupported`, depending on the preserved response evidence. |
| Candidate-tool compatibility | Not reported by the importer. | Always `unknown`; the import does not compare a candidate AgentVersion's tool contract. |

Only root-input readiness can be `ready`. A `ready` result still does not execute
a replay or prove that the declared AgentVersion, its tools, or provider behavior
can reproduce the historical run.

## Run a registered PydanticAI candidate

A registered `KitaruAgent` can create a new experiment from imported PydanticAI
evidence. The candidate runs through the normal Agent path, so its execution,
AgentVersion, scores, protections, lineage, limits, and verdict use the same
durable contracts as other registered-Agent experiments.

Start from the recorded root input when you want the candidate to generate the
whole path again:

```python
from my_agent import support_agent

result = support_agent.replay(
    "<imported-execution-id>",
    imported_mode="root_input",
    on_error="fail",
    idempotency_key="support-root-v1",
    scorers=[quality_score],
)
```

A root rerun is counterfactual. Even when every requested recorded tool response
is served, the default strict policy reports `HOLD`, rather than claiming direct
comparability with the source path.

To continue after a recorded tool result, select one complete boundary from the
immutable evidence:

```python
from kitaru.imports import (
    ImportedReplayBoundary,
    ImportedReplayBoundaryKind,
    load_imported_replay_evidence,
)
from my_agent import support_agent

source_id = "<imported-execution-id>"
evidence = load_imported_replay_evidence(source_id)
tool_result = next(
    part
    for observation in evidence.replay_bundle.observations
    for part in observation.parts
    if part.kind == "tool_result"
)
boundary = ImportedReplayBoundary(
    kind=ImportedReplayBoundaryKind.TOOL_RESULT,
    observation_id=tool_result.observation_id,
    sequence=tool_result.sequence,
    occurrence=tool_result.occurrence,
    call_id=tool_result.call_id,
)

result = support_agent.replay(
    source_id,
    imported_mode="message_history",
    imported_boundary=boundary,
    on_error="fail",
    idempotency_key="support-history-v1",
    scorers=[quality_score],
)
result.assert_pass()
```

Kitaru accepts only complete recorded model-message or tool-result boundaries.
It checks the source and candidate tool contracts before submission, serves
matching recorded responses without calling the live read or write tool, and
blocks every miss. Argument changes, reordered calls, missing occurrences,
schema drift, implementation drift, and scope mismatches cannot fall through to
live tool execution.

Comparability evidence is immutable and separate from scores:

- `recorded_path_comparable` means the selected prefix was complete, every
  eligible recorded response was used, and the candidate did not diverge;
- `counterfactual` identifies a root rerun;
- `degraded` records a blocked call, unused response, or other path divergence;
- `non_comparable` means Kitaru could not establish the recorded-response
  contract.

Missing evidence, incomplete prefixes, unused or blocked recorded responses, and
unaccepted comparability states produce `HOLD`. Complete evidence can still
produce `FAIL` when an objective or protection fails. `PASS` requires both
complete accepted replay evidence and passing scores and protections.

Repeat a completed attempt through its experiment ID to preserve the frozen
source evidence and boundary while applying normal suite limits:

```python
from kitaru.experiments import RegressionLimits

repeated = support_agent.replay(
    experiment=result.spec.experiment_id,
    idempotency_key="support-history-repeat-v1",
    repeats=3,
    scorers=[quality_score],
    limits=RegressionLimits(max_trials=3),
)
```

Inspect attempts without changing them:

```bash
kitaru agents experiments support-agent
kitaru agents experiments support-agent <experiment-id> --output json
```

The SDK `ExperimentRecord` and JSON output include the frozen imported plan,
per-child lineage and recorded-response decisions, aggregate comparability,
scores, protections, limits, and final verdict. They do not include recorded
response values.

## Preview an export

The CLI is read-only unless you pass `--write`:

```bash
kitaru import langfuse langfuse-observations.jsonl \
  --source-project-id <langfuse-project-id> \
  --agent support-agent \
  --agent-version prod
```

The preview resolves and verifies the AgentVersion, reads and normalizes the
export, classifies attribution and readiness, and reports what a write would do.
It does not create executions or store evidence artifacts. Evidence artifact IDs
and schema-version result fields are `None` in preview; hashes and replay-readiness
analysis are still available.

Use JSON output for automation:

```bash
kitaru import langfuse langfuse-observations.jsonl \
  --source-project-id <langfuse-project-id> \
  --agent support-agent \
  --agent-version prod \
  --output json
```

The result includes the Agent Project and AgentVersion IDs, the registered
workflow name in `flow_name`, the requested label when present, outcome and
attribution counts, storage details, optional cohort tag, non-secret URI fetch
provenance in `fetch_provenance`, and one outcome per selected trace. Each outcome includes attribution, supported
provider fields, evidence hashes, capability-specific readiness, diagnostics,
and an execution ID when one is available.

### Choose storage explicitly

Use `--stack` to select the stack that stores imported payloads and evidence:

```bash
kitaru import langfuse langfuse-observations.jsonl \
  --source-project-id <langfuse-project-id> \
  --agent support-agent \
  --agent-version prod \
  --stack production-cloud
```

The selector accepts an exact stack name or ID in the Agent Project. If you omit
it, Kitaru uses the active stack and prints a warning. Selecting a stack for an
import does not change your active stack.

{% hint style="warning" %}
When Kitaru is connected to a shared server but the selected stack uses local
storage, execution metadata is available on the server while evidence remains on
the importing machine. The shared UI may be unable to load those artifacts. Use
remotely accessible storage when imported evidence must be shared.
{% endhint %}

## Select and label a cohort

Repeat `--trace-id` to select exact traces. Kitaru preserves that order and
applies `--limit` after selection:

```bash
kitaru import langfuse langfuse-observations.jsonl \
  --source-project-id <langfuse-project-id> \
  --agent support-agent \
  --agent-version prod \
  --trace-id trace-a \
  --trace-id trace-b \
  --limit 2 \
  --cohort-tag customer-a
```

Without `--trace-id`, every trace in the export is selected. The optional cohort
tag is attached to every imported execution. It must contain 1 to 64 letters,
numbers, dots, underscores, or hyphens and cannot use a reserved Kitaru prefix.
The tag is immutable for an existing imported trace.

## Write the selected traces

Writing requires both mutation intent and explicit acknowledgement of evidence
storage:

```bash
kitaru import langfuse langfuse-observations.jsonl \
  --source-project-id <langfuse-project-id> \
  --agent support-agent \
  --agent-version prod \
  --trace-id trace-a \
  --write \
  --confirm-data-storage
```

The default processes one trace at a time. After validating a preview, you can
set `--max-workers` from 1 to 8 for bounded concurrency.

### Concurrent writers and interrupted imports

Kitaru gives each active import writer a five-minute lease and refreshes it
before every stored change. A preview or second writer reports a conflict while
that lease is active.

If a writer stops, a later preview reports `would_resume` after the lease
expires. A later write validates the existing execution and immutable evidence,
then resumes the missing work. Kitaru checks lease ownership around each change,
but the backend cannot cancel a write that was already in flight when a lease
expired. Keep one writer active for a source trace whenever possible.

## Import through the SDK

The SDK exposes the same dry-run-first contract:

```python
from kitaru import KitaruClient

client = KitaruClient()
source = "langfuse-observations.jsonl"
source_project_id = "<langfuse-project-id>"
# To fetch one trace instead:
# source = "langfuse://trace/<trace-id>"
# source_project_id = None

preview = client.imports.langfuse(
    source,
    source_project_id=source_project_id,
    agent="support-agent",
    version="prod",
    stack="production-cloud",
    trace_ids=["trace-a", "trace-b"],
    cohort_tag="customer-a",
)
print(preview.attribution_counts)
print(preview.fetch_provenance)  # None for JSONL; query details for trace URIs.
print(preview.outcomes[0].replay_readiness)

result = client.imports.langfuse(
    source,
    source_project_id=source_project_id,
    agent="support-agent",
    version="prod",
    stack="production-cloud",
    trace_ids=["trace-a", "trace-b"],
    cohort_tag="customer-a",
    dry_run=False,
    confirm_data_storage=True,
)

for outcome in result.outcomes:
    print(outcome.trace_id, outcome.attribution.status, outcome.execution_id)
```

After a write, inspect typed provenance without parsing metadata keys:

```python
execution_id = result.outcomes[0].execution_id
assert execution_id is not None
execution = client.executions.get(execution_id)
info = execution.import_info
assert info is not None
assert info.raw_evidence is not None
assert info.replay_readiness is not None

print(info.source_agent_version_id, info.source_agent_version_label)
print(info.attribution.status)
print(info.raw_evidence.artifact_id, info.raw_evidence.sha256)
print(info.replay_readiness.root_input_candidate_rerun.status)
```

## Integrity and outcomes

Kitaru classifies each normalized observation graph before writing:

| Integrity | Meaning | Default behavior |
|---|---|---|
| `complete` | The observations form one connected graph with no missing parent. | Accepted. |
| `root_omitted` | One parent is missing, commonly because the export omitted the trace root. | Accepted, with the gap recorded. |
| `fragmented` | The export has multiple disconnected components or more than one missing parent. | Rejected unless you pass `--allow-fragmented` or `allow_fragmented=True`. |
| `invalid` | The observation graph contains a cycle. | Always rejected. |

Allowing a fragmented trace records the available components. It does not infer
or repair missing relationships. Kitaru also rejects observations without a
terminal source status. Export the trace again after Langfuse records its end
time, then retry.

| Outcome | Meaning |
|---|---|
| `would_create` | Preview found no attributed execution with this source identity. |
| `would_resume` | Preview found an interrupted matching import that a write can continue. |
| `created` | A new imported execution and both evidence artifacts were stored. |
| `resumed` | A matching interrupted import was completed. |
| `unchanged` | The same source identity, AgentVersion, stack, content, evidence, and cohort tag already has a complete import. |
| `conflict` | An immutable identity or evidence field differs from the existing import. |
| `rejected` | The trace or request violates an import safety rule. |
| `failed` | A backend operation failed for that trace. |

The stable external identity is provider, source project ID, and trace ID. The
declared AgentVersion, stack, normalized content, raw evidence, replay bundle,
and cohort tag are immutable conflict checks. Kitaru never overwrites or
reattributes historical evidence. Inspect `existing_execution_id`, `reason`, and
`resolution`, then retry with the original values or remove the existing
execution and artifacts before intentionally creating a replacement.

The CLI exits non-zero after rendering all outcomes if any trace is `conflict`,
`rejected`, or `failed`.

## Legacy imports

Imports created with the earlier storage format remain readable and unchanged.
Their typed attribution status is `legacy_unattributed` because Kitaru cannot
reliably infer which AgentVersion produced them. Their inspection projection
does not invent source labels, evidence artifacts, or readiness results.

Importing the same source again with an explicit Agent and AgentVersion creates
the attributed representation beside the legacy record. It does not rewrite or
relabel the older execution.

## Inspect imported executions

Use the normal execution interfaces after import:

```bash
kitaru executions get <execution-id>
```

Or through Python:

```python
execution = client.executions.get("<execution-id>")
info = execution.import_info
assert info is not None
print(info.attribution.status)
for checkpoint in execution.checkpoints:
    print(checkpoint.name, checkpoint.status)
```

An error on an imported root or Agent observation marks the execution as failed.
A failed child tool observation remains visible as a failed checkpoint without
necessarily changing the overall execution status.
