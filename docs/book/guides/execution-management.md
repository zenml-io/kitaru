---
description: Inspect execution status, fetch runtime logs, resolve waits, and manage lifecycle actions
icon: list-check
---

# Manage Executions with KitaruClient and CLI

`KitaruClient` is the programmatic API for managing and inspecting executions
outside your flow functions.

{% hint style="info" %}
`KitaruClient` and the CLI use your current Kitaru connection context. If you
want to inspect executions from a deployed Kitaru server, connect first with
`kitaru login ...` or provide `KITARU_*` connection variables in the current
environment.
{% endhint %}

## Create a client

```python
import kitaru

client = kitaru.KitaruClient()
```

The client uses your current Kitaru connection/project context.

## Inspect a single execution

```python
execution = client.executions.get(exec_id)
print(execution.exec_id)
print(execution.flow_name)
print(execution.status)  # running/waiting/completed/failed/cancelled

if execution.pending_wait:
    print(execution.pending_wait.name, execution.pending_wait.question)

if execution.failure:
    print(execution.failure.origin, execution.failure.message)
```

Execution details include:

- start/end timestamps
- stack name
- summary metadata
- checkpoint calls
- pending wait details (`execution.pending_wait`)
- execution failure details (`execution.failure`) when status is `failed`
- checkpoint retry/failure attempt history (`checkpoint.attempts`)
- artifacts
- frozen execution spec (when available)

## List and query executions

```python
recent = client.executions.list(limit=20)
completed_for_flow = client.executions.list(
    flow="content_pipeline",
    status="completed",
    limit=10,
)
latest = client.executions.latest(flow="content_pipeline")
```

## Execution statistics

Use execution statistics when you want counts, trends, or health checks without
fetching every individual execution first. This is the difference between asking
"show me the last 20 executions" and asking "how many executions failed this
week?" Kitaru sends the aggregate question to the active Kitaru runtime and
returns a small grouped result. Each group always includes an execution count.
You can also ask for numeric metrics, such as average duration or the sum of a
numeric execution metadata key.

```python
from kitaru import KitaruClient

client = KitaruClient()

# One global count for the current project.
stats = client.executions.statistics()
print(stats.groups[0].execution_count)

# Count executions by public Kitaru status.
by_status = client.executions.statistics(group_by=["status"])
for group in by_status.groups:
    print(group.keys["status"], group.execution_count)

# Count daily health by status.
daily_health = client.executions.statistics(group_by=["time:day", "status"])

# Count executions by status and include average run duration per status.
status_with_duration = client.executions.statistics(
    group_by=["status"],
    metrics=["duration_avg:duration:avg"],
)
for group in status_with_duration.groups:
    print(group.keys["status"], group.execution_count, group.metrics["duration_avg"])

# Sum a numeric execution metadata key by flow.
cost_by_flow = client.executions.statistics(
    group_by=["flow"],
    metrics=[
        {
            "name": "cost_usd_sum",
            "source": "metadata",
            "aggregation": "sum",
            "metadata_key": "cost_usd",
        }
    ],
)

# Count one flow's failures by stack.
flow_failures = client.executions.statistics(
    group_by=["stack", "status"],
    flow="content_pipeline",
    status="failed",
)
```

The CLI exposes the same surface:

```bash
# One global count
kitaru executions statistics

# Failure/success mix for the current project
kitaru executions statistics --group-by status

# Failure/success mix with average run duration per status
kitaru executions statistics --group-by status --metric duration_avg:duration:avg

# Daily execution health, script-friendly JSON
kitaru executions statistics --group-by time:day --group-by status -o json

# Sum a numeric execution metadata key by flow
kitaru executions statistics \
  --group-by flow \
  --metric cost_usd_sum:metadata:cost_usd:sum

# A focused question for one flow and two required tags
kitaru executions statistics \
  --group-by status \
  --flow content_pipeline \
  --tag nightly \
  --tag customer-facing
```

Text output is intentionally small:

```text
Kitaru execution statistics
Status      Executions
completed   12
failed      2
running     1
```

When you request metrics, text output adds one column per metric:

```text
Kitaru execution statistics
Status      Executions   Duration Avg
completed   12           43.2
failed      2            18.7
running     1
```

Supported groupings are:

- `status` → public Kitaru status (`running`, `waiting`, `completed`, `failed`, `cancelled`)
- `flow` → `flow_id`
- `stack` → `stack_id`
- `tag` → tag value
- `time:hour`, `time:day`, `time:week`, `time:month`
- `metadata:<key>` → the value stored for that execution metadata key

Supported metric sources are:

- `duration`
- `step_count`
- `cached_step_count`
- `output_artifact_count`
- `metadata:<key>` through a metric spec that sets `source="metadata"` and
  `metadata_key="<key>"`

Supported aggregations are `avg`, `sum`, `min`, and `max`.

CLI metric specs use this format:

- `<name>:<source>:<avg|sum|min|max>` for built-in sources
- `<name>:metadata:<metadata_key>:<avg|sum|min|max>` for metadata

{% hint style="warning" %}
Grouping by `metadata:<key>` includes the matching metadata values in the
statistics output. Only use it for metadata keys whose values are safe to show
to whoever can read the CLI, SDK, or MCP response.
{% endhint %}

{% hint style="warning" %}
Metadata metrics read numeric execution metadata. If the metadata value is
stored as text or as a nested object, the active Kitaru runtime cannot aggregate
it as a number. Store the value as an integer or float when you want to use it
in statistics.
{% endhint %}

### LLM usage and cost metadata

When an execution makes LLM calls through `kitaru.llm()` or the supported agent
adapters, Kitaru records per-call `llm_usage_v1` metadata on the checkpoint that
made or reused the call. When `FlowHandle.wait()` or `FlowHandle.get()` observes
the execution finishing, Kitaru reads those checkpoint records and writes two
execution-level views:

- `llm_usage_summary_v1` is the inspection view. `kitaru executions get` and the
  Python client parse it into `execution.llm_usage_summary`. It tells you what
  happened in one execution.
- Flat numeric metadata keys such as `kitaru_llm_display_cost_usd_v1` and
  `kitaru_llm_total_tokens_v1` are the statistics view. Kitaru execution
  statistics can sum or average these because they are top-level numbers, not
  nested objects.

Cost fields are intentionally split:

- `actual_cost_usd` means the provider reported a cost. Claude Agent SDK exposes
  this via `total_cost_usd`.
- `estimated_cost_usd` means Kitaru used an adapter cost calculator. OpenAI
  Agents and LangGraph can report this when you configure their calculator hook.
- `display_cost_usd` uses actual cost for a record when present, otherwise
  estimated cost. Treat it as observability, not as a billing invoice.

Direct `kitaru.llm()` records token counts and latency, but it does not invent a
cost number. If the provider call does not return a real cost source, cost stays
empty and the execution summary increments `records_without_cost_count`.

Useful statistics queries:

```bash
# Sum display cost by flow. This is an observability number, not an invoice.
kitaru executions statistics \
  --group-by flow \
  --metric llm_display_cost_sum:metadata:kitaru_llm_display_cost_usd_v1:sum

# Sum incurred token volume by day.
kitaru executions statistics \
  --group-by time:day \
  --metric llm_tokens_sum:metadata:kitaru_llm_incurred_total_tokens_v1:sum

# Count calls that reused checkpoint metadata instead of making a new provider call.
kitaru executions statistics \
  --group-by flow \
  --metric llm_reused_calls:metadata:kitaru_llm_reused_call_count_v1:sum
```

{% hint style="warning" %}
In v1, terminal LLM summaries are written when the SDK observes completion via
`FlowHandle.wait()` or `FlowHandle.get()`. A remote execution that finishes but
is never observed through those paths can still have per-checkpoint
`llm_usage_v1` records, but it may not have `llm_usage_summary_v1` or the flat
`kitaru_llm_*_v1` statistics keys yet. `executions.get` stays read-only and does
not backfill missing summaries.
{% endhint %}

Supported filters are `flow`, `status`, `stack`, `tags`, and `max_groups`.
Multiple tag filters mean "executions that have all of these tags". When
`max_groups` truncates a time-grouped result, Kitaru keeps the newest time rows
and still displays the rows from oldest to newest.

{% hint style="info" %}
`flow` and `stack` groupings currently return IDs (`flow_id` and `stack_id`),
not display names. This avoids guessing when a flow or stack has been renamed
or deleted. You can still filter by a flow or stack name.
{% endhint %}

{% hint style="info" %}
The current statistics surface supports grouping by time and metadata, but not
filtering by time range or metadata values yet. If you need "last 7 days" or
"only executions where `customer_tier=enterprise`", fetch/list those executions
separately or add a stable tag for that cohort before querying statistics.
{% endhint %}

Agent and operations summaries should use this same general surface. For
example, an assistant can ask for daily volume first, then drill into only the
unhealthy cohort:

```python
client.executions.statistics(group_by=["time:day"])
client.executions.statistics(group_by=["flow", "status"])
client.executions.statistics(group_by=["stack", "status"], status="failed")
client.executions.statistics(group_by=["metadata:customer_tier", "status"])
```

## Fetch runtime logs

```python
entries = client.executions.logs(exec_id, checkpoint="write_draft", limit=100)
for entry in entries:
    print(entry.timestamp, entry.level, entry.checkpoint_name, entry.message)
```

Runtime log retrieval requires a server-backed connection. For CLI options,
follow mode, grouped output, and retrieval caveats, see
[View Execution Runtime Logs](execution-logs.md).

## Resolve wait input

On local interactive runs, the runtime prompts for input in the same terminal.
For non-interactive or timed-out executions, resolve the pending wait externally:

```python
execution = client.executions.input(
    exec_id,
    wait="approve_deploy",
    value=True,
)
```

If the execution does not continue automatically after input (e.g. the original
runner already exited), call `resume(...)`:

```python
execution = client.executions.resume(exec_id)
```

## Retry, replay, and cancel

```python
# Same-execution retry (failed executions only)
retried = client.executions.retry(exec_id)

# Replay into a new execution from a checkpoint boundary
replayed = client.executions.replay(
    exec_id,
    from_="write_draft",
    overrides={"checkpoint.research": "Edited notes"},
    topic="New topic",
)

# Cancel a running execution
cancelled = client.executions.cancel(exec_id)
```

## Execution convenience methods

`Execution` objects returned by `client.executions.get(...)` also expose
convenience methods that call back into the same client:

```python
execution = client.executions.get(exec_id)

fresh = execution.refresh()          # re-fetch latest state
retried = execution.retry()          # retry a failed execution
resumed = execution.resume()         # resume after wait input
cancelled = execution.cancel()       # cancel a running execution
replayed = execution.replay(from_="write_draft", overrides={...})

checkpoints = execution.list_checkpoints()
artifacts = execution.list_artifacts()
```

These are equivalent to calling `client.executions.retry(exec_id)` etc. — they
return a new `Execution` snapshot rather than mutating the existing object.

## Inspect or abort waits programmatically

List all pending wait conditions for an execution:

```python
waits = client.executions.pending_waits(exec_id)
for w in waits:
    print(w.name, w.question, w.schema)
```

Abort a pending wait instead of continuing it:

```python
execution = client.executions.abort_wait(exec_id, wait="approve_deploy")
```

## Browse and load artifacts

```python
artifacts = client.artifacts.list(exec_id)
for artifact in artifacts:
    print(artifact.name, artifact.kind, artifact.save_type)

context_artifact = client.artifacts.get(artifact_id)
value = context_artifact.load()
```

You can also filter artifact lists:

```python
client.artifacts.list(exec_id, kind="context")
client.artifacts.list(exec_id, producing_call="research")
client.artifacts.list(exec_id, name="research_context", limit=1)
```

## Manage executions from the CLI

```bash
# Inspect and filter executions
kitaru executions get kr-a8f3c2
kitaru executions get kr-a8f3c2 --output json
kitaru executions list
kitaru executions list --status waiting --flow content_pipeline --limit 20
kitaru executions list --status waiting --output json
kitaru executions statistics --group-by status
kitaru executions statistics --group-by time:day --group-by status --output json
kitaru executions logs kr-a8f3c2 --checkpoint write_draft
kitaru executions logs kr-a8f3c2 --output json

# Agent/script-friendly status and stack inspection
kitaru status --output json
kitaru stack list --output json

# Wait-input and lifecycle actions
kitaru executions input kr-a8f3c2 --value true
kitaru executions input kr-a8f3c2 --abort
kitaru executions input kr-a8f3c2 --interactive
kitaru executions input --interactive  # sweep all waiting executions
kitaru executions resume kr-a8f3c2
kitaru executions replay kr-a8f3c2 --from write_draft --args '{"topic":"New topic"}' --overrides '{"checkpoint.research":"Edited notes"}'
kitaru executions retry kr-a8f3c2
kitaru executions cancel kr-a8f3c2
```

## Query executions through MCP

If you want assistant-native tooling (Claude Code, Cursor, etc.), install and
run the MCP server:

```bash
pip install "kitaru[mcp]"
kitaru-mcp
```

Then use tool calls like:

- `kitaru_executions_list(status="waiting")`
- `kitaru_executions_statistics(group_by=["status"])`
- `kitaru_executions_input(exec_id=..., wait=..., value=...)` (MCP requires explicit `wait`)
- `get_execution_logs(exec_id=...)`
- `kitaru_artifacts_get(artifact_id=...)`
- `kitaru_status()`

If the execution does not continue automatically after wait input is resolved
(e.g. the original runner already exited), use the CLI or SDK `resume(...)` call.
MCP does not currently expose a separate resume tool.

See the full setup guide at [MCP Server](../agent-native/mcp-server.md).

## Try the examples

For the broader catalog, see [Examples](../getting-started/examples.md).

```bash
uv sync --extra local
uv run python examples/features/execution_management/client_execution_management.py
uv run pytest tests/test_phase11_client_example.py

uv run python examples/features/execution_management/wait_and_resume.py
uv run pytest tests/test_phase15_wait_example.py

uv run python examples/features/replay/replay_with_overrides.py
uv run pytest tests/test_phase16_replay_example.py

uv sync --extra local --extra mcp
uv run python examples/features/mcp/mcp_query_tools.py
uv run pytest tests/mcp/test_phase19_mcp_example.py
```
