---
description: Inspect execution status, fetch runtime logs, resolve waits, and manage lifecycle actions
icon: list-check
---

# Manage Executions with KitaruClient and CLI

`KitaruClient` is the programmatic API for inspecting and acting on executions
outside your flow functions. It is how you look up a real run by its `exec_id`,
read its checkpoints and cost, resolve waits, and drive the run/replay/improve
loop, replaying a recorded execution from a checkpoint with one input changed.
The same surface is exposed over the CLI and the MCP server, so a coding agent
can do all of this too.

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

The text form of `kitaru executions get` shows every public checkpoint call:

```text
Kitaru execution
  Execution ID: kr-a8f3c2
  Flow: content_pipeline
  Status: failed

Checkpoints
  Call ID call-research-1: research (completed)
  Call ID call-write-2: write (failed); original call ID: call-write-1 (may identify a call in the source execution; not a --at selector for this execution)

Replay into a new execution
  Command: kitaru executions replay kr-a8f3c2 --at call-write-2
```

Use the current call ID after `Call ID` with `kitaru executions replay --at`.
This identifies one specific call when a checkpoint name appears more than once.
An `original call ID` records replay history. It may identify the corresponding
call in the source execution, but it is not a valid `--at` selector for the
execution currently displayed. For scripts, use `--output json` rather than
parsing the human-readable layout.

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

Use execution statistics when you want counts, trends, or health checks across a
cohort without fetching every execution first, the difference between "show me
the last 20 executions" and "how many failed this week?" Kitaru runs the
aggregate query on the active runtime and returns a small grouped result. Each
group includes an execution count; you can also request numeric metrics such as
average duration or the sum of a numeric metadata key (e.g. cost per flow), which
is how you compare a cohort before and after a change.

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
kitaru executions statistics --group-by time:day --group-by status --output json
kitaru executions statistics --group-by status -o json

# Second page of grouped status results
kitaru executions statistics --group-by status --page 2 --size 20

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

Statistics JSON output uses the shared `--output json` / `-o json` option.
When `--page` or `--size` is used, `group_count` is the number of groups in the
current response page. Like other Kitaru CLI JSON outputs, statistics JSON does
not include separate pagination metadata. `--max-groups` limits the total groups
returned by the statistics query before CLI pagination is applied.

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
adapters, Kitaru records canonical `llm_usage_v1` metadata on the checkpoint
that made or reused the provider work. One usage record usually means one
provider interaction or one adapter-level graph/agent invocation, depending on
which adapter produced it. When the execution finishes, Kitaru reads those
checkpoint records and writes two execution-level views. Checkpoint-owned records
expose the physical attempt ID as `checkpoint_id` and the normalized checkpoint
name as `checkpoint_name` when the producer did not set those fields itself.
This keeps retry attempts distinct. Records written only at execution level stay
unattributed unless their producer supplied checkpoint identity.

`FlowHandle.wait()` and `FlowHandle.get()` can populate missing summaries for
older executions or executions where the finish-time summary was not written:

- `llm_usage_summary_v1` is the inspection view. `kitaru executions get` and the
  Python client parse it into `execution.llm_usage_summary`. It tells you what
  happened in one execution. Its `usage_record_count`,
  `incurred_usage_record_count`, and `reused_usage_record_count` fields count
  Kitaru usage records, not raw provider API calls.
- Common LLM totals are available as execution-statistics shortcuts:
  `llm_display_cost`, `llm_estimated_cost`, `llm_total_tokens`, and
  `llm_incurred_tokens`. These shortcuts read the flat execution-level numeric
  metadata that Kitaru writes for statistics, so users do not need to spell the
  internal `_v1` metadata keys for common cost and token totals.

Cost fields are intentionally split:

- `actual_cost_usd` means the provider reported a final cost for this exact
  call. Treat this as observability, not as a billing invoice.
- `estimated_cost_usd` means Kitaru or an SDK calculated a cost from token counts
  and pricing data. Direct `kitaru.llm()` calls and framework adapters write this
  field automatically when Kitaru has reliable provider, model, and token data
  that `genai-prices` can price. Claude Agent SDK `total_cost_usd` is also
  recorded here because it is an SDK-side estimate, not a provider invoice line.
  Adapter-level user calculators also write this field.
- `display_cost_usd` uses actual cost for a record when present, otherwise
  estimated cost. Treat it as observability, not as a billing invoice.

Aggregation follows the `non_reused_is_incurred_v1` policy recorded in each
summary's `cost_policy` field. Every usage record counts as incurred unless its
`billing_effect` is explicitly `reused_not_incurred`. An `unknown` record
therefore contributes to workload and incurred counts and tokens. Its price
contributes to display cost when available, and a missing price makes incurred
cost incomplete. Mock mode keeps `billing_effect="unknown"` because it does not
claim that a provider call occurred, but summaries still apply this conservative
rule.

Kitaru does not eagerly rewrite all stored summaries after this policy changes.
A summary with any other policy marker is treated as stale and is refreshed when
Kitaru next performs its normal execution aggregation.

Automatic `genai-prices` cost estimates are on by default. After the provider
or adapter call succeeds, Kitaru sends the known provider, model name, and token
usage to [`genai-prices`](https://github.com/pydantic/genai-prices), stores the
returned value as `estimated_cost_usd`, and records provenance like this:

```text
cost.source = "calculator"
cost.source_label = "genai-prices"
cost.pricing_version = "genai-prices:<installed package version>"
actual_cost_usd = null
estimated_cost_usd = <calculated USD estimate>
```

The provider or adapter call always comes first. If `genai-prices` is missing,
cannot price the model, returns an invalid value, or cannot read its price data,
Kitaru still returns the model response and records the token counts. The
`llm_usage_v1` record gets a warning and no estimated cost, so the execution
summary increments `records_without_cost_count` for that record.

Kitaru does not guess when a usage record combines multiple models or lacks a
trusted provider/model identity. In that case it records tokens only. User
`cost_calculator=` hooks still take priority over the built-in estimate; if a
user calculator fails, Kitaru records that calculator error instead of hiding it
with a fallback estimate.

To disable automatic `genai-prices` estimates for a process, set:

```bash
export KITARU_LLM_ESTIMATED_COSTS=off
```

You can also set the runtime policy from Python:

```python
import kitaru

kitaru.configure(llm_estimated_costs="off")  # or "auto" to enable again
```

Use `[tool.kitaru] llm_estimated_costs = "off"` in `pyproject.toml` when you want
a repository default. The environment variable and `kitaru.configure(...)` are
useful for temporary opt-out without editing the project file.

{% hint style="warning" %}
`estimated_cost_usd` is not an invoice. It depends on the installed
`genai-prices` package and its price data. Providers can change prices, apply
account-specific discounts, or round bills differently. Use the estimate for
observability and trend analysis, not financial reconciliation.
{% endhint %}

Useful statistics queries:

```bash
# Sum display cost and total token volume for one flow.
# Display cost is an observability number, not an invoice.
kitaru executions statistics \
  --flow content_pipeline \
  --metric llm_display_cost \
  --metric llm_total_tokens

# Sum estimated cost by flow.
kitaru executions statistics \
  --group-by flow \
  --metric llm_estimated_cost

# Sum incurred token volume by day.
kitaru executions statistics \
  --group-by time:day \
  --metric llm_incurred_tokens
```

The shortcut names above mean "sum this common LLM total". The raw
`<name>:metadata:<metadata_key>:<avg|sum|min|max>` form still exists for custom
numeric execution metadata and for advanced internal debugging, but you should
not need `kitaru_llm_*_v1` keys for common LLM cost and token totals.

{% hint style="info" %}
Kitaru normally writes terminal LLM summaries when executions finish, so a
remote execution can get `llm_usage_summary_v1` and the flat `kitaru_llm_*_v1`
statistics keys even if no local SDK process calls `.wait()` or `.get()`. Those
methods can still populate missing summaries for older executions or terminal executions where the finish-time summary was not written.
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

## Replay, retry, cancel, and delete

Replay is the core of the improve loop: it re-executes a recorded run into a
**new** execution from a checkpoint boundary, optionally with inputs changed.
Replaying with no overrides reproduces the baseline; replaying with one thing
changed (a different model or prompt) lets you diff the two and attribute the
difference to your change. Retry, by contrast, resumes the **same** failed
execution in place.

For a failed execution, `kitaru executions get` prints a replay command only
when both the execution ID and a failed checkpoint's current call ID use a
portable format: they must start with a letter or number and contain only
letters, numbers, `.`, `_`, or `-`. This keeps the displayed command safe to
copy as written. If several failed calls qualify, the command uses the first one
in the order returned by Kitaru. The command is omitted for other execution
states, when there are no failed checkpoint calls, or when either ID does not
use this format.

```python
# Replay into a new execution from a checkpoint boundary.
# Override flow inputs (e.g. topic) and prior checkpoint outputs.
replayed = client.executions.replay(
    exec_id,
    at="write_draft",
    flow_overrides={"topic": "New topic"},
    checkpoint_overrides={"research": {"output": "Edited notes"}},
)

# Same-execution retry (failed executions only)
retried = client.executions.retry(exec_id)

# Cancel a running execution
cancelled = client.executions.cancel(exec_id)
```

For the full replay/diff workflow, see the
[ZenML Learn Agents guide](https://docs.zenml.io/user-guides/agents-guide).

### Delete an execution

Cancellation and deletion have different effects. `cancel(...)` asks a running
execution to stop and keeps its record available for inspection. `delete(...)`
removes the execution record.

{% hint style="warning" %}
Deleting an active execution removes its stored metadata without stopping the
underlying workload. That workload can continue consuming compute and may later
fail when it tries to update metadata that no longer exists. Cancel the execution
and wait for it to terminate before deleting it.
{% endhint %}

Delete by execution ID through the client namespace:

```python
client.executions.delete(exec_id)
```

If you already fetched the execution, the model convenience method performs the
same deletion:

```python
execution = client.executions.get(exec_id)
execution.delete()
```

`Execution` is a frozen snapshot. Calling `execution.delete()` does not mutate
that Python object, but refreshing it or fetching the same ID again will fail
once the deletion has succeeded.

The CLI prompts before deleting. Pass `--yes` (or `-y`) when a person has already
approved the deletion and the command runs non-interactively:

```bash
kitaru executions delete kr-a8f3c2
kitaru executions delete kr-a8f3c2 --yes
```

With `--output json`, `--yes` is required. This keeps stdout and stderr
machine-readable by avoiding an interactive prompt in structured-output mode.

MCP callers use the direct mutation tool and receive a minimal acknowledgement:

```text
kitaru_executions_delete(exec_id="kr-a8f3c2")
```

```json
{"exec_id": "kr-a8f3c2", "deleted": true}
```

The MCP tool does not prompt or accept a confirmation argument. The MCP caller
must obtain any required user approval before invoking it.

## Execution convenience methods

`Execution` objects returned by `client.executions.get(...)` also expose
convenience methods that call back into the same client:

```python
execution = client.executions.get(exec_id)

fresh = execution.refresh()          # re-fetch latest state
retried = execution.retry()          # retry a failed execution
resumed = execution.resume()         # resume after wait input
cancelled = execution.cancel()       # cancel a running execution
replayed = execution.replay(at="write_draft", checkpoint_overrides={"research": {"output": "Edited notes"}})
execution.delete()                    # permanently delete the execution record

checkpoints = execution.list_checkpoints()
artifacts = execution.list_artifacts()
```

These are equivalent to calling the corresponding method on
`client.executions`. `refresh`, `retry`, `resume`, and `cancel` return a new
`Execution` snapshot rather than mutating the existing object; `replay` returns
a `ReplaySubmission` describing the new replay execution(s). `delete` returns
`None`, and the frozen snapshot remains unchanged after the stored execution is
deleted.

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
kitaru executions replay kr-a8f3c2 --at write_draft --flow-overrides '{"topic":"New topic"}' --checkpoint-overrides '{"research":{"output":"Edited notes"}}'
kitaru executions retry kr-a8f3c2
kitaru executions cancel kr-a8f3c2
kitaru executions delete kr-a8f3c2 --yes
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
- `kitaru_executions_abort_wait(exec_id=..., wait=...)`
- `kitaru_executions_resume(exec_id=...)`
- `kitaru_executions_delete(exec_id=...)`
- `get_execution_logs(exec_id=...)`
- `kitaru_artifacts_get(artifact_id=...)`
- `kitaru_status()`

Use the pending-wait name or ID shown by the execution, or already known by the
workflow. Resolve that wait with `kitaru_executions_input` or explicitly abort
it with `kitaru_executions_abort_wait`, then inspect the returned execution's
`status` and singular `pending_wait`. If `pending_wait` is absent but the
execution does not continue, call `kitaru_executions_resume` (for example,
because the original runner exited).

Aborting a wait resolves only that selected wait; it does not cancel the whole
execution. The input and abort-wait tools do not call resume themselves, though
an existing runner may continue after the wait is resolved.

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
