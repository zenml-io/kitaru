---
description: "Import Pydantic Logfire records-query exports into Kitaru: accepted export shapes, how OpenTelemetry GenAI spans become nodes, conversation grouping, and what the importer marks as lossy."
icon: fire
---

# Logfire

If your agent already sends spans to Logfire, you do not need to instrument anything to start using Kitaru. Export the records, run one import, and each conversation lands as a [session](../concepts/agents-and-sessions.md): the same object a live-recorded run produces, ready to evaluate and replay.

**Logfire stays your system of record.** Kitaru takes a runnable copy of the runs you care about, so last Tuesday's incident becomes a test case and last month's traffic becomes a regression population.

Like every import, this one executes on a [worker](../concepts/workers.md) in your environment: the server stores the export blob, your worker parses it. [Import your traces](../getting-started/import-your-traces.md) covers the generic importer contract; this page is the Logfire specifics.

## 1. Export your records

The importer reads rows from Logfire's **records** table, one row per span. Export them as JSON or NDJSON. It accepts a UTF-8 file that is any of:

- **JSONL**, one record row per line.
- **A JSON array** of record rows.
- **A JSON object with a `data` array** of rows.
- **A single JSON object**, treated as a one-row export.
- **The Query API's streaming NDJSON**, where each line is a typed message. `schema`, `explain`, and `end` messages are skipped, rows arrive inside `{"type": "data", "rows": [...]}` (or a single `{"type": "data", "data": {...}}`), and a `{"type": "error"}` message fails the import with the message it carries.

Payloads are capped at 50 MiB per import (the importer's own limit, separate from the server's configurable blob limit). Export in slices as often as you like; [dedup](#re-runs-skip-what-is-already-there) makes overlapping slices safe.

Every row needs `trace_id` and `span_id`; a row without both is reported as a failure and the rest of the file still imports. Beyond those, the importer reads `project_id`, `parent_span_id`, `span_name`, `message`, `kind`, `level`, `start_timestamp`, `end_timestamp`, `otel_status_code` / `status_code`, `otel_status_message`, `is_exception`, `exception_message`, `service_name`, `service_namespace`, `service_version`, `deployment_environment`, `otel_scope_name`, `otel_scope_version`, `tags`, and the `attributes` column:

```json
{
  "project_id": "project-1",
  "trace_id": "trace-1",
  "span_id": "llm",
  "parent_span_id": "root",
  "span_name": "chat claude-haiku-4-5",
  "start_timestamp": "2026-07-22T13:15:00.100000Z",
  "end_timestamp": "2026-07-22T13:15:01Z",
  "service_name": "support-agent",
  "deployment_environment": "production",
  "otel_scope_name": "pydantic-ai",
  "attributes": {
    "gen_ai.operation.name": "chat",
    "gen_ai.conversation.id": "conversation-1",
    "gen_ai.request.model": "claude-haiku-4-5",
    "gen_ai.response.model": "claude-haiku-4-5-20251001",
    "gen_ai.provider.name": "anthropic",
    "gen_ai.usage.input_tokens": 507,
    "gen_ai.usage.output_tokens": 77,
    "operation.cost": 0.000892
  }
}
```

{% hint style="info" %} `attributes` and the payload values inside it are commonly JSON-encoded strings in query output. The importer decodes them, so you don't have to pre-process the file. {% endhint %}

Export whole traces rather than filtered subsets. A span whose parent is missing from the file still imports, but it lands as a root and the session records a warning.

## 2. Import it

Register the agent the traces belong to, if you have not, and start a worker:

```bash
kitaru agent register support-agent --command "python support.py"
kitaru worker start
```

Then import:

```bash
kitaru session import logfire-records.jsonl \
  --importer kitaru/logfire@latest \
  --agent support-agent@latest \
  --media-type application/x-ndjson \
  --tag imported-baseline --wait
```

`kitaru/logfire` is one of the built-in importers registered at server startup, so `@latest` always resolves and there is no importer code to write. Use `--media-type application/json` when you upload a JSON array or a `data` object instead of JSONL or streaming NDJSON.

`--tag` labels every session the import creates, so later commands can select them as a group (`kitaru session evaluate --tag imported-baseline ...`). Tagging happens once the import finishes, which is why it requires `--wait`. The receipt reports sessions `created`, `skipped`, and `failed`, with samples of the failures.

List what landed:

```bash
kitaru session list --agent support-agent --origin imported --imported-from logfire
```

### Importer params

| Param | Meaning |
| --- | --- |
| `source_instance` | Project identity, and half of the session's external id. The importer prefers this, then `project_id`, then each row's own `project_id` column. |
| `project_id` | Alternative spelling of the same fallback, checked after `source_instance`. |
| `join_on` | Dotted path or RFC 6901 JSON Pointer selecting the value that groups traces into one session. Omit it to use the defaults below. See [Grouping traces into sessions](#grouping-traces-into-sessions). |
| `framework` | Extra evidence for framework detection, matched alongside the scope and span names found in the export. |

Pass them with `--params '{"source_instance": "my-logfire-project"}'`, or use the dedicated `--join-on` flag, which accepts a JSON Pointer only (it must start with `/`) and cannot be combined with `join_on` inside `--params`:

```bash
kitaru session import logfire-records.jsonl \
  --importer kitaru/logfire@latest \
  --agent support-agent@latest \
  --join-on '/attributes/customer.case~1id' \
  --media-type application/x-ndjson --wait
```

If no project identity is available from any of those three sources, the importer falls back to `source_instance` `logfire` and says so in the session's warnings.

## 3. Or fetch from the Logfire API

Skip the export and upload, and let the import task fetch records from Logfire directly:

```bash
kitaru session import \
  --importer kitaru/logfire@latest \
  --agent support-agent@latest \
  --since 7d \
  --tag imported-baseline --wait
```

Omitting FILE and setting `--since` selects an API import: the worker calls the Logfire Query API instead of parsing an uploaded payload. `--since` and `--until` accept an ISO 8601 timestamp or a relative duration (`7d`, `12h`, `30m`). `--trace-id` (repeatable) fetches exactly those trace ids instead of a time window. The same selection is a query object on the SDK and REST request:

| Query key | Meaning |
| --- | --- |
| `trace_ids` | Logfire trace ids to fetch. When present, exactly those traces are fetched and the time window is ignored. |
| `since` | Timezone-aware ISO 8601 datetime, lower bound of trace start time. Required when `trace_ids` is absent. Also used as the query's `min_timestamp`. |
| `until` | Timezone-aware ISO 8601 datetime, upper bound of trace start time. Defaults to now. |
| `concurrency` | Traces fetched at once. Defaults to 4. |

The worker needs `kitaru-logfire-importer[adapter]` installed (the default plugin catalog already installs it that way) and `LOGFIRE_READ_TOKEN` in its environment. The token itself carries the Logfire host, so no separate host variable is needed. Each fetched trace is parsed the same way an uploaded export would be, so the node mapping, grouping, and limitations below apply the same way.

## What a trace becomes

Every Logfire record becomes one node, and `parent_span_id` is rebuilt as the node tree, so a tool span nested under a model span stays nested. Node type is read from OpenTelemetry GenAI semantics:

| Logfire record | Kitaru node |
| --- | --- |
| `gen_ai.operation.name` is `execute_tool`, `tool`, or `tool_call`, or a `gen_ai.tool.name` / `tool.name` / `tool_name` attribute is present | `tool_call`, with `tool_name` from that attribute (falling back to the span name) |
| `gen_ai.operation.name` is `chat`, `completion`, `embeddings`, `generate_content`, or `text_completion`, or the record carries `gen_ai.request.model`, `gen_ai.response.model`, or `gen_ai.system` | `llm_call` |
| Everything else | `span` |

Per node, the importer preserves:

- **Inputs**, from the first present of `input`, `inputs`, `raw_input`, `pydantic_ai.all_messages`, `gen_ai.input.messages`, `gen_ai.prompt`, `tool.arguments`, `gen_ai.tool.call.arguments`.
- **Outputs**, from the first present of `output`, `outputs`, `final_result`, `gen_ai.output.messages`, `gen_ai.completion`, `tool.result`, `gen_ai.tool.call.result`.
- **Model identity**: requested model from `gen_ai.request.model`, resolved model from `gen_ai.response.model` (falling back to the requested model), provider from `gen_ai.provider.name` or `gen_ai.system`.
- **Token usage**: input from `gen_ai.usage.input_tokens` or `gen_ai.usage.prompt_tokens`, output from `gen_ai.usage.output_tokens` or `gen_ai.usage.completion_tokens`, cached input from `gen_ai.usage.details.cache_read_tokens` or `gen_ai.usage.cached_input_tokens`, and reasoning tokens from `gen_ai.usage.details.reasoning_tokens`.
- **Cost** from `gen_ai.usage.cost`, `gen_ai.cost.total`, or `operation.cost`.
- **Model parameters** from `gen_ai.request.parameters`, `model_parameters`, `model_request_parameters`, or `model_settings`.
- **Timings** from `start_timestamp` and `end_timestamp`, parsed as ISO 8601.
- **Status**: a record is failed when its status code is `error`, when `is_exception` is true, when `level` is the string `error` or `fatal`, or when `level` is a number of 17 or higher. A failed node carries `exception_message`, `otel_status_message`, or `message` as its error.
- **Attributes**: the record's `kind`, `level`, `message`, and the full decoded `attributes` object are kept on the node under `logfire.*`.
- **Metadata**, allowlisted. From the row's columns: `deployment_environment`, `service_name`, `service_namespace`, `service_version`, `otel_scope_name`, `otel_scope_version`, `tags`. From `attributes`: `agent_name`, `deployment.environment.name`, `gen_ai.agent.name`, `gen_ai.conversation.id`, `gen_ai.operation.name`, `gen_ai.provider.name`, `gen_ai.request.model`, `gen_ai.response.model`, `gen_ai.system`, `service.name`, `service.version`, `session.id`, `session_id`, `thread_id`, `user.id`.

The importer also detects the agent framework (PydanticAI, LangGraph, OpenAI Agents, Google ADK, or the Claude Agent SDK) from the scope names, span names, and `gen_ai.agent.name` attributes in the export, provided the evidence points at exactly one.

### Grouping traces into sessions

Logfire's unit is a trace; a multi-turn conversation is usually several traces. Kitaru groups them:

- By default, traces are grouped by the first of these attribute paths that any record in the trace carries: `attributes.session.id`, `attributes.session_id`, `attributes.conversation_id`, `attributes.thread_id`, `attributes.gen_ai.conversation.id`, `attributes.conversation.id`. Values that Logfire scrubbed (`[redacted]`, `[scrubbed]`) count as absent.
- A trace with none of them becomes its own single-turn session, keyed by trace id, and records the warning `"No session attribute found; grouped by trace id"`.
- With `join_on`, traces are grouped by the scalar at that path instead, which is how you group by your own correlation key. A trace whose records disagree at the path fails with `"Trace '<id>' has conflicting values at join path '<path>'"`; a trace missing your configured value fails with `"Trace '<id>' has no value at join path '<path>'"`. Either way the rest of the file still imports.

Grouped traces become **turns**, ordered by start time. Each turn's inputs and outputs come from that trace's root record, read from the same attribute lists as node inputs and outputs. The session's `inputs` is a versioned turn list (`{"schema_version": 1, "turns": [{"source_trace_id", "inputs", "outputs"}, ...]}`), and the session's outputs come from the last turn. Session status follows the last turn's root record: a tool that failed and was retried successfully leaves the session completed.

Session metadata records the provenance you'll want when reading the import back: `logfire.session_id`, `logfire.project_id`, `logfire.trace_ids`, `logfire.join_paths`, `logfire.services`, `logfire.environments`, `source_trace_count`, `source_completeness`, and `normalization_warnings`.

## Re-runs skip what is already there

Every imported session records its source identity: `imported_from` (`logfire`) and an `external_id` of `<source_instance>:<session>`. That pair is unique on the server, so re-importing an overlapping export **skips** what is already stored and reports it as `skipped`, not as an error. Exporting the last 24 hours every night is safe; it will not duplicate earlier sessions.

It also means the grouping key matters: if you change `source_instance` or `join_on` between imports of the same records, the same conversation lands as a second session rather than deduping against the first.

## Limitations

Because a records query returns exactly the rows you asked for, the importer never claims a session is complete: `source_completeness` is always `query-dependent`. What it did notice goes into `normalization_warnings` on the session:

- `"No Logfire project identity supplied; using source_instance 'logfire'"` when neither the params nor the rows carry a project id.
- `"No session attribute found; grouped by trace id"` when a trace has no conversation identity to group on.
- `"Trace '<id>' has <n> root records"` when a trace has no single root span, usually a query that sliced through the middle of a trace.
- `"Span '<id>' references missing parent '<id>'"` when a `parent_span_id` is not in the file. Those nodes are kept as roots.

Some problems fail one session or one row rather than the file, and are reported as import failures: `"Logfire row lacks trace_id or span_id"`, `"Session '<id>' contains conflicting Logfire project ids"`, `"The import contains duplicate span ids"`, and `"The imported span graph contains a parent cycle"`. A malformed file (invalid JSON, non-UTF-8, empty, no data rows, or over 50 MiB) fails the task as a whole.

Two more things worth knowing before you rely on an import:

- Logfire's own evaluations and alerts do not come across, and neither do metrics or logs that are not span records. Evaluate imported sessions with Kitaru [evaluators](../concepts/evaluators.md) instead; backfilling your history is a single batch call.
- Replay re-runs your agent's real code, which no trace export contains. Register the agent version whose code produced these records, with its run command, and imported sessions replay exactly like recorded ones.

{% hint style="warning" %} An import stores the parsed trace content, including prompts, tool arguments, and tool results, on your Kitaru server. The server is self-hosted, but check your own access and retention rules before importing exports that contain customer data. {% endhint %}

## Next

Evaluate your imported history with [Write an evaluator](write-an-evaluator.md), then freeze the sessions that matter into a cohort and put a change to the test with [Build a regression suite from production](regression-suite.md).
