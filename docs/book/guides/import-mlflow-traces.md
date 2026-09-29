---
description: "Import MLflow traces into Kitaru: accepted export shapes, how MLflow spans become nodes, session grouping, fetching from a tracking server, and what the importer marks as lossy."
icon: flask
---

# MLflow

If your agent already records traces with MLflow Tracing, you do not need to instrument anything to start using Kitaru. Export the traces, run one import, and each conversation lands as a [session](../concepts/agents-and-sessions.md): the same object a live-recorded run produces, ready to evaluate and replay.

**MLflow stays your system of record.** Kitaru takes a runnable copy of the runs you care about, so last Tuesday's incident becomes a test case and last month's traffic becomes a regression population.

Like every import, this one executes on a [worker](../concepts/workers.md) in your environment: the server stores the export blob, your worker parses it. [Import your traces](../getting-started/import-your-traces.md) covers the generic importer contract; this page is the MLflow specifics.

## 1. Export your traces

Export traces with the MLflow CLI, which writes one page of full traces, spans included:

```bash
mlflow traces search --experiment-id 1 --max-results 500 --output json > mlflow-traces.json
```

The importer accepts a UTF-8 file that is any of:

- **An `mlflow traces search --output json` page**: `{"traces": [...], "next_page_token": ...}`.
- **A single trace**, as `Trace.to_json()` or `mlflow traces get` writes it.
- **A JSON array** of traces.
- **JSONL** whose lines are any of the above, which is how you concatenate several search pages.

Uploads are capped by the server's configurable blob limit. Export in pages as often as you like; overlapping pages repeat a trace verbatim, and an identical repeated trace imports once. [Dedup](#re-runs-skip-what-is-already-there) makes overlapping imports safe too.

Traces must include their spans. A trace exported with `--no-include-spans` fails with a remedy, and the rest of the file still imports. The importer reads MLflow 3 traces and also accepts the older MLflow 2.x trace schema.

{% hint style="info" %} MLflow stores every span attribute as a JSON-encoded string, so `mlflow.spanType` reads `"\"CHAT_MODEL\""` in the file. The importer decodes them, so you don't have to pre-process the export. {% endhint %}

## 2. Import it

Register the agent the traces belong to, if you have not, and start a worker:

```bash
kitaru agent register support-agent --command "python support.py"
kitaru worker start
```

Then import:

```bash
kitaru session import mlflow-traces.json \
  --importer kitaru/mlflow@latest \
  --agent support-agent@latest \
  --tag imported-baseline --wait
```

`kitaru/mlflow` is one of the built-in importers registered at server startup, so `@latest` always resolves and there is no importer code to write. Use `--media-type application/x-ndjson` when you upload JSONL.

`--tag` labels every session the import creates, so later commands can select them as a group (`kitaru session evaluate --tag imported-baseline ...`). Tagging happens once the import finishes, which is why it requires `--wait`. The receipt reports sessions `created`, `skipped`, and `failed`, with samples of the failures.

List what landed:

```bash
kitaru session list --agent support-agent --origin imported --imported-from mlflow
```

### Importer params

| Param | Meaning |
| --- | --- |
| `source_instance` | Source identity, and half of the session's external id. The importer prefers this, then `experiment_id`, then the experiment in each trace's location. |
| `experiment_id` | Alternative spelling of the same fallback, checked after `source_instance`. |
| `join_on` | Dotted path or RFC 6901 JSON Pointer inside each trace selecting the value that groups traces into one session. Omit it to group by `mlflow.trace.session`. See [Grouping traces into sessions](#grouping-traces-into-sessions). |
| `framework` | Extra evidence for framework detection, matched alongside the trace and span names found in the export. |

Pass them with `--params '{"source_instance": "support-prod"}'`, or use the dedicated `--join-on` flag, which accepts a JSON Pointer only (it must start with `/`) and cannot be combined with `join_on` inside `--params`:

```bash
kitaru session import mlflow-traces.json \
  --importer kitaru/mlflow@latest \
  --agent support-agent@latest \
  --join-on '/info/trace_metadata/mlflow.trace.user' --wait
```

Experiment ids are only unique within one tracking server. If you import from several MLflow servers into the same agent, give each server its own `source_instance`. Traces stored in a Databricks Unity Catalog location carry no experiment id, so those imports need `source_instance`; without it the affected trace fails with a `--params` remedy. See [Import your traces](../getting-started/import-your-traces.md) for the shared identity rules.

## 3. Or fetch from an MLflow tracking server

Skip the export and upload, and let the import task fetch traces from your tracking server directly:

```bash
kitaru session import \
  --importer kitaru/mlflow@latest \
  --agent support-agent@latest \
  --since 7d \
  --query '{"experiment_ids": ["1"]}' \
  --tag imported-baseline --wait
```

Omitting FILE and setting `--since` selects an API import: the worker searches the tracking server instead of parsing an uploaded payload. `--since` and `--until` accept an ISO 8601 timestamp or a relative duration (`7d`, `12h`, `30m`). `--trace-id` (repeatable) fetches exactly those trace ids instead of a time window. The same selection is a query object on the SDK and REST request:

| Query key | Meaning |
| --- | --- |
| `trace_ids` | MLflow trace ids to fetch. When present, exactly those traces are fetched and the time window is ignored. A trace the server does not find is skipped. |
| `since` | Timezone-aware ISO 8601 datetime, lower bound of trace start time. Required when `trace_ids` is absent. |
| `until` | Timezone-aware ISO 8601 datetime, upper bound of trace start time. Defaults to now. |
| `experiment_ids` | Experiments a time window searches. Defaults to `MLFLOW_EXPERIMENT_ID`; a time window with neither fails. |
| `filter_string` | An MLflow search filter combined with the time window, for example `"trace.status = 'OK'"` or `"tags.env = 'prod'"`. |
| `concurrency` | Batches fetched at once. Defaults to 4. |

The worker installs the package's `api` extra for an API import, which carries the MLflow tracing SDK. A [connection](provider-connections.md) you name with `--connection`, or the provider's default connection, supplies the tracking server settings:

| Variable | Meaning |
| --- | --- |
| `MLFLOW_TRACKING_URI` | Tracking server URL. Required. |
| `MLFLOW_TRACKING_TOKEN` | Bearer token, for a server behind token authentication. |
| `MLFLOW_TRACKING_USERNAME`, `MLFLOW_TRACKING_PASSWORD` | Credentials, for a server running MLflow's basic authentication. |
| `MLFLOW_EXPERIMENT_ID` | Default experiment for time-window imports. |

Without a connection, the worker's own environment supplies them, and only a worker started with `--selector kitaru/requires-credentials=mlflow` claims the task. A window lists matching traces first, then fetches them in batches that never split a session, so each session arrives complete. Each fetched trace is parsed the same way an uploaded export would be, so the node mapping, grouping, and limitations below apply the same way.

## What a trace becomes

Every MLflow span becomes one node, and each span's parent is rebuilt as the node tree, so a tool span nested under an agent span stays nested. Node type is read from the span type MLflow records in `mlflow.spanType`:

| MLflow span type | Kitaru node |
| --- | --- |
| `LLM` or `CHAT_MODEL` | `llm_call` |
| `TOOL` | `tool_call`, with `tool_name` from `mlflow.spanFunctionName` (falling back to the span name) |
| Everything else, including `AGENT`, `CHAIN`, and `RETRIEVER` | `span` |

Per node, the importer preserves:

- **Inputs and outputs** from `mlflow.spanInputs` and `mlflow.spanOutputs`, in the provider's own message format. The node's input, output, and system-prompt text selectors point at the user message, the assistant reply, and the system prompt inside that payload, which covers the OpenAI, Anthropic, and LangChain message formats MLflow records.
- **Model identity**: resolved model from `mlflow.llm.model`, provider from `mlflow.llm.provider`, and the requested model from the `model` field of a model call's inputs.
- **Token usage** from `mlflow.chat.tokenUsage`: input, output, and cache-read input tokens.
- **Cost** from the `total_cost` of `mlflow.llm.cost`, which MLflow computes from its pricing data when it knows the model and token usage.
- **Model parameters** from LangChain's `invocation_params`.
- **Timings** from the span's start and end timestamps.
- **Status**: a span with an error status is failed, and carries the status message, or the `exception` event's type and message, as its error. A span without an end time is in progress.
- **Attributes**: every other decoded span attribute, plus the span's events, is kept on the node under `mlflow.attributes` and `mlflow.events`.
- **Metadata**: the span id, the MLflow span type, and the message format MLflow recorded.

### Each model request counts once

When you enable MLflow autologging for LangChain and for OpenAI together, one request produces two nested model spans: LangChain's chat model span and, inside it, the OpenAI span for the same network call. Both report the same tokens and cost. Integrations such as PydanticAI, DSPy, and Agno also record cumulative usage on an agent span above the per-call spans.

Kitaru sums every node into the session totals, so the importer keeps usage where the request actually happened. Only the innermost span carrying token usage or cost keeps it; a span above it keeps its raw usage under `mlflow.attributes` and records `mlflow.usage_counted_on_descendants` in its metadata. A model span that wraps another model span becomes a `span` node, so the session's call count matches the requests made. The session totals then agree with the trace totals MLflow shows.

### Grouping traces into sessions

MLflow's unit is a trace; a multi-turn conversation is usually several traces. Kitaru groups them:

- By default, traces that share the `mlflow.trace.session` metadata value become one session. Set it in your agent with `mlflow.update_current_trace(session_id=...)`.
- A trace without it becomes its own single-turn session, keyed by trace id, and records the warning `"No mlflow.trace.session metadata; grouped by trace id"`.
- With `join_on`, traces are grouped by the scalar at that path instead, for example `/info/trace_metadata/mlflow.trace.user` or a tag under `/info/tags/`. A trace missing your configured value fails with `"Trace '<id>' has no value at join path '<path>'"`, and a path that selects an object or array fails too. Either way the rest of the file still imports.

Grouped traces become **turns**, ordered by start time. Each turn's inputs and outputs come from that trace's root span, falling back to the trace's `mlflow.traceInputs` and `mlflow.traceOutputs` metadata. The session's `inputs` is a versioned turn list (`{"schema_version": 1, "turns": [{"source_trace_id", "inputs", "outputs"}, ...]}`), and the session's outputs come from the last turn. Session status follows the last turn: the session fails when that trace's state is `ERROR` or its root span failed.

Session metadata records the provenance you'll want when reading the import back: `mlflow.session_id`, `mlflow.experiment_id`, `mlflow.trace_ids`, `mlflow.join_paths`, `mlflow.users` (from `mlflow.trace.user`), `mlflow.client_request_ids`, `mlflow.tags` (your own trace tags, without MLflow's internal `mlflow.*` tags), `mlflow.assessments`, `source_trace_count`, `source_completeness`, and `normalization_warnings`.

MLflow feedback and expectations attached to a trace come across in `mlflow.assessments`, with their name, value, rationale, and source. Assessments MLflow marked invalid, because a later one overrode them, are dropped.

## Re-runs skip what is already there

Every imported session records its source identity: `imported_from` (`mlflow`) and an `external_id` of `<source_instance>:<session>`. That pair is unique per destination agent, so re-importing an overlapping export with the same identity **skips** what is already stored and reports it as `skipped`, not as an error. Skipped sessions are not refreshed with new nodes, so a conversation that gained turns after its first import keeps the turns it had.

It also means the grouping key matters: if you change `source_instance` or `join_on` between imports of the same traces, the same conversation lands as a second session rather than deduping against the first.

## Limitations

What the importer noticed while normalizing goes into `normalization_warnings` on the session:

- `"No mlflow.trace.session metadata; grouped by trace id"` when a trace has no session to group on.
- `"Trace '<id>' has <n> root spans"` when a trace has no single root span.
- `"Span '<id>' references missing parent '<id>'"` when a parent span is not in the file. Those nodes are kept as roots.
- `"Trace '<id>' was still in progress"` when a trace had not finished when it was exported. The session's `source_completeness` is then `partial` instead of `full`.

Some problems fail one trace or one session rather than the file, and are reported as import failures: a trace without spans, a duplicate span id, a span parent cycle, span chains deeper than 64 levels, an invalid token count or cost, and two different copies of the same trace id in one session. A malformed file (non-UTF-8, empty, or no parseable JSON at all) fails the task as a whole.

Two more things worth knowing before you rely on an import:

- MLflow assessments come across as metadata, not as Kitaru evaluations. Evaluate imported sessions with Kitaru [evaluators](../concepts/evaluators.md) instead; backfilling your history is a single batch call.
- Replay re-runs your agent's real code, which no trace export contains. Register the agent version whose code produced these traces, with its run command, and imported sessions replay exactly like recorded ones.

{% hint style="warning" %} An import stores the parsed trace content, including prompts, tool arguments, and tool results, on your Kitaru server. The server is self-hosted, but check your own access and retention rules before importing exports that contain customer data. {% endhint %}

## Next

Evaluate your imported history with [Write an evaluator](write-an-evaluator.md), then freeze the sessions that matter into a cohort and put a change to the test with [Build a regression suite from production](regression-suite.md).
