---
description: "Import Arize Phoenix trace exports into Kitaru: supported UI and CLI files, span mapping, source identity, and known limits."
icon: fire
---

# Arize Phoenix

If your agent already sends traces to Arize Phoenix, export the runs you care about and import the file into Kitaru. Each Phoenix trace becomes one [session](../concepts/agents-and-sessions.md), with its model calls, tool calls, agent spans, timings, status, token usage, and cost preserved where the export records them.

Phoenix stays your system of record. Kitaru stores a runnable copy for evaluation, cohort building, and replay. The importer runs on a [worker](../concepts/workers.md) in your environment; the server stores the uploaded file, but does not parse it.

## 1. Export traces from Phoenix

### Phoenix UI

Open a project in Phoenix, select **Traces**, select the traces to export, and choose **Download selection**. In the download dialog:

1. Choose **Traces** for the data.
2. Choose **JSONL** for the format.
3. Include span or trace annotations if you want them retained as import metadata.
4. Download the file.

Phoenix's UI trace download is one flat span object per JSONL line. The file is still a trace export: `context.trace_id` groups its lines, while `context.span_id` and `parent_id` reconstruct the graph. Line order is not significant.

### Phoenix CLI

The importer also accepts the JSON written by Phoenix CLI trace retrieval. A CLI trace object contains `traceId` and a `spans` array, with optional trace `annotations` and `notes`. You can import one object, a JSON array of objects, or JSONL with one trace object per line.

The UI and CLI therefore carry the same span objects in different containers. You do not need to reshape either one. See Phoenix's [trace retrieval guide](https://arize.com/docs/phoenix/tracing/how-to-tracing/importing-and-exporting-traces/retrieve-traces-via-cli) for the current CLI commands.

Payloads are capped at 50 MiB per import. Split a larger export into smaller files.

## 2. Import the file

Register the agent the traces belong to, if needed, and run a worker:

```bash
kitaru agent register support-agent --command "python support.py"
kitaru worker start
```

Then import a Phoenix UI download:

```bash
kitaru session import phoenix-traces.jsonl \
  --importer kitaru/phoenix@latest \
  --agent support-agent@latest \
  --media-type application/x-ndjson \
  --tag imported-baseline \
  --wait
```

Use `--media-type application/json` for a CLI JSON object or array. On Kitaru 0.22.2 and later, `kitaru/phoenix` is a built-in importer registered at server startup, so there is no importer code to register. Older servers do not have it in their catalog; upgrade the server before importing.

List the imported sessions:

```bash
kitaru session list \
  --agent support-agent \
  --origin imported \
  --imported-from phoenix
```

## 3. Or fetch from the Phoenix API

Skip the export and upload, and let the import task fetch spans from Phoenix directly:

```bash
kitaru session import \
  --importer kitaru/phoenix@latest \
  --agent support-agent@latest \
  --since 7d \
  --tag imported-baseline --wait
```

Omitting FILE and setting `--since` selects an API import: the worker calls the Phoenix API instead of parsing an uploaded payload. `--since` and `--until` accept an ISO 8601 timestamp or a relative duration (`7d`, `12h`, `30m`). `--trace-id` (repeatable) fetches exactly those trace ids instead of a time window. The same selection is a query object on the SDK and REST request:

| Query key | Meaning |
| --- | --- |
| `project` | Phoenix project to fetch from. Defaults to the project name from the environment. |
| `trace_ids` | Phoenix trace ids to fetch. When present, exactly those traces are fetched and the time window is ignored. |
| `since` | Timezone-aware ISO 8601 datetime, lower bound of span start time. Required when `trace_ids` is absent. |
| `until` | Timezone-aware ISO 8601 datetime, upper bound of span start time. Defaults to now. |
| `concurrency` | Traces fetched at once. Defaults to 4. |

Pass `project` through `--query '{"project": "my-project"}'`. The worker needs `kitaru-phoenix-importer[adapter]` installed (the default plugin catalog already installs it that way) and, in its environment, `PHOENIX_ENDPOINT` or `PHOENIX_COLLECTOR_ENDPOINT`, `PHOENIX_API_KEY`, and `PHOENIX_PROJECT` for the default project. Each fetched trace is parsed the same way an uploaded export would be, so the node mapping and limits below apply the same way.

## What becomes a session

The safe default is one Phoenix trace per Kitaru session. The Phoenix `trace_id` becomes the session's `external_id`, so importing the same trace again skips it rather than creating a duplicate. Phoenix session or conversation attributes remain on the span, but this first importer version does not join several traces into one multi-turn session.

Every exported span becomes a node. The importer sorts spans by time and reconstructs their parent relationships instead of trusting export order.

| Phoenix `span_kind` | Kitaru node |
| --- | --- |
| `LLM` | `llm_call` |
| `TOOL` | `tool_call` |
| `AGENT`, `CHAIN`, `UNKNOWN`, and other kinds | `span` |

`AGENT` remains a plain span because a Phoenix agent span does not by itself prove that Kitaru should treat it as a separately replayable subagent.

The importer reads common OpenInference and OpenTelemetry GenAI attributes for:

- inputs and outputs, including model messages, tool arguments and results, and Google ADK request and response payloads;
- requested and resolved model names, model provider, and model parameters;
- input, output, cached-input, and reasoning token counts;
- recorded cost;
- tool name;
- PydanticAI or Google ADK framework identity when provider-specific attributes establish it.

The original Phoenix attributes and events remain on each node under `phoenix.attributes` and `phoenix.events`. CLI trace annotations and notes remain in session metadata.

## Status and partial exports

Phoenix `ERROR` spans become failed nodes. `OK` and `UNSET` spans become completed nodes because both are terminal states in exported traces. Session status follows the root span, so a tool call that failed and was successfully retried does not incorrectly fail the whole session.

A span whose parent is absent from the file remains importable as a root node. The session records `source_completeness: partial` and a `normalization_warnings` entry. Duplicate span ids and parent cycles fail only the affected trace; other valid traces in the same file still import.

## Limits

- The parser reads files. Live API access is the separate fetch path described above, not something the parser itself does.
- It supports Phoenix's native JSON and JSONL trace shapes, not arbitrary OTLP JSON envelopes. Export JSONL from the Phoenix UI or JSON with the Phoenix CLI.
- It does not accept JSONL produced by serializing `get_spans_dataframe()`. That table uses flattened top-level column names rather than the UI and CLI span objects.
- It does not import Phoenix datasets, experiments, evaluators, or project configuration. Trace and span annotations included in the export are retained as metadata, but do not become Kitaru evaluations.
- Replay still needs the registered agent code that produced the trace. No trace export contains runnable agent code.

{% hint style="warning" %} A trace export can contain prompts, tool arguments, tool results, annotations, and exception stack traces. Importing stores that content on your Kitaru server. Check your access and retention rules before importing production data. {% endhint %}

## Next

Evaluate the imported history with [Write an evaluator](write-an-evaluator.md), then freeze the sessions that matter into a cohort with [Build a regression suite from production](regression-suite.md).
