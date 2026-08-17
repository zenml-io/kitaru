---
description: "Import Braintrust project logs into Kitaru: accepted export shapes, how spans become nodes, multi-turn grouping, and what the importer marks as lossy."
icon: brain
---

# Braintrust

If your agent already logs to Braintrust, you don't need to instrument anything to start using Kitaru. Export the logs, run one import, and each trace lands as a [session](../concepts/agents-and-sessions.md): the same object a live-recorded run produces, ready to evaluate and replay.

**Braintrust stays your system of record.** Kitaru takes a runnable copy of the runs you care about so that last Tuesday's incident becomes a test case and last month's traffic becomes a regression population.

Like every import, this one executes on a [worker](../concepts/workers.md) in your environment: the server stores the export blob, your worker parses it. See [Import Langfuse traces](import-langfuse-traces.md) for the generic importer contract; this page is the Braintrust specifics.

## 1. Export your Braintrust logs

The importer is deliberately permissive about the container, because Braintrust logs reach you in more than one shape. It accepts a UTF-8 file that is any of:

- **JSONL**, one Braintrust event object per line.
- **A JSON array** of event objects.
- **A JSON object with an `events` array**, the shape the Braintrust API returns for a log fetch.
- **A single JSON object**, treated as a one-event export.

Payloads are capped at 50 MiB per import (the importer's own limit, separate from the server's configurable blob limit). Import in slices as often as you like; [dedup](#re-runs-skip-what-is-already-there) makes overlapping slices safe.

What matters is the fields on each record, not how you got the file. A full project-log export carries span identity, and that is what you want:

```json
{
  "id": "event-llm",
  "project_id": "project-1",
  "span_id": "llm",
  "root_span_id": "root",
  "span_parents": ["root"],
  "span_attributes": {"name": "weather-model", "type": "llm"},
  "input": {"messages": [{"role": "user", "content": "Weather?"}]},
  "output": {"role": "assistant", "content": "Sunny."},
  "metadata": {"session_id": "conversation-1", "model": "gpt-4o"},
  "metrics": {"start": 1785000000.1, "end": 1785000000.4,
              "prompt_tokens": 5, "completion_tokens": 2,
              "estimated_cost": 0.00125},
  "created": "2026-07-24T10:00:00Z"
}
```

Rows that carry `span_id`, `root_span_id`, or `span_attributes` are treated as a **full export**. Rows without them (a flat export copied out of the Braintrust UI, for example) still import, at lower fidelity; see [Lower-fidelity exports](#lower-fidelity-exports).

## 2. Import it

Register the agent the traces belong to, if you haven't, and start a worker:

```bash
kitaru agent register support-agent --command "python support.py"
kitaru worker start
```

Then import:

```bash
kitaru session import braintrust-logs.jsonl \
  --importer kitaru/braintrust@latest \
  --agent support-agent@latest \
  --media-type application/x-ndjson \
  --tag imported-baseline --wait
```

`kitaru/braintrust` is one of the built-in importers registered at server startup, so `@latest` always resolves and there is no importer code to write. Use `--media-type application/json` when you upload a JSON array or an `events` object instead of JSONL.

`--tag` labels every session the import creates, so later commands can select them as a group (`kitaru session evaluate --tag imported-baseline ...`). Tagging happens once the import finishes, which is why it requires `--wait`. The receipt reports sessions `created`, `skipped`, and `failed`, with samples of the failures.

List what landed:

```bash
kitaru session list --agent support-agent --origin imported --imported-from braintrust
```

### Importer params

| Param | Meaning |
| --- | --- |
| `source_instance` | Project identity fallback. The importer prefers each record's `project_id`; `source_instance` is used when the export carries none. |
| `filename` | Optional label. When neither `project_id` nor `source_instance` is available, the filename stem becomes the project identity. A record with none of the three fails with "Braintrust export has no project id; provide source\_instance". |
| `join_on` | Dotted path or RFC 6901 JSON Pointer selecting the value that groups traces into one session. Defaults to the session id found in metadata. See [Grouping traces into sessions](#grouping-traces-into-sessions). |

Pass them with `--params '{"source_instance": "my-braintrust-project"}'`, or use the dedicated `--join-on` flag, which accepts a JSON Pointer only (it must start with `/`) and cannot be combined with `join_on` inside `--params`.

## What a trace becomes

Every Braintrust event in a trace becomes one node, and the `span_parents` links are rebuilt as the node tree, so a tool span nested under a model span stays nested. Node type is mapped conservatively:

| Braintrust record | Kitaru node |
| --- | --- |
| `span_attributes.type == "tool"`, or `metadata["tool.name"]` present | `tool_call`, with `tool_name` from `metadata["tool.name"]` (falling back to the span name) |
| `span_attributes.type == "llm"`, and `metadata["openinference.span.kind"]` is empty or `LLM` | `llm_call` |
| Everything else, including OpenInference `CHAIN` wrappers | `span` |

An `llm` span whose OpenInference kind says it is really a chain stays a plain span rather than being mislabeled as a model call.

Per node, the importer preserves:

- **Inputs and outputs** from `input` / `output`, falling back to OpenInference's `metadata["input.value"]` and `metadata["output.value"]`, JSON-decoded when those hold encoded JSON strings.
- **Model identity**: requested model from `gen_ai.request.model` or `model`, resolved model from `gen_ai.response.model` or `model`, provider from `gen_ai.provider.name` or `provider`.
- **Token usage** from `metrics.prompt_tokens`, `metrics.completion_tokens`, and `metrics.prompt_cached_tokens`. A non-integer value there fails that session and is reported as an import failure; other sessions in the file still import.
- **Cost** from `metrics.estimated_cost`.
- **Timings** from `metrics.start` / `metrics.end`, with `created` as a start fallback. Both ISO 8601 strings and Unix timestamps parse.
- **Status**: a record with a non-empty `error` becomes a failed node carrying that error.
- **Metadata**, allowlisted. Only session, conversation, model, and provider keys come across (`session_id`, `sessionId`, `thread_id`, `conversation_id`, `gen_ai.conversation.id`, `gen_ai.request.model`, `gen_ai.response.model`, `gen_ai.provider.name`, `model`, `provider`, `turn_index`). Everything else in `metadata` is dropped rather than copied wholesale into Kitaru.

The importer also normalizes each node for the UI and for evaluators: it locates the user input text, the visible assistant output text, the system prompt on model calls, and any visible reasoning, recording pointers into the payload rather than copying the text. Reasoning and tool-call parts are excluded from what counts as visible output. When the export's metadata names a known framework (PydanticAI, LangGraph, OpenAI Agents, Google ADK, or the Claude Agent SDK), the session records it, provided the evidence points at exactly one.

### Grouping traces into sessions

Braintrust's unit is a trace; a multi-turn conversation is usually several root traces. Kitaru groups them:

- By default, traces are grouped by the first session-like key present in the root record's metadata: `session_id`, `sessionId`, `thread_id`, `conversation_id`, or `gen_ai.conversation.id`. A trace with none of these becomes its own single-turn session.
- With `join_on`, traces are grouped by the scalar at that path in each trace's root record instead, which is how you group by your own correlation key: `--join-on '/metadata/case~1id'`. A trace missing that value, or holding an object or list there, is reported as a failure rather than silently grouped elsewhere.

Grouped traces become **turns**, ordered by start time. The session's `inputs` is a versioned turn list (`{"schema_version": 1, "turns": [{"source_trace_id", "inputs", "outputs"}, ...]}`), and the session's outputs come from the last turn. Session status follows the last turn's root record: a tool that failed and was retried successfully leaves the session completed.

Session metadata records the provenance you'll want when reading the import back: `braintrust.project_ids`, `braintrust.session_id`, `braintrust.trace_ids`, `source_trace_count`, `source_completeness`, `braintrust.join_on` when you set one, and `normalization_warnings`.

## Re-runs skip what is already there

Every imported session records its source identity: `imported_from` (`braintrust`) and an `external_id` of `<project>:<session>`. That pair is unique on the server, so re-importing an overlapping export **skips** what is already stored and reports it as `skipped`, not as an error. Exporting the last 24 hours every night is a safe cron job, not a duplication engine.

## Limitations

The importer is explicit about fidelity it cannot recover, and writes what it noticed into `normalization_warnings` on the session:

- `"Braintrust UI export omits span identity and hierarchy"` on a flat export.
- `"One or more spans reference a missing parent"` when a `span_parents` entry is not in the file, usually a partial export. Those nodes are kept as roots.
- `"Model output contains tool activity but no explicit tool spans"` when a model output references `tool_calls` that the export never recorded as their own spans. Kitaru does not invent nodes for them.
- `"One or more LLM spans lack recorded input or output"` when a model call came across without its payload.

Two more things worth knowing before you rely on an import:

- Non-allowlisted `metadata` keys and Braintrust's own evaluations do not come across. Score imported sessions with Kitaru [evaluators](../concepts/evaluators.md) instead; backfilling your history is a single batch call.
- Replay re-runs your agent's real code, which no trace export contains. Register the agent version whose code produced these traces, with its run command, and imported sessions replay exactly like recorded ones.

### Lower-fidelity exports

A flat export (rows with `input`, `output`, `metadata`, and `metrics`, but no `span_id` or `span_parents`) still imports. The importer marks it `source_completeness: "flat"`, gives each row a synthetic identity, and relaxes one rule: without span types to read, a row that carries `metadata.model` or token metrics is treated as a model call. There is no hierarchy to rebuild, so the nodes land flat. Prefer a full project-log export whenever you can get one.

{% hint style="warning" %} An import stores the parsed trace content, including prompts, tool arguments, and tool results, on your Kitaru server. The server is self-hosted, but check your own access and retention rules before importing exports that contain customer data. {% endhint %}

## Next

Score your imported history with [Write an evaluator](write-an-evaluator.md), then freeze the sessions that matter into a cohort and put a change to the test with [Build a regression suite from production](regression-suite.md).
