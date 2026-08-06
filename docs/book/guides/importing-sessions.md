---
description: Import provider traces or portable Kitaru session JSONL into a workspace.
icon: file-import
---

# Import Sessions

Kitaru importers convert exported trace data into one session graph. Provider importers decode source records, join related traces into sessions, order turns, reconstruct node relationships, and project common fields for the UI while preserving source inputs and outputs.

Use a provider importer for Langfuse, LangSmith, Braintrust, or OpenTelemetry data. Use the `kitaru-jsonl` importer when your producer already emits the Kitaru session and node contract.

## The portable session contract

Each imported session contains session fields and a list of nodes. A session is the user-visible execution or conversation. A node is one recorded model call, tool call, subagent call, or span.

| Session field | Type | Meaning |
|---|---|---|
| `status` | `in_progress`, `completed`, or `failed` | Final source status. |
| `name` | string or null | Display name. |
| `system_prompt` | string or null | Most recently recorded system prompt. |
| `inputs` | any JSON value | Complete session input. Provider importers use a versioned `turns` object for multi-turn sessions. |
| `outputs` | any JSON value | Final session output. |
| `error` | string or null | Failure message. |
| `started_at`, `ended_at` | ISO 8601 timestamp or null | Session time range. |
| `external_id` | string | Stable identity in the source system. Kitaru uses it with `imported_from` for deduplication. |
| `metadata` | JSON object | Source identity, normalization warnings, and user metadata. |
| `imported_from` | string or null | Source importer. Kitaru sets this from the selected importer rather than the JSONL record. |
| `framework` | string or null | Agent framework when the trace identifies one, such as `pydantic-ai` or `langgraph`. |
| `nodes` | node array | Flat indexed nodes. |

Each node uses the fields below. Optional fields can be omitted or set to null.

| Node field | Type | Meaning |
|---|---|---|
| `index` | integer | Stable position within the session import. Parents must have lower indexes. |
| `parent_index` | integer or null | Primary parent. |
| `secondary_parent_indexes` | integer array | Additional parents for graph joins. |
| `external_id`, `trace_id` | string or null | Source node and trace identities. |
| `node_type` | `llm_call`, `tool_call`, `subagent_call`, or `span` | Work represented by the node. |
| `name` | string | Display name. |
| `status` | `in_progress`, `completed`, or `failed` | Node status. |
| `error` | string or null | Failure message. |
| `started_at`, `ended_at` | ISO 8601 timestamp or null | Node time range. |
| `input_text` | string or null | Primary human-readable input. Provider importers extract the latest user message or prompt here. |
| `output_text` | string or null | Primary human-readable output. Provider importers extract the latest assistant message or result here. |
| `system_prompt` | string or null | System prompt for this model call. |
| `reasoning` | string or null | Visible reasoning text when the source exports it. |
| `inputs`, `outputs` | any JSON value | Complete source payloads. Importers preserve message history, tool arguments, multimodal parts, and provider-specific content here. |
| `requested_model`, `model`, `provider` | string or null | Requested model, served model, and model provider. |
| `tokens` | object or null | Input, output, cached input, and reasoning token counts when reported. |
| `cost` | decimal or null | Recorded or estimated call cost. |
| `model_params` | object or null | Model request parameters. |
| `tool_name`, `subagent_id` | string or null | Tool or subagent identity for the matching node type. |
| `attributes` | any JSON value | Span attributes retained for diagnostics. |
| `metadata` | JSON object | Bounded source metadata. |

`input_text` and `output_text` are display projections. Model calls expose the latest user and assistant text instead of the accumulated message history. Tool calls use compact JSON when their arguments or results have no text representation. `inputs` and `outputs` retain the complete source payloads for inspection and replay.

`reasoning` contains visible text only. Redacted, encrypted, or unavailable reasoning remains null, while the provider payload stays in `inputs` or `outputs`. Token usage can also include `reasoning_tokens` when a provider reports the count.

## Create Kitaru JSONL

Write one session object per line. The `kitaru-jsonl` importer validates every field and rejects unknown fields. Invalid lines are reported independently, so valid sessions in the same upload can still import.

The formatted object below represents one JSONL record. Serialize it onto one line in the file.

```json
{
  "status": "completed",
  "name": "Weather request",
  "system_prompt": "Answer in one sentence.",
  "inputs": {"question": "What is the weather in Delft?"},
  "outputs": {"answer": "Delft is rainy and 18 C."},
  "started_at": "2026-07-22T10:00:00Z",
  "ended_at": "2026-07-22T10:00:01Z",
  "external_id": "weather-session-42",
  "metadata": {"environment": "production"},
  "framework": "pydantic-ai",
  "nodes": [
    {
      "index": 0,
      "parent_index": null,
      "secondary_parent_indexes": [],
      "external_id": "model-call-42",
      "trace_id": "trace-42",
      "node_type": "llm_call",
      "name": "answer weather question",
      "status": "completed",
      "started_at": "2026-07-22T10:00:00Z",
      "ended_at": "2026-07-22T10:00:01Z",
      "input_text": "What is the weather in Delft?",
      "output_text": "Delft is rainy and 18 C.",
      "system_prompt": "Answer in one sentence.",
      "reasoning": "The weather tool reports rain and a temperature of 18 C.",
      "inputs": [{"role": "system", "content": "Answer in one sentence."}, {"role": "user", "content": "What is the weather in Delft?"}],
      "outputs": [{"role": "assistant", "content": "Delft is rainy and 18 C."}],
      "model": "claude-haiku-4-5-20251001",
      "provider": "anthropic",
      "tokens": {"input_tokens": 24, "output_tokens": 11, "cached_input_tokens": 0, "reasoning_tokens": 0},
      "attributes": {},
      "metadata": {}
    }
  ]
}
```

Node indexes do not need to be contiguous. Every `parent_index` and `secondary_parent_indexes` value must be lower than the child index. A node index must be unique within its session.

## Import a file

The session import command uploads the file, resolves an exact importer and agent version, and creates an import job:

```bash
kitaru session import sessions.jsonl \
  --importer kitaru-jsonl@latest \
  --agent customer-service@latest \
  --media-type application/x-ndjson \
  --wait
```

Use `--tag` with `--wait` to tag every created session. Use `--params` for provider-specific grouping and source settings.

## Join provider traces into sessions

Providers often record one conversation turn as one trace. Importers group related traces into one Kitaru session, then order the traces by start time with a stable trace-ID tie-breaker.

Default grouping uses the provider's native conversation or session identifier. When that identifier is absent, each trace becomes one session. Langfuse and LangSmith also accept explicit selectors for internal metadata.

For Langfuse, select a JSON object and a key inside it:

```bash
kitaru session import langfuse-observations.jsonl \
  --importer langfuse@latest \
  --agent customer-service@latest \
  --params '{"join_path":"/metadata/customer","join_key":"case_id"}' \
  --wait
```

The example reads `case_id` from the object at `/metadata/customer`. Five Langfuse traces with the same value become five ordered turns in one Kitaru session. JSON Pointer escaping follows RFC 6901: `~1` represents `/` and `~0` represents `~`.

You can provide the complete dotted path or JSON Pointer with `join_on` instead:

```json
{"join_on": "/metadata/customer/case_id"}
```

An explicit selector must resolve to exactly one non-empty value per source trace. A trace with a missing or conflicting value becomes an import failure. Kitaru does not silently split it into another session. Imported session metadata records the selector under `langfuse.join_paths` or `langsmith.join_paths`.

## What provider importers normalize

Provider importers apply the same output contract to different source formats:

| Source | Accepted shape | Default grouping |
|---|---|---|
| Langfuse | Trace, observation, and ingestion-event JSON or JSONL | `sessionId`, then `traceId` |
| LangSmith | Run-query and bulk-export JSON or JSONL | Known thread metadata paths, then `trace_id` |
| Braintrust | Project-log and UI JSON exports | Known session or conversation fields, then trace ID |
| OpenTelemetry | OTLP collector envelopes, flattened OTLP JSONL, Arize JSONL, and Logfire JSONL | Standard conversation attributes, then trace ID |
| Kitaru | One portable Kitaru session per JSONL line | No grouping; each line is one session |

Normalization includes source identity, parent-child graph reconstruction, deterministic ordering, status and error mapping, model fields, token counts, cost, tool arguments and results, `input_text`, `output_text`, `system_prompt`, visible `reasoning`, and framework detection. Source payloads remain in `inputs` and `outputs`. Session metadata reports normalization warnings and source completeness.

Framework detection only sets `framework` when trace metadata identifies one supported framework without conflict. Unknown or sparse traces keep the field null.

## Inspect failures

The import job result reports created, skipped, and failed counts plus a bounded failure sample. Reimporting the same `(imported_from, external_id)` pair skips the duplicate.
