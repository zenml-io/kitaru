---
description: Import provider traces or portable Kitaru session JSONL into a workspace.
icon: file-import
---

# Import Sessions

Kitaru importers convert exported trace data into one session graph. Provider importers decode source records, join related traces into sessions, order turns, reconstruct node relationships, and project common fields for the UI while preserving source inputs and outputs.

Use a provider importer for Langfuse, LangSmith, or Braintrust data. Use the `kitaru-jsonl` importer when your producer already emits the Kitaru session and node contract.

## The portable session contract

Each imported session contains session fields and a list of nodes. A session is the user-visible execution or conversation. A node is one recorded model call, tool call, subagent call, or span.

| Session field | Type | Meaning |
|---|---|---|
| `status` | `in_progress`, `completed`, or `failed` | Final source status. |
| `name` | string or null | Display name. |
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
| `input_text_selector` | string or null | RFC 6901 JSON Pointer selecting the primary human-readable text inside `inputs`. |
| `output_text_selector` | string or null | RFC 6901 JSON Pointer selecting the primary human-readable text inside `outputs`. |
| `system_prompt_selector` | string or null | RFC 6901 JSON Pointer selecting the system prompt inside `inputs`. |
| `reasoning` | string or null | Visible reasoning text when the source exports it. |
| `inputs`, `outputs` | any JSON value | Complete source payloads. Importers preserve message history, tool arguments, multimodal parts, and provider-specific content here. |
| `requested_model`, `model`, `model_provider` | string or null | Requested model, served model, and model provider. |
| `tokens` | object or null | Input, output, cached input, and reasoning token counts when reported. |
| `cost` | decimal or null | Recorded or estimated call cost. |
| `model_params` | object or null | Model request parameters. |
| `tool_name`, `subagent_id` | string or null | Tool or subagent identity for the matching node type. |
| `attributes` | any JSON value | Span attributes retained for diagnostics. |
| `metadata` | JSON object | Bounded source metadata. |

Text selectors avoid copying potentially large values into separate columns. A selector is present only when the importer can identify one relevant string in the corresponding payload. A client resolves that [RFC 6901 JSON Pointer](https://www.rfc-editor.org/rfc/rfc6901.html) when it loads the node payload and can show the complete `inputs` or `outputs` value for inspection. The selectors remain available in node list responses without loading the payload columns. `system_prompt_selector` resolves against `inputs`. A null selector means that the importer could not choose one text value without guessing. The empty string is the JSON Pointer for the complete payload, which is useful when the payload itself is the selected string.

`reasoning` contains visible text only. Redacted, encrypted, or unavailable reasoning remains null, while the provider payload stays in `inputs` or `outputs`. Token usage can also include `reasoning_tokens` when a provider reports the count.

## Create Kitaru JSONL

Write one session object per line. The `kitaru-jsonl` importer validates every field and rejects unknown fields. Invalid lines are reported independently, so valid sessions in the same upload can still import.

The formatted object below represents one JSONL record. Serialize it onto one line in the file.

```json
{
  "status": "completed",
  "name": "Weather request",
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
      "input_text_selector": "/1/content",
      "output_text_selector": "/0/content",
      "system_prompt_selector": "/0/content",
      "reasoning": "The weather tool reports rain and a temperature of 18 C.",
      "inputs": [{"role": "system", "content": "Answer in one sentence."}, {"role": "user", "content": "What is the weather in Delft?"}],
      "outputs": [{"role": "assistant", "content": "Delft is rainy and 18 C."}],
      "model": "claude-haiku-4-5-20251001",
      "model_provider": "anthropic",
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
  --importer kitaru/kitaru-jsonl@latest \
  --agent customer-service@latest \
  --media-type application/x-ndjson \
  --wait
```

Use `--tag` with `--wait` to tag every created session. Use `--join-on` to group provider traces by a source value. Use `--params` for other provider-specific settings.

## Join provider traces into sessions

Providers often record one conversation turn as one trace. Importers group related traces into one Kitaru session, then order the traces by start time with a stable trace-ID tie-breaker.

Default grouping uses the provider's native conversation or session identifier. When that identifier is absent, each trace becomes one session. Use `--join-on` when the export carries the shared session identity in another field.

The option takes an [RFC 6901 JSON Pointer](https://www.rfc-editor.org/rfc/rfc6901.html) that selects one scalar value from each source trace:

```bash
kitaru session import langfuse-observations.jsonl \
  --importer kitaru/langfuse@latest \
  --agent customer-service@latest \
  --join-on '/metadata/customer/case_id' \
  --wait
```

The example reads the scalar at `/metadata/customer/case_id`. Five traces with the value `case-42` become five ordered turns in the same Kitaru session. Traces with another value form another session.

Escape source keys according to RFC 6901. Use `~1` for `/` and `~0` for `~`. For example, `/metadata/customer~1case~0id` selects the key `customer/case~id` inside `metadata`.

The pointer root depends on the importer:

| Importer | Pointer root | Example |
|---|---|---|
| Braintrust | Each raw trace-root record | `/metadata/customer~1case_id` |
| Langfuse | Observation records belonging to one trace; every selected value must agree | `/metadata/customer/case_id` |
| LangSmith | Each raw trace-root run | `/extra/metadata/thread_id` |

The selected value must be a non-empty string, number, or boolean. A missing, conflicting, object, or array value produces an isolated failure for that trace. Kitaru does not silently place the trace into a fallback session. Imported metadata records explicit grouping provenance under `braintrust.join_on`, `langfuse.join_paths`, or `langsmith.join_paths`.

### SDK and REST

The CLI validates `--join-on` and adds it to the importer parameter object. SDK callers pass the same `join_on` parameter directly:

```python
from kitaru.api_models.v1.imports import ImportCreateRequest

job = await client.imports.create(
    ImportCreateRequest(
        importer="kitaru/langfuse",
        version=1,
        agent_id=agent_id,
        agent_version_id=agent_version_id,
        payload_blob_id=blob_id,
        params={"join_on": "/metadata/customer/case_id"},
    )
)
```

The REST request uses the same structure:

```json
{
  "importer": "kitaru/langfuse",
  "version": 1,
  "agent_id": "00000000-0000-0000-0000-000000000000",
  "agent_version_id": "00000000-0000-0000-0000-000000000001",
  "payload_blob_id": "00000000-0000-0000-0000-000000000002",
  "params": {"join_on": "/metadata/customer/case_id"}
}
```

Send this object to `POST /api/v1/imports`. The server stores `params` on the import task, the worker includes them in `ImportTaskDetails`, and the task process calls the selected importer as `parse(payload, params)`.

Existing integrations can continue to send `params.join_on` as a dotted path. The explicit CLI option accepts JSON Pointer syntax only. Langfuse also retains its older `join_path` plus `join_key` parameters for compatibility, but new integrations should use `join_on`.

## What provider importers normalize

Provider importers apply the same output contract to different source formats:

| Source | Accepted shape | Default grouping |
|---|---|---|
| Langfuse | Trace, observation, and ingestion-event JSON or JSONL | `sessionId`, then `traceId` |
| LangSmith | Run-query and bulk-export JSON or JSONL | Known thread metadata paths, then `trace_id` |
| Braintrust | Project-log and UI JSON exports | Known session or conversation fields, then trace ID |
| Kitaru | One portable Kitaru session per JSONL line | No grouping; each line is one session |

Normalization includes source identity, parent-child graph reconstruction, deterministic ordering, status and error mapping, model fields, token counts, cost, tool arguments and results, text selectors, visible `reasoning`, and framework detection. Source payloads remain in `inputs` and `outputs`. Session metadata reports normalization warnings and source completeness.

Framework detection only sets `framework` when trace metadata identifies one supported framework without conflict. Unknown or sparse traces keep the field null.

## Inspect failures

The import job result reports created, skipped, and failed counts plus a bounded failure sample. Reimporting the same `(imported_from, external_id)` pair skips the duplicate.

## No importer for your provider

You have two ways in, and neither requires waiting for us to ship an importer.

**Convert to Kitaru JSONL.** Write out [Kitaru JSONL](#create-kitaru-jsonl) — one session object per line, exactly the contract above. This is the right choice for a one-off backfill or an export you can transform with a script. Nothing gets installed or registered.

**Write an importer.** Worth it when the conversion is ongoing, or when the source needs real normalization rather than a field rename. The contract is one function:

```python
Parser = Callable[[bytes, dict[str, Any]], Iterator[ImportedSession | ImportFailure]]
```

That is the whole interface. You receive the uploaded bytes and the `--params` object, and yield one `ImportedSession` per session you recognize — or an `ImportFailure` for a record you cannot parse, which isolates that record instead of failing the whole import:

```python
from kitaru.api_models.v1.imports import ImportFailure
from kitaru.task.importer import ImportedSession

def parse(content: bytes, params: dict[str, Any]) -> Iterator[ImportedSession | ImportFailure]:
    for line_number, line in enumerate(content.decode("utf-8").splitlines(), start=1):
        try:
            yield ImportedSession.model_validate(transform(json.loads(line)))
        except ValueError as exc:
            yield ImportFailure(line=line_number, external_id=None, error=str(exc))
```

Three things to get right, because they are where custom importers usually go wrong:

- **`external_id` is your identity, and it must be stable.** Kitaru deduplicates on `(imported_from, external_id)`, so a re-import is only safe if the id does not move between runs. Derive it from the source's own identifier, never from a row number or a timestamp.
- **Decide session boundaries deliberately.** One `ImportedSession` should be one end-to-end run — see [what a session is](../concepts/agents-and-sessions.md). If your source splits a run across records, join them in the parser.
- **Yield failures, don't raise them.** An exception ends the import; an `ImportFailure` costs you one record and keeps the rest.

The shipped importers are the reference: `plugins/packages/jsonl-importer` is the smallest at under 80 lines, and the Langfuse one shows real normalization. The `kitaru-importer-builder` [agent skill](../agent-native/skills.md) exists for this job — it turns a representative export into a locally validated importer, keeps the mapping from source evidence to normalized sessions explicit so you can see what is preserved, approximated, or unavailable, and finishes locally until you approve registration:

```bash
npx skills add zenml-io/kitaru-skills
```
