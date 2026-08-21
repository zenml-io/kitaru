---
description: "Langfuse JSONL imports end to end: the accepted export, dedup semantics, and how imports execute on your worker."
icon: bolt
---

# Langfuse

[Import your traces](../getting-started/import-your-traces.md) covers the shortest path: one `kitaru session import` against the built-in Langfuse importer. This guide is the full contract: what the importer understands, how re-runs dedup, and how to write an importer for any other format.

## How an import executes

An import is a job with one importer task. You upload the export as a blob; a [worker](../concepts/workers.md) claims the task, materializes the importer's code and your payload, and runs the parse **in your environment**; the server never parses your data. Each parsed trace becomes one [session](../concepts/agents-and-sessions.md) with `origin: imported`, its observations ingested as nodes in batches.

The CLI wraps the upload and the job in one command:

```bash
kitaru session import langfuse-export.jsonl \
  --importer kitaru/langfuse@latest \
  --agent support-agent@latest \
  --params '{"source_instance": "my-langfuse-project"}' \
  --media-type application/x-ndjson \
  --tag imported-baseline --wait
```

`--tag` labels the created sessions once the import completes (so it requires `--wait`); downstream commands select on it. On the Python client the same import is explicit:

```python
from kitaru.api_models.v1.imports import ImportCreateRequest

job = await client.imports.create(
    ImportCreateRequest(
        importer="langfuse",  # importer name in the registry
        agent_id=AGENT_ID,  # sessions land under this agent
        agent_version_id=None,  # optional: stamp a version on them
        payload_blob_id=blob.id,
        params={"source_instance": "my-langfuse-project"},
    )
)
```

Set `agent_version_id` when you know which code produced the traces; it's what lets a later replay default to the right version. The task's result carries the stats: sessions `created`, `skipped`, `failed`, with up to 20 failure samples (line number, external id, error).

## The built-in importers

Kitaru ships provider importers as default plugins, registered at server startup under the `kitaru/` namespace, so `--importer kitaru/langfuse@latest` always resolves. They run on your worker like any other importer; there is nothing to write. See [Import your traces](../getting-started/import-your-traces.md) for the current built-in list.

The Langfuse importer parses **Langfuse JSONL exports**, up to 50 MiB per payload (the importer's own cap, separate from the server's configurable blob limit), and understands three record shapes: `trace`, `observation`, and raw `ingestion_event` lines. Traces map to sessions; observations map to nodes with their parent relationships, timings, model names, token usage, and cost preserved. `params`:

| Param | Meaning |
| --- | --- |
| `source_instance` | The Langfuse project the export came from. Optional when the export itself carries project ids; required when it doesn't; it anchors the sessions' external identity. |
| `filename` | Optional label used as a fallback source name. |
| `infer_tool_call_links` | Optional boolean, default `true`. The importer matches tool-call ids emitted by a generation with `gen_ai.tool.call.id` on tool observations, nests each unambiguous tool call under the requesting generation, and retains its original Langfuse parent as a secondary parent. Unmatched or ambiguous ids remain unchanged. Set this to `false` to keep only the source observation hierarchy. |

Import in slices as often as you like; dedup makes it safe.

## Dedup: one session per (imported_from, external_id)

Every imported session records its source identity: `imported_from` (`langfuse`) and the trace's `external_id`. That pair is unique on the server, so re-importing an overlapping export **skips** what's already stored; the stats report it as `skipped`, not as an error. This is the property that makes "export the last 24 hours every night" a safe cron job rather than a duplication engine.

Node identity works the same way inside a session: nodes upsert by index, so a re-parse states each node's full content and replaces it whole.

## No importer for your format?

The importer contract is deliberately small, about a page of Python, and the shipped Langfuse importer is a reference implementation of it. See [No importer for your format](custom-importer.md) to scaffold, test, and register your own.

## After the import

Imported sessions are full Kitaru sessions: evaluate them with [evaluators](write-an-evaluator.md) (backfilling your history is a single batch call), freeze them into [cohorts](../concepts/cohorts.md), and [replay](replay-and-overrides.md) them. Replay re-runs your code, which no trace export contains, so the agent's code must be registered as an agent version with a run command.
