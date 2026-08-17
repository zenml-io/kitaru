---
description: The importer contract in full — Langfuse JSONL imports, custom importers, dedup semantics, and how imports execute on your worker.
icon: file-import
---

# Import Langfuse traces

[Import your traces](../getting-started/import-your-traces.md) covers the happy path — one `kitaru session import` against the built-in Langfuse importer. This guide is the full contract: what the importer understands, how re-runs dedup, and how to write an importer for any other format.

## How an import executes

An import is a job with one importer task. You upload the export as a blob; a [worker](../concepts/workers.md) claims the task, materializes the importer's code and your payload, and runs the parse **in your environment** — the server never parses your data. Each parsed trace becomes one [session](../concepts/agents-and-sessions.md) with `origin: imported`, its observations ingested as nodes in batches.

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
        importer="langfuse",              # importer name in the registry
        agent_id=AGENT_ID,                # sessions land under this agent
        agent_version_id=None,            # optional: stamp a version on them
        payload_blob_id=blob.id,
        params={"source_instance": "my-langfuse-project"},
    )
)
```

Set `agent_version_id` when you know which code produced the traces — it's what lets a later replay default to the right version. The task's result carries the stats: sessions `created`, `skipped`, `failed`, with up to 20 failure samples (line number, external id, error).

## The built-in importers

Kitaru ships four importers as default plugins, registered at server startup under the `kitaru/` namespace — `kitaru/langfuse`, `kitaru/langsmith`, `kitaru/braintrust`, and `kitaru/kitaru-jsonl` (a native JSONL shape) — so `--importer kitaru/langfuse@latest` always resolves. They run on your worker like any other importer; there is nothing to write.

The Langfuse importer parses **Langfuse JSONL exports** — up to 50 MiB per payload (the importer's own cap, separate from the server's configurable blob limit) — and understands three record shapes: `trace`, `observation`, and raw `ingestion_event` lines. Traces map to sessions; observations map to nodes with their parent relationships, timings, model names, token usage, and cost preserved. `params`:

| Param | Meaning |
| --- | --- |
| `source_instance` | The Langfuse project the export came from. Optional when the export itself carries project ids; required when it doesn't — it anchors the sessions' external identity. |
| `filename` | Optional label used as a fallback source name. |
| `unwrap_root_names` | Optional array of root observation names to omit from the node tree. Their children become roots, while their input, output, status, and timing continue to define the session turn. Roots without children remain in the tree. |

Use `unwrap_root_names` when instrumentation adds a wrapper around a complete agent run and that wrapper duplicates the run node in Kitaru:

```bash
kitaru session import langfuse-export.jsonl \
  --importer kitaru/langfuse@latest \
  --agent support-agent@latest \
  --params '{"source_instance":"my-langfuse-project","unwrap_root_names":["resolve-ticket"]}' \
  --media-type application/x-ndjson \
  --wait
```

Import in slices as often as you like — dedup makes it safe.

## Dedup: one session per (imported_from, external_id)

Every imported session records its source identity: `imported_from` (`langfuse`) and the trace's `external_id`. That pair is unique on the server, so re-importing an overlapping export **skips** what's already stored — the stats report it as `skipped`, not as an error. This is the property that makes "export the last 24 hours every night" a safe cron job rather than a duplication engine.

Node identity works the same way inside a session: nodes upsert by index, so a re-parse states each node's full content and replaces it whole.

## Writing your own importer

An importer is one callable:

```python
from collections.abc import Iterator
from typing import Any

from kitaru.task.importer import ImportFailure, ParsedNode, ParsedSession


def parse(payload: bytes, params: dict[str, Any]) -> Iterator[ParsedSession | ImportFailure]:
    for line_number, line in enumerate(payload.splitlines(), start=1):
        try:
            record = decode_my_format(line)
        except ValueError as error:
            yield ImportFailure(line=line_number, error=str(error))
            continue
        yield ParsedSession(
            status="completed",
            name=record.title,
            inputs=record.question,
            outputs=record.answer,
            error=None,
            started_at=record.started_at,
            ended_at=record.ended_at,
            external_id=record.trace_id,
            metadata={},
            nodes=[
                ParsedNode(node_type="llm_call", name="model", status="completed",
                           inputs=record.prompt, outputs=record.completion),
            ],
        )
```

Yield lazily — the flow consumes one item at a time, so payload size is bounded by disk, not memory. Yield an `ImportFailure` for a bad record and the import counts it and moves on; only a crash of the parser itself fails the task (with partial stats preserved).

Scaffold, exercise offline, and register:

```bash
kitaru importer scaffold my-format          # writes my_format_importer.py
kitaru importer test my_format_importer.py \
  --entrypoint parse --payload sample-export.jsonl
kitaru importer register my-format \
  --script my_format_importer.py --entrypoint parse --provider my-format
```

A script importer may declare dependencies as PEP 723 inline metadata (`# /// script` block) — the worker builds it an isolated environment. An importer that outgrows one file ships as a package instead: `--package "my-importer==1.0.0"` with `--entrypoint "my_importer:parse"`. Importers are versioned like evaluators and agents; imports name the importer and pin to its latest version unless you pass one.

{% hint style="warning" %} Imported payloads contain whatever your traces contain — prompts, customer data, tool results. They are stored on your self-hosted server and parsed on your workers, but access and retention are yours to govern. {% endhint %}

## After the import

Imported sessions are full citizens: score them with [evaluators](write-an-evaluator.md) (backfilling your history is a single batch call), freeze them into [cohorts](../concepts/cohorts.md), and [replay](replay-and-overrides.md) them — provided the agent's code is registered as an agent version with a run command, since replay re-runs your code, which no trace export contains.
