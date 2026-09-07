---
description: "No built-in importer for your format? An importer is one Python callable: scaffold it, test it offline, register it, and your export imports like any other."
icon: puzzle-piece
---

# No importer for your format

The built-in importers cover [Langfuse](import-langfuse-traces.md), [LangSmith](import-langsmith-traces.md), [Braintrust](import-braintrust-traces.md), [Logfire](import-logfire-traces.md), [Arize Phoenix](import-phoenix-traces.md), and the [Kitaru JSONL contract](importing-sessions.md). Any other trace store, or a homegrown logging format, comes in through a custom importer. An importer is small by design: one callable that parses your export bytes into sessions, usually about a page of Python.

There are two ways to get one, and the fast way is to not write it yourself: the `kitaru-importer-builder` [agent skill](../agent-native/setup.md) turns a representative export into a locally validated importer. It keeps the mapping from source evidence to normalized sessions explicit, so you can see what is preserved, approximated, or unavailable, and it finishes on your machine until you approve registration.

## The contract

```python
from collections.abc import Iterator
from typing import Any

from kitaru.task.importer import ImportFailure, ParsedNode, ParsedSession


def parse(
    payload: bytes, params: dict[str, Any]
) -> Iterator[ParsedSession | ImportFailure]:
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
                ParsedNode(
                    node_type="llm_call",
                    name="model",
                    status="completed",
                    inputs=record.prompt,
                    outputs=record.completion,
                ),
            ],
        )
```

Yield lazily; the import consumes one item at a time, so payload size is bounded by disk, not memory. Yield an `ImportFailure` for a bad record and the import counts it and moves on. Only a crash of the parser itself fails the task, with partial stats preserved. The full field reference for `ParsedSession` and `ParsedNode` is the [portable session contract](importing-sessions.md). `parse` may be a regular or an async generator.

Set a stable `external_id` from your source system: together with the importer's provider name it is the dedup key, so re-importing an overlapping export skips what is already stored instead of duplicating it.

## Fetch traces from your own API instead of a file

A custom importer can accept an API import too, the same way the built-in provider importers do. Instead of a bare `parse` function, register an importer object as the entrypoint. It exposes `parse` and `fetch`, each of which may be a regular or an async generator:

```python
from collections.abc import AsyncIterator, Iterator
from typing import Any


class MyImporter:
    def parse(
        self, payload: bytes, params: dict[str, Any]
    ) -> Iterator[ParsedSession]: ...

    async def fetch(self, query: dict[str, Any]) -> AsyncIterator[bytes]:
        for trace_id in select_traces(query):
            yield fetch_trace_bytes(trace_id)


importer = MyImporter()
```

Register it with `--entrypoint importer` for a script source, or `my_importer:importer` for a package source. An entrypoint that is a plain callable stays upload-only.

A package source declares what `fetch` needs under a `fetch` extra in its `pyproject.toml`, and the worker installs `my-importer[fetch]` for an API import and the bare package for an upload. A script source lists its dependencies inline as for any script plugin, so they are installed for both.

`fetch` receives the import's `--query` (or `source.query` on the request) and yields parser payloads. The server validates the shared keys (`trace_ids`, `since`, `until`, `concurrency`) as `ImportQuery` from `kitaru.api_models.v1.imports` before the import is created, and passes provider-specific keys such as `project_id` through untouched, so `fetch` receives the full merged dict. Each yielded payload runs through `parse` with the import's `params`, exactly like a file upload would, so every trace that `parse` groups into one session must be in the same payload. The built-in importers yield one payload holding every fetched trace, oldest first, and their bounded `concurrency` relies on `fetch` being an async generator. Raise from `fetch` to end the import task with the failure recorded in the import stats. An API import against an importer without `fetch` fails the same way, so `kitaru session import --wait` reports it in the import stats.

## Scaffold, test offline, register

```bash
kitaru importer scaffold my-format          # writes my_format_importer.py
kitaru importer test my_format_importer.py \
  --entrypoint parse --payload sample-export.jsonl
kitaru importer register my-format \
  --script my_format_importer.py --entrypoint parse --provider my-format
```

A script importer may declare dependencies as PEP 723 inline metadata (a `# /// script` block); the worker builds it an isolated environment. An importer that outgrows one file ships as a package instead: `--package "my-importer==1.0.0"` with `--entrypoint "my_importer:parse"`. Importers are versioned like evaluators and agents; imports name the importer and pin to its latest version unless you pass one.

Once registered, your format imports exactly like the built-in ones:

```bash
kitaru session import my-export.jsonl \
  --importer my-format@latest \
  --agent support-agent@latest --wait
```

The shipped importers are the reference implementations: `plugins/packages/jsonl-importer` is the smallest at under 80 lines, and the Langfuse one shows real normalization with turn grouping and warnings.

{% hint style="warning" %}
Imported payloads contain whatever your traces contain: prompts, customer data, tool results. They are stored on your self-hosted server and parsed on your workers, but access and retention are yours to govern.
{% endhint %}

## Next

Evaluate the history you imported with [Write an evaluator](write-an-evaluator.md), then freeze the sessions that matter into a cohort and put a change to the test with [Build a regression suite from production](regression-suite.md).
