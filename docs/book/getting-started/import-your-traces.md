---
description: Bring the traces you already collect — Langfuse stays your system of record, Kitaru gets a runnable copy.
icon: file-import
---

# Import your traces

You don't have to run a single request through Kitaru to start. If your
agent already logs to Langfuse (or any tracing system you can export
from), your history is the raw material: import it, and every trace lands
as a [session](../concepts/agents-and-sessions.md) — the same object a
live-recorded run produces, ready to replay and evaluate like any other.

This is the honest division of labor: **your observability stack stays
your system of record**. Kitaru takes a copy of the runs you care about
and makes them runnable — the incident from Tuesday becomes a test case,
last month's traffic becomes a regression population.

Imports execute on a [worker](../concepts/workers.md) in your environment.
The export file is parsed by your worker, not by anything outside your
infrastructure.

## 1. Register the agent the traces belong to

The Langfuse importer is **built in** — registered into the server when
it's set up, so there is no importer code to write for Langfuse exports.
Braintrust and OpenTelemetry (OTLP) importers are in the works; until
they land, other formats come in through a
[custom importer](../guides/import-langfuse-traces.md#writing-your-own-importer).

<!-- TODO(v2-launch): default-plugin seeding moved from server startup to
     scripts/seed_default_plugins.py (Aug 4) — confirm the shipped
     mechanism and exact setup step before publish. Braintrust/OTLP
     importers were descoped Aug 6 (codex/v2-importer-braintrust-otlp is
     re-adding them) — re-check the shipped importer list. -->

Register the agent these traces belong to, if you haven't:

```bash
kitaru agent register support-agent --command "python support.py"
```

## 2. Import the export

Export your traces from Langfuse as JSONL (trace, observation, and
ingestion-event records are all understood), start a
[worker](../concepts/workers.md) in another terminal
(`kitaru worker start`), then:

```bash
kitaru session import langfuse-export.jsonl \
  --importer langfuse@latest \
  --agent support-agent \
  --tag imported-baseline \
  --media-type application/x-ndjson \
  --wait
```

`--tag` labels every session this import creates (repeat it for more
than one label), so later steps can select them as a group —
`kitaru session evaluate --tag imported-baseline ...` — without copying
IDs around. (Tagging happens once the import completes, which is why
`--tag` requires `--wait`.)

The final receipt reports what happened: sessions `created`, `skipped`,
and `failed`, with samples of the failures. Each imported trace becomes
one session (`origin: imported`) with its observations as nodes — model
calls with token usage and cost, tool calls with arguments and results.
List them:

```bash
kitaru session list --agent support-agent --origin imported
```

The same import is two calls on the
[Python client](../concepts/agents-and-sessions.md) when you'd rather
script it — upload the export with `client.blobs.upload(...)`, then
create the import with `client.imports.create(ImportCreateRequest(
importer="langfuse", agent_id=..., payload_blob_id=...))`.

## Re-runs are safe

Every imported session keeps its source identity (`imported_from` +
`external_id`). Importing the same export twice — or a bigger export that
overlaps an earlier one — skips what's already there instead of
duplicating it. Import incrementally, as often as you like.

{% hint style="warning" %}
An import stores the parsed trace content — prompts, tool arguments, tool
results — on your Kitaru server. The server is self-hosted, but check your
own access and retention rules before importing exports that contain
customer data.
{% endhint %}

## What imported sessions can do

Everything recorded sessions can:

* **Inspect** them — nodes, cost, and token rollups all populate.
* **Evaluate** them with [evaluators](../concepts/evaluators.md), including
  backfilling evaluations over your whole history.
* **Group** them into [cohorts](../concepts/cohorts.md) and run
  [experiments](../concepts/experiments.md) against them.
* **Replay** them — with one honest caveat. Replay re-runs *your agent's
  real code*, which the trace itself doesn't contain. Register the agent
  version whose code produced the traces (its run command), and replay
  works exactly as for recorded sessions: recorded tool calls answered
  from the imported history, everything else per your
  [tool policy](../guides/tool-policies.md).

Other formats: an importer is ~a page of Python — a callable that parses
your export bytes into sessions. `kitaru importer scaffold my-format`
gives you the skeleton; the contract is in
[Import Langfuse traces](../guides/import-langfuse-traces.md).

## Next

Evaluate your imported history with your first evaluator —
[Write an evaluator](../guides/write-an-evaluator.md) — then pick the
sessions that matter into a cohort and put a change to the test with
[Build a regression suite from production](../guides/regression-suite.md).
