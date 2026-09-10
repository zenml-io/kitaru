---
description: Bring the traces you already collect. Langfuse stays your system of record, Kitaru gets a runnable copy.
icon: file-import
---

# Import your traces

You don't have to run a single request through Kitaru to start. If your agent already logs to Langfuse (or any tracing system you can export from), your history is the raw material: import it, and every trace lands as a [session](../concepts/agents-and-sessions.md), the same object a live-recorded run produces, ready to replay and evaluate like any other.

This is the honest division of labor: **your observability stack stays your system of record**. Kitaru takes a copy of the runs you care about and makes them runnable: the incident from Tuesday becomes a test case, last month's traffic becomes a regression population.

Imports execute on a [worker](../concepts/workers.md) in your environment. The export file is parsed by your worker, not by anything outside your infrastructure.

## 1. Register the agent the traces belong to

Importers for **Langfuse, LangSmith, Braintrust, Logfire, Arize Phoenix, and a native JSONL format** are built in, registered at server startup under the `kitaru/` namespace, so there is no importer code to write for those. For existing Mastra exports, register the supplied parser using the [Mastra import workflow](../adapters/mastra.md). Other formats come in through a [custom importer](../guides/custom-importer.md).

{% hint style="info" %}
**Traces in OpenTelemetry format?** There is no OTel ingestion endpoint yet. Export the spans and convert them to [Kitaru JSONL](../guides/importing-sessions.md), or wrap that conversion in a [custom importer](../guides/custom-importer.md) so your exports import directly; the `kitaru-importer-builder` skill drafts one from a sample export.
{% endhint %}

Register the agent these traces belong to, if you haven't:

```bash
kitaru agent register support-agent --command "python support.py"
```

## 2. Import the export

Export your traces from Langfuse as JSONL (trace, observation, and ingestion-event records are all understood), start a [worker](../concepts/workers.md) in another terminal (`kitaru worker start`), then:

```bash
kitaru session import langfuse-export.jsonl \
  --importer kitaru/langfuse@latest \
  --agent support-agent@latest \
  --params '{"source_instance":"my-langfuse-project"}' \
  --tag imported-baseline \
  --media-type application/x-ndjson \
  --wait
```

`--tag` labels every session this import creates (repeat it for more than one label), so later steps can select them as a group (`kitaru session evaluate --tag imported-baseline ...`) without copying IDs around. (Tagging happens once the import completes, which is why `--tag` requires `--wait`.)

The final receipt reports what happened: sessions `created`, `skipped`, and `failed`, with samples of the failures. Each imported trace becomes one session (`origin: imported`) with its observations as nodes: model calls with token usage and cost, tool calls with arguments and results. List them:

```bash
kitaru session list --agent support-agent --origin imported
```

The same import is two calls on the [Python client](../deploy/configuration.md) when you'd rather script it: upload the export with `client.blobs.upload(...)`, then create the import with `client.imports.create(ImportCreateRequest( importer="langfuse", agent_id=..., payload_blob_id=...))`.

## Or skip the export

Langfuse, LangSmith, Braintrust, Logfire, and Arize Phoenix importers can fetch traces themselves instead of you exporting a file first. Omit the file argument, name a time window instead, and the worker calls the provider's API directly:

```bash
kitaru session import \
  --importer kitaru/langfuse@latest \
  --agent support-agent@latest \
  --since 7d \
  --tag imported-baseline --wait
```

`--since` and `--until` accept an ISO 8601 timestamp or a relative duration such as `7d`, `12h`, or `30m`. `--trace-id` fetches exactly the trace ids you name instead of a window. These merge with `--query` into one `ImportQuery` (`kitaru.api_models.v1.imports`), validated before the import is created, and provider-specific keys pass through untouched. The fetch runs on your worker, the same way the parse does, so provider credentials never leave your infrastructure. A [connection](../guides/provider-connections.md) named with `--connection`, or the provider's default connection, supplies them, and the worker's own environment is still the fallback when neither is set. Each provider's guide lists its query keys and the environment variables the fetch reads.

Use the file upload from step 2 when you already have an export, when you'd rather not hand a worker live API credentials, or for the Kitaru JSONL importer, which only accepts uploaded files. Use the API fetch to skip the export step for the five provider importers.

## Source identity

The five provider importers choose project identity in the same order: `params.source_instance`, the provider-specific parameter below, then project identity embedded in the export. If none is available, the affected trace or session fails with an error showing the `--params` remedy. Filenames and generic provider names are not identity fallbacks.

| Importer | Alternative parameter |
| --- | --- |
| Langfuse | `project_id` |
| LangSmith | `project_name` |
| Braintrust | `project_id` |
| Logfire | `project_id` |
| Arize Phoenix | `project` |

Identity values must be strings. Surrounding whitespace is removed; `null` and empty or whitespace-only strings count as absent. Other types are rejected, including when an explicit override is available. Conflicting embedded project identities fail the affected trace or session even with an override. Each importer keeps its existing rules for grouping traces into sessions.

Use the same identity value for every import from the same source project, including file and API imports. These parameters do not look up project names or convert them to IDs: `support` and `project-123` are different identities even if they describe the same provider project. `--query` selects what to fetch; `--params` supplies parser options. If fetched records do not carry identity, supply it in `--params`. Phoenix includes its selected API project in the fetched payload.

The native Kitaru JSONL importer is different: each record already supplies its final `external_id`, which the importer preserves.

### Existing imports

- For an earlier Langfuse or Braintrust import that used a filename stem, supply that stem explicitly as `source_instance` to keep the same identity.
- Braintrust now honors explicit parameters ahead of embedded project IDs. If an earlier import ignored your explicit parameter, omit it or set it to the previously selected embedded ID to retain the same identity.
- For a Logfire import that used the old `logfire` fallback, supply `"source_instance":"logfire"` explicitly to retain that prefix.
- Phoenix now prefixes the trace ID with project identity. Previously imported bare trace IDs do not match the new IDs, so importing overlapping traces into the same agent creates additional sessions. Existing sessions are not rewritten automatically.

Trimming whitespace also changes any earlier identity that included surrounding whitespace. New identity validation does not reconcile previously imported sessions.

## Re-runs skip existing sessions

Every imported session keeps its source identity (`imported_from` + `external_id`). This pair is unique per destination agent. Importing the same export twice with the same identity adds the new batch to the session that's already there instead of duplicating it. Nodes are matched by their own external id: an existing node is replaced and a new one is added. The receipt reports these sessions as `skipped`, meaning they already existed and received the batch. Changing project identity or the grouping key can create additional sessions.

{% hint style="warning" %} An import stores the parsed trace content (prompts, tool arguments, tool results) on your Kitaru server. The server is self-hosted, but check your own access and retention rules before importing exports that contain customer data. {% endhint %}

## What imported sessions can do

Everything recorded sessions can:

- **Inspect** them: nodes, cost, and token rollups all populate.
- **Evaluate** them with [evaluators](../concepts/evaluators.md), including backfilling evaluations over your whole history.
- **Group** them into [cohorts](../concepts/cohorts.md) and run [experiments](../concepts/experiments.md) against them.
- **Replay** them, with one honest caveat. Replay re-runs _your agent's real code_, which the trace itself doesn't contain. Register the agent version whose code produced the traces (its run command), and replay works exactly as for recorded sessions: recorded tool calls answered from the imported history, everything else per your [tool policy](../guides/tool-policies.md).

Other formats: an importer is about a page of Python, a callable that parses your export bytes into sessions, and the `kitaru-importer-builder` agent skill will draft it for you. See [No importer for your format](../guides/custom-importer.md).

## Next

Evaluate your imported history with your first evaluator ([Write an evaluator](../guides/write-an-evaluator.md)), then pick the sessions that matter into a cohort and put a change to the test with [Build a regression suite from production](../guides/regression-suite.md).
