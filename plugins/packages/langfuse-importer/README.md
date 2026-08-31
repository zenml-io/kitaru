# Kitaru Langfuse importer

Import Langfuse JSON and JSONL trace exports as Kitaru sessions. This package backs the built-in `kitaru/langfuse` importer and runs on a Kitaru worker, so the export is parsed in your environment.

Most users do not install or call this package directly. Start a Kitaru worker, then select the built-in importer:

```bash
kitaru session import langfuse-export.jsonl \
  --importer kitaru/langfuse@latest \
  --agent support-agent@latest \
  --wait
```

The importer understands Langfuse trace, observation, and ingestion-event records. It preserves hierarchy, timing, models, token usage, cost, and source payloads when the export provides them. Re-importing the same source identity skips sessions that Kitaru already stores.

See the [Langfuse import guide](https://docs.zenml.io/kitaru/guides/import-langfuse-traces) for accepted formats, parameters, deduplication behavior, and fidelity limits.

## Malformed exports

The importer rejects a grouped session with invalid costs, token counts, model fields, or payload text that cannot be serialized, while preserving unrelated sessions. Costs must be finite and nonnegative, token counts must be nonnegative, and optional model/provider fields must be strings or null. A trace rejected before session grouping does not invalidate another trace sharing its session ID.

Nested observation trees support up to 64 nodes along any root-to-leaf path, counting the root as depth 1. The limit applies before and after inferred tool links. Tool-call scanning also has a cumulative depth budget of 64 across containers and decoded JSON strings; exceeding it rejects the session instead of silently dropping links. An outer document that cannot be decoded is rejected as an upload.

## Links

- [Kitaru documentation](https://docs.zenml.io/kitaru)
- [Source code](https://github.com/zenml-io/kitaru)
- [Issue tracker](https://github.com/zenml-io/kitaru/issues)

Licensed under Apache-2.0.
