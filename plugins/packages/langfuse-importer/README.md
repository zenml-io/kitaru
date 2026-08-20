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

## Links

- [Kitaru documentation](https://docs.zenml.io/kitaru)
- [Source code](https://github.com/zenml-io/kitaru)
- [Issue tracker](https://github.com/zenml-io/kitaru/issues)

Licensed under Apache-2.0.
