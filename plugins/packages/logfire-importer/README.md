# Kitaru Logfire importer

Import Logfire records-query JSON and NDJSON exports as Kitaru sessions. This package backs the built-in `kitaru/logfire` importer and runs on a Kitaru worker, so the export is parsed in your environment.

Most users do not install or call this package directly. Start a Kitaru worker, then select the built-in importer:

```bash
kitaru session import logfire-records.jsonl \
  --importer kitaru/logfire@latest \
  --agent support-agent@latest \
  --wait
```

The importer rebuilds span hierarchy and conservatively identifies model and tool calls from OpenTelemetry attributes. Supply a stable source identity when an export does not contain one, especially when importing from more than one Logfire project.

See the [Logfire import guide](https://docs.zenml.io/kitaru/guides/import-logfire-traces) for accepted formats, parameters, deduplication behavior, and fidelity limits.

Nested span paths support up to 64 levels, counting a root as level 1. Invalid costs, token counts, embedded JSON, and text that cannot serialize reject the affected grouped session; unrelated sessions continue. A trace rejected before grouping does not prevent a valid sibling trace from importing into the same session.

## Links

- [Kitaru documentation](https://docs.zenml.io/kitaru)
- [Source code](https://github.com/zenml-io/kitaru)
- [Issue tracker](https://github.com/zenml-io/kitaru/issues)

Licensed under Apache-2.0.
