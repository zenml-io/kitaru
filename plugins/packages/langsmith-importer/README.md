# Kitaru LangSmith importer

Import LangSmith run-query and bulk-export records as Kitaru sessions. This package backs the built-in `kitaru/langsmith` importer and runs on a Kitaru worker, so the export is parsed in your environment.

Most users do not install or call this package directly. Start a Kitaru worker, then select the built-in importer:

```bash
kitaru session import langsmith-runs.jsonl \
  --importer kitaru/langsmith@latest \
  --agent support-agent@latest \
  --wait
```

The importer accepts LangSmith JSON and JSONL export shapes, reconstructs run hierarchy, and groups traces using thread-like metadata or an explicit grouping path. Re-importing the same source identity skips sessions that Kitaru already stores.

See the [LangSmith import guide](https://docs.zenml.io/kitaru/guides/import-langsmith-traces) for accepted formats, grouping parameters, deduplication behavior, and fidelity limits.

## Validation limits

Nested node trees support at most 64 nodes along a parent path, counting the root as level 1. Tool-activity scans have a separate limit of 64 container or embedded-JSON decoding steps. Costs must be finite and nonnegative, token counts must be nonnegative, and returned sessions must serialize as JSON.

Duplicate run IDs reject the complete LangSmith trace before grouping, including identical repeated records. Reusing a run ID in another trace remains valid; a self-parent link is treated as a root. A failure discovered before grouping leaves other traces available, even when they share a session key. Invalid fields or serialization failures found after grouping reject that grouped session; unrelated sessions still import.

## Links

- [Kitaru documentation](https://docs.zenml.io/kitaru)
- [Source code](https://github.com/zenml-io/kitaru)
- [Issue tracker](https://github.com/zenml-io/kitaru/issues)

Licensed under Apache-2.0.
