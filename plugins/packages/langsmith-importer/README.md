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

## Links

- [Kitaru documentation](https://docs.zenml.io/kitaru)
- [Source code](https://github.com/zenml-io/kitaru)
- [Issue tracker](https://github.com/zenml-io/kitaru/issues)

Licensed under Apache-2.0.
