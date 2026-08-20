# Kitaru Braintrust importer

Import Braintrust project-log and UI exports as Kitaru sessions. This package backs the built-in `kitaru/braintrust` importer and runs on a Kitaru worker, so the export is parsed in your environment.

Most users do not install or call this package directly. Start a Kitaru worker, then select the built-in importer:

```bash
kitaru session import braintrust-logs.jsonl \
  --importer kitaru/braintrust@latest \
  --agent support-agent@latest \
  --wait
```

The importer accepts Braintrust JSON and JSONL export shapes, preserves source hierarchy and evidence when present, and records fidelity warnings for incomplete exports. Re-importing the same source identity skips sessions that Kitaru already stores.

See the [Braintrust import guide](https://docs.zenml.io/kitaru/guides/import-braintrust-traces) for accepted formats, grouping parameters, deduplication behavior, and fidelity limits.

## Links

- [Kitaru documentation](https://docs.zenml.io/kitaru)
- [Source code](https://github.com/zenml-io/kitaru)
- [Issue tracker](https://github.com/zenml-io/kitaru/issues)

Licensed under Apache-2.0.
