# Kitaru Arize Phoenix importer

Import Arize Phoenix JSON and JSONL trace exports as Kitaru sessions. This package backs the built-in `kitaru/phoenix` importer and runs on a Kitaru worker, so the export is parsed in your environment.

Most users do not install or call this package directly. Start a Kitaru worker, then select the built-in importer:

```bash
kitaru session import phoenix-traces.jsonl \
  --importer kitaru/phoenix@latest \
  --agent support-agent@latest \
  --wait
```

The importer accepts Phoenix UI and CLI export shapes, preserves trace hierarchy and source evidence, and maps model and tool spans to Kitaru node types. Each Phoenix trace becomes one Kitaru session.

See [Import your traces](https://docs.zenml.io/kitaru/getting-started/import-your-traces) for the live import workflow. The [provider-specific guide in the Kitaru repository](https://github.com/zenml-io/kitaru/blob/develop/docs/book/guides/import-phoenix-traces.md) documents accepted Phoenix formats, deduplication behavior, and fidelity limits.

## Links

- [Kitaru documentation](https://docs.zenml.io/kitaru)
- [Source code](https://github.com/zenml-io/kitaru)
- [Issue tracker](https://github.com/zenml-io/kitaru/issues)

Licensed under Apache-2.0.
