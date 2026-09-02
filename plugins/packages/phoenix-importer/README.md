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

## Adapter

Install the package with the `adapter` extra to use the adapter, which adds the provider SDK it needs:

```bash
uv add "kitaru-phoenix-importer[adapter]"
```

The package also ships an adapter that imports Arize Phoenix traces of wrapped agent runs. The adapter uses the OTel tracer provider Phoenix tracing already configured in your process, for example via `phoenix.otel.register()`, and the Kitaru connection from your environment.

The trace fetch goes through the Phoenix client, which reads `PHOENIX_ENDPOINT` (or `PHOENIX_COLLECTOR_ENDPOINT`), `PHOENIX_API_KEY`, and the project name from `PHOENIX_PROJECT` from your environment. Fetching by trace id requires a Phoenix server >= 13.9.0. Then wrap your agent entrypoint in a `PhoenixAdapter` and run it through the adapter.

```python
from kitaru_phoenix_importer import PhoenixAdapter

adapter = PhoenixAdapter()
result = adapter.run(my_agent, "Hello")
```

The adapter runs the function inside an OTel trace, waits for Phoenix to finish ingesting the trace, fetches it, and imports it as one Kitaru session. Use `run_async` for async functions. When the trace does not complete within the completeness timeout, the adapter creates a failed session carrying the trace id.

## Links

- [Kitaru documentation](https://docs.zenml.io/kitaru)
- [Source code](https://github.com/zenml-io/kitaru)
- [Issue tracker](https://github.com/zenml-io/kitaru/issues)

Licensed under Apache-2.0.
