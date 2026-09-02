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

## Adapter

Install the package with the `adapter` extra to use the adapter, which adds the provider SDK it needs:

```bash
uv add "kitaru-logfire-importer[adapter]"
```

The package also ships an adapter that imports Logfire traces of wrapped agent runs. The adapter uses the Logfire SDK already configured in your process and the Kitaru connection from your environment. Set `LOGFIRE_TOKEN` to the write token the SDK records traces with.

The trace fetch goes through the Logfire Query API, which authenticates with a read token, a separate credential from the SDK's write token. Create one under your Logfire project settings and set it as `LOGFIRE_READ_TOKEN`. Then wrap your agent entrypoint in a `LogfireAdapter` and run it through the adapter.

```python
from kitaru_logfire_importer import LogfireAdapter

adapter = LogfireAdapter()
result = adapter.run(my_agent, "Hello")
```

The adapter runs the function inside a Logfire trace, waits for Logfire to finish ingesting the trace, fetches it, and imports it as one Kitaru session. Use `run_async` for async functions. When the trace does not complete within the completeness timeout, the adapter creates a failed session carrying the trace id.

## Links

- [Kitaru documentation](https://docs.zenml.io/kitaru)
- [Source code](https://github.com/zenml-io/kitaru)
- [Issue tracker](https://github.com/zenml-io/kitaru/issues)

Licensed under Apache-2.0.
