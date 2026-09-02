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

## Adapter

Install the package with the `adapter` extra to use the adapter, which adds the provider SDK it needs:

```bash
uv add "kitaru-langsmith-importer[adapter]"
```

The package also ships an adapter that imports LangSmith traces of wrapped agent runs. The adapter uses the LangSmith SDK already configured in your process and the Kitaru connection from your environment. Set `LANGSMITH_API_KEY` (plus `LANGSMITH_ENDPOINT` for a self-hosted instance) to the credentials the trace fetch authenticates with, then wrap your agent entrypoint in a `LangSmithAdapter` and run it through the adapter.

```python
from kitaru_langsmith_importer import LangSmithAdapter

adapter = LangSmithAdapter()
result = adapter.run(my_agent, "Hello")
```

The adapter runs the function inside a LangSmith trace, waits for LangSmith to finish ingesting the trace, fetches it, and imports it as one Kitaru session. Use `run_async` for async functions. When the trace does not complete within the completeness timeout, the adapter creates a failed session carrying the trace id.

## Links

- [Kitaru documentation](https://docs.zenml.io/kitaru)
- [Source code](https://github.com/zenml-io/kitaru)
- [Issue tracker](https://github.com/zenml-io/kitaru/issues)

Licensed under Apache-2.0.
