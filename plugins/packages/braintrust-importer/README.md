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

## Adapter

The package also ships an adapter that imports Braintrust traces of wrapped agent runs. The adapter uses the Braintrust logger already configured in your process and the Kitaru connection from your environment. Set `KITARU_AGENT_ID` to the agent imported sessions are created under and `BRAINTRUST_API_KEY` to the key the trace fetch authenticates with, then wrap your agent entrypoint in a `BraintrustAdapter` and run it through the adapter.

```python
from kitaru_braintrust_importer import BraintrustAdapter

adapter = BraintrustAdapter()
result = adapter.run(my_agent, "Hello")
```

The adapter runs the function inside a Braintrust span, waits for Braintrust to finish ingesting the trace, fetches it, and imports it as one Kitaru session. Use `run_async` for async functions. When the trace does not complete within the completeness timeout, the adapter creates a failed session carrying the trace id.

## Links

- [Kitaru documentation](https://docs.zenml.io/kitaru)
- [Source code](https://github.com/zenml-io/kitaru)
- [Issue tracker](https://github.com/zenml-io/kitaru/issues)

Licensed under Apache-2.0.
