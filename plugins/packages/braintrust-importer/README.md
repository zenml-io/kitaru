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

For exports without project identity, pass `--params '{"source_instance":"my-project"}'` or use the `project_id` alias. `source_instance` takes precedence over `project_id`, and both override embedded project identity. Filenames do not determine source identity. Reuse the same value for repeated exports from the same project.

See the [Braintrust import guide](https://docs.zenml.io/kitaru/guides/import-braintrust-traces) for accepted formats, grouping parameters, deduplication behavior, and fidelity limits.

## Validation limits

Nested node trees support at most 64 nodes along a parent path, counting the root as level 1. Tool-activity scans have a separate limit of 64 container or embedded-JSON decoding steps. Costs must be finite and nonnegative, token counts must be nonnegative, and returned sessions must serialize as JSON.

Conflicting project IDs reject the complete Braintrust trace before grouping. An embedded project ID anywhere in a trace supplies identity when import params do not. Conflicting embedded project IDs also reject a grouped session, even when explicit params would give its traces the same source identity. A failure discovered before grouping leaves other traces available, even when they share a session key. Invalid fields or serialization failures found after grouping reject that grouped session; unrelated sessions still import.

## Adapter

Install the package with the `adapter` extra to use the adapter, which adds the provider SDK it needs:

```bash
uv add "kitaru-braintrust-importer[adapter]"
```

The package also ships an adapter that imports Braintrust traces of wrapped agent runs. The adapter uses the Braintrust logger already configured in your process and the Kitaru connection from your environment. Set `BRAINTRUST_API_KEY` to the key the trace fetch authenticates with, then wrap your agent entrypoint in a `BraintrustAdapter` and run it through the adapter.

```python
from kitaru_braintrust_importer.adapter import BraintrustAdapter

adapter = BraintrustAdapter()
result = adapter.run(my_agent, "Hello")
```

The adapter runs the function inside a Braintrust span, waits for Braintrust to finish ingesting the trace, fetches it, and imports it as one Kitaru session. Use `run_async` for async functions. When the trace does not complete within the completeness timeout, the adapter creates a failed session carrying the trace id.

## Links

- [Kitaru documentation](https://docs.zenml.io/kitaru)
- [Source code](https://github.com/zenml-io/kitaru)
- [Issue tracker](https://github.com/zenml-io/kitaru/issues)

Licensed under Apache-2.0.

Project identity values must be strings. Leading and trailing whitespace is removed; null, empty, and whitespace-only values are absent. Invalid identity types and conflicting embedded projects are rejected even when an explicit parameter would override them. API query fields select what to fetch; import params select the identity used by the parser. File and API imports use the same identity rules.
