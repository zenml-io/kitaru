# Kitaru Phoenix adapter

Import Arize Phoenix traces of wrapped agent runs into Kitaru.

## Install

```bash
uv add kitaru-phoenix
```

## Use

The adapter uses the OTel tracer provider Phoenix tracing already configured in your process, for example via `phoenix.otel.register()`, and the Kitaru connection from your environment. Set `KITARU_AGENT_ID` to the agent imported sessions are created under.

The trace fetch goes through the Phoenix client, which reads `PHOENIX_ENDPOINT` (or `PHOENIX_COLLECTOR_ENDPOINT`), `PHOENIX_API_KEY`, and the project name from `PHOENIX_PROJECT` from your environment. Fetching by trace id requires a Phoenix server >= 13.9.0. Then wrap your agent entrypoint in a `PhoenixAdapter` and run it through the adapter.

```python
from kitaru_phoenix import PhoenixAdapter

adapter = PhoenixAdapter()
result = adapter.run(my_agent, "Hello")
```

The adapter runs the function inside an OTel trace, waits for Phoenix to finish ingesting the trace, fetches it, and imports it as one Kitaru session. Use `run_async` for async functions. When the trace does not complete within the completeness timeout, the adapter creates a failed session carrying the trace id.

## Links

- [Kitaru documentation](https://docs.zenml.io/kitaru)
- [Source code](https://github.com/zenml-io/kitaru)
- [Issue tracker](https://github.com/zenml-io/kitaru/issues)

Licensed under Apache-2.0.
