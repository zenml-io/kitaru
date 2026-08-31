# Kitaru Logfire adapter

Import Logfire traces of wrapped agent runs into Kitaru.

## Install

```bash
uv add kitaru-logfire
```

## Use

The adapter uses the Logfire SDK already configured in your process and the Kitaru connection from your environment. Set `KITARU_AGENT_ID` to the agent imported sessions are created under and `LOGFIRE_TOKEN` to the write token the SDK records traces with.

The trace fetch goes through the Logfire Query API, which authenticates with a read token, a separate credential from the SDK's write token. Create one under your Logfire project settings and set it as `LOGFIRE_READ_TOKEN`. Then wrap your agent entrypoint in a `LogfireAdapter` and run it through the adapter.

```python
from kitaru_logfire import LogfireAdapter

adapter = LogfireAdapter()
result = adapter.run(my_agent, "Hello")
```

The adapter runs the function inside a Logfire trace, waits for Logfire to finish ingesting the trace, fetches it, and imports it as one Kitaru session. Use `run_async` for async functions. When the trace does not complete within the completeness timeout, the adapter creates a failed session carrying the trace id.

## Links

- [Kitaru documentation](https://docs.zenml.io/kitaru)
- [Source code](https://github.com/zenml-io/kitaru)
- [Issue tracker](https://github.com/zenml-io/kitaru/issues)

Licensed under Apache-2.0.
