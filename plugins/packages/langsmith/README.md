# Kitaru LangSmith adapter

Import LangSmith traces of wrapped agent runs into Kitaru.

## Install

```bash
uv add kitaru-langsmith
```

## Use

The adapter uses the LangSmith SDK already configured in your process and the Kitaru connection from your environment. Set `KITARU_AGENT_ID` to the agent imported sessions are created under and `LANGSMITH_API_KEY` (plus `LANGSMITH_ENDPOINT` for a self-hosted instance) to the credentials the trace fetch authenticates with, then wrap your agent entrypoint in a `LangSmithAdapter` and run it through the adapter.

```python
from kitaru_langsmith import LangSmithAdapter

adapter = LangSmithAdapter()
result = adapter.run(my_agent, "Hello")
```

The adapter runs the function inside a LangSmith trace, waits for LangSmith to finish ingesting the trace, fetches it, and imports it as one Kitaru session. Use `run_async` for async functions. When the trace does not complete within the completeness timeout, the adapter creates a failed session carrying the trace id.

## Links

- [Kitaru documentation](https://docs.zenml.io/kitaru)
- [Source code](https://github.com/zenml-io/kitaru)
- [Issue tracker](https://github.com/zenml-io/kitaru/issues)

Licensed under Apache-2.0.
