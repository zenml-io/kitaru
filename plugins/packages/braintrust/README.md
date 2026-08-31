# Kitaru Braintrust adapter

Import Braintrust traces of wrapped agent runs into Kitaru.

## Install

```bash
uv add kitaru-braintrust
```

## Use

The adapter uses the Braintrust logger already configured in your process and the Kitaru connection from your environment. Set `KITARU_AGENT_ID` to the agent imported sessions are created under and `BRAINTRUST_API_KEY` to the key the trace fetch authenticates with, then wrap your agent entrypoint in a `BraintrustAdapter` and run it through the adapter.

```python
from kitaru_braintrust import BraintrustAdapter

adapter = BraintrustAdapter()
result = adapter.run(my_agent, "Hello")
```

The adapter runs the function inside a Braintrust span, waits for Braintrust to finish ingesting the trace, fetches it, and imports it as one Kitaru session. Use `run_async` for async functions. When the trace does not complete within the completeness timeout, the adapter creates a failed session carrying the trace id.

## Links

- [Kitaru documentation](https://docs.zenml.io/kitaru)
- [Source code](https://github.com/zenml-io/kitaru)
- [Issue tracker](https://github.com/zenml-io/kitaru/issues)

Licensed under Apache-2.0.
