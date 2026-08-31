# Kitaru Langfuse adapter

Import Langfuse traces of wrapped agent runs into Kitaru.

## Install

```bash
uv add kitaru-langfuse
```

## Use

The adapter uses the Langfuse client already configured in your process. Wrap your agent entrypoint in a `LangfuseAdapter` and run it through the adapter.

```python
import uuid

from kitaru.client.api_client import KitaruAPIClient

from kitaru_langfuse import LangfuseAdapter

adapter = LangfuseAdapter(
    KitaruAPIClient(),
    agent_id=uuid.UUID("018f0000-0000-7000-8000-000000000100"),
)
result = adapter.run(my_agent, "Hello")
```

The adapter runs the function inside a Langfuse trace, waits for Langfuse to finish ingesting the trace, fetches it, and imports it as one Kitaru session. Use `run_async` for async functions. When the trace does not complete within the completeness timeout, the adapter creates a failed session carrying the trace id.

## Links

- [Kitaru documentation](https://docs.zenml.io/kitaru)
- [Source code](https://github.com/zenml-io/kitaru)
- [Issue tracker](https://github.com/zenml-io/kitaru/issues)

Licensed under Apache-2.0.
