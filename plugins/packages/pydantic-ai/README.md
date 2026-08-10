# Kitaru PydanticAI adapter

Record and replay PydanticAI agent runs with Kitaru.

## Install

```bash
uv add "kitaru-pydantic-ai[openai]"
```

Install `kitaru-pydantic-ai` without the extra when your project provides another PydanticAI model implementation.

## Use

```python
import uuid

from pydantic_ai import Agent

from kitaru_pydantic_ai import KitaruAgent

agent = KitaruAgent(
    Agent("openai:gpt-5-nano"),
    agent_id=uuid.UUID("018f0000-0000-7000-8000-000000000100"),
)
result = agent.run_sync("Hello")
print(result.output)
```

The adapter accepts the same run arguments as the wrapped PydanticAI agent. Kitaru workers provide task, replay, and authentication context through the standard task environment.
