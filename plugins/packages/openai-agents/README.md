# Kitaru OpenAI Agents adapter

Record OpenAI Agents SDK runs with Kitaru. The adapter is non-streaming: it preserves the native `RunResult` while recording each run as a Kitaru session with root and observed activity nodes.

## Install

```bash
uv add kitaru-openai-agents
```

## Use

```python
import uuid

from agents import Agent

from kitaru_openai_agents import KitaruRunner

agent = Agent(
    name="assistant",
    instructions="Answer in one concise sentence.",
    model="gpt-5-nano",
)
runner = KitaruRunner(
    agent_id=uuid.UUID("018f0000-0000-7000-8000-000000000100"),
    session_name="openai-agents-example",
)
result = runner.run_sync(agent, "Hello")
print(result.final_output)
```

`KitaruRunner.run()` and `KitaruRunner.run_sync()` accept the same run arguments as the OpenAI Agents SDK `Runner`. Kitaru workers provide task, replay, and authentication context through the standard task environment.
