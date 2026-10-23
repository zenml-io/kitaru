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

## Keep a simulator's prompt

By default, task inputs from `KITARU_TASK_INPUTS` or the task spec replace the run's prompt and supply recorded message history. If a benchmark task ID is an input to your simulator rather than a model prompt, disable that substitution:

```python
agent = KitaruAgent(pydantic_agent, use_task_inputs_as_prompt=False)
result = await agent.run(simulator_prompt, message_history=simulator_history)
```

The adapter preserves the caller's prompt and message history while recording task inputs as the session inputs and root node inputs. An explicit `replay.override.prompt` still takes precedence, and model overrides and tool policies continue to apply.

## Attach grades after a run

The adapter completes the session before the run returns, so later updates to its outputs or metadata return HTTP 409. Use `client.sessions.create_evaluations(session_id, request)` to attach a manual benchmark grade to a completed session, including from its own agent task before the process exits. Manual evaluations do not contribute to experiment aggregates. For experiment metrics, include the grade in the final output through an `output_validator`, then read it with a registered evaluator. See the [adapter guide](https://docs.zenml.io/kitaru/adapters/pydantic-ai) for the request example and evaluator details.

## Model costs

The adapter estimates each completed LLM call's USD cost from its resolved model, provider, token usage, and request time. Pricing comes from the bundled `genai-prices` catalog. Unsupported models and providers leave the cost unset without failing the agent run. The recorded node attributes describe whether pricing was estimated or unavailable.

Supply `cost_calculator` for private models, negotiated rates, or provider billing rules. The callback receives a `PydanticAIUsageSummary` and returns a non-negative USD amount. A supplied callback takes priority over the bundled catalog.

```python
from decimal import Decimal

from kitaru_pydantic_ai import KitaruAgent, PydanticAIUsageSummary


def calculate_cost(usage: PydanticAIUsageSummary) -> Decimal:
    uncached_input = usage.input_tokens - usage.cached_input_tokens
    return (
        Decimal(uncached_input) * Decimal("0.000002")
        + Decimal(usage.cached_input_tokens) * Decimal("0.0000002")
        + Decimal(usage.output_tokens) * Decimal("0.000008")
    )


agent = KitaruAgent(pydantic_agent, cost_calculator=calculate_cost)
```

Set `estimate_costs=False` when cost estimation should be disabled. This does not disable a supplied `cost_calculator`.

See the [PydanticAI adapter guide](https://docs.zenml.io/kitaru/adapters/pydantic-ai) for the recording lifecycle, replay overrides, tool policies, payload handling, and failure behavior.

## Links

- [Kitaru documentation](https://docs.zenml.io/kitaru)
- [Source code](https://github.com/zenml-io/kitaru)
- [Issue tracker](https://github.com/zenml-io/kitaru/issues)

Licensed under Apache-2.0.
