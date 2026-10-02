# Kitaru LangGraph adapter

Record and replay LangGraph agent runs with Kitaru.

## Install

```bash
uv add kitaru-langgraph
```

Install `kitaru-langgraph[deepagents]` to also record agents built with `deepagents.create_deep_agent` through `KitaruGraphRunner.from_agent_factory`. The `langchain.agents.create_agent` factory and direct graph wrapping work without the extra.

## Model providers

This distribution does not install any model-provider packages. Model strings passed to `init_chat_model`, such as `"openai:gpt-5-nano"`, require the matching LangChain provider package (for example `langchain-openai`) in the agent environment. Install the provider package for each model your agent uses.

## Use

```python
import uuid

from langchain.agents import create_agent

from kitaru_langgraph import KitaruGraphRunner

runner = KitaruGraphRunner.from_agent_factory(
    create_agent,
    factory_kwargs={"model": "openai:gpt-5-nano", "tools": []},
    agent_id=uuid.UUID("018f0000-0000-7000-8000-000000000100"),
)
result = runner.invoke({"messages": [{"role": "user", "content": "Hello"}]})
print(result)
```

Wrap an existing compiled graph directly with `KitaruGraphRunner(graph)` when you build the graph yourself. The runner accepts the same invocation arguments as the wrapped runnable. Kitaru workers provide task, replay, and authentication context through the standard task environment.

Replay support depends on how the graph was constructed. See the [LangGraph adapter guide and capability matrix](https://docs.zenml.io/kitaru/adapters/langgraph) for supported invocation methods, overrides, tool policies, interrupts, and failure behavior.

LangGraph trace policies can omit middleware span inputs before callbacks receive them. With Deep Agents 0.7.9 and later, some built-in middleware spans therefore record `inputs: {}`; this does not prove that the hook received empty state. Model and tool call inputs remain recorded. See the [adapter guide](https://docs.zenml.io/kitaru/adapters/langgraph) for details.

Deep Agents runs its built-in middleware before custom middleware, including Kitaru's. A built-in rejection, such as a parallel write to the same file, therefore takes precedence over Kitaru's static or history tool policy. Keeping the same Deep Agents version when replaying avoids changes to these built-in rules across versions.

Tool attempts that return a native result without executing a tool are recorded with `execution: short_circuited`. An error `ToolMessage` remains a completed result with its native error status, so history replay can return it to the model rather than turn it into a raised exception.

## Links

- [Kitaru documentation](https://docs.zenml.io/kitaru)
- [Source code](https://github.com/zenml-io/kitaru)
- [Issue tracker](https://github.com/zenml-io/kitaru/issues)

Licensed under Apache-2.0.
