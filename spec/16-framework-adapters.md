# 16. Framework Adapters

Kitaru is designed around **primitives first, frameworks second**.

That means you should always be able to use Kitaru directly with:

- `@kitaru.flow`
- `@kitaru.checkpoint`
- `kitaru.wait()`
- `kitaru.llm()`

Framework adapters exist to make existing agent code durable with less rewriting.

They are a convenience layer, not the core runtime model.

## What adapters are for

Adapters are useful when a team already has agent code written in a framework and wants:

- checkpointed execution
- replayability
- artifact capture
- cost and token visibility
- dashboard timelines
- compatibility with Kitaru waits, artifacts, and flow orchestration

## What adapters should not change

An adapter should not redefine Kitaru's execution model.

The durable execution model remains:

- rerun from the top
- replay prior durable outcomes
- suspend with `wait()`
- explicit durable boundaries

Adapters should fit into that model rather than invent their own.

## MVP boundary compatibility

Adapters must respect the MVP durable-boundary restrictions:

- **No nested checkpoint-within-checkpoint semantics** introduced by adapters
- **No `wait()` inside checkpoints** via adapter magic
- **Child events stay child events** — adapter-internal model and tool calls do not become independent replay boundaries
- `@kitaru.checkpoint` remains the real replay boundary; adapter activity is child-level visibility

This is a hard rule, not a suggestion. Adapters that bypass these restrictions would create ambiguous nested durable semantics.

## MVP adapter: PydanticAI

For the MVP, the main framework adapter is the **PydanticAI adapter**.

### Shape

```python
from pydantic_ai import Agent
from kitaru.adapters import pydantic_ai as kp

researcher = kp.wrap(
    Agent(
        "openai:gpt-4o",
        name="researcher",
        tools=[],
    )
)
```

### Example

```python
from pydantic_ai import Agent
from kitaru.adapters import pydantic_ai as kp

researcher = kp.wrap(
    Agent(
        "openai:gpt-4o",
        name="researcher",
    )
)

@kitaru.flow
def research(topic: str) -> str:
    result = researcher.run_sync(f"Research {topic}")
    return result.output
```

## What the PydanticAI adapter should do

At a high level, the adapter should make framework activity visible and durable in a way that fits Kitaru's execution model.

The core mapping is:

- **agent tool calls** map to checkpoint child events (type `tool_call`)
- **agent model requests** map to checkpoint child events (type `llm_call`)
- **agent loop iterations** map to the enclosing checkpoint's execution timeline

The adapter should help capture:

- model requests
- tool calls
- outputs
- usage and cost metadata
- useful timeline structure

## Replay boundary vs child event

This is the most important semantic question for adapters.

Kitaru should distinguish between:

- **replay boundaries**
- **timeline child events**

For the MVP, the cleanest rule is:

- the enclosing `@kitaru.checkpoint` remains the real replay boundary
- framework-internal model calls and tool calls show up as child events, artifacts, and metadata under that checkpoint

This keeps the runtime coherent.

Without that distinction, adapters risk creating ambiguous nested checkpoint semantics.

## Recommended pattern

The clearest pattern is:

- use `@kitaru.flow` for orchestration
- use `@kitaru.checkpoint` around meaningful framework-driven units of work
- let the adapter emit child artifacts, child events, and metadata inside that checkpoint

Example:

```python
from pydantic_ai import Agent
from kitaru.adapters import pydantic_ai as kp

research_agent = kp.wrap(Agent("openai:gpt-4o", name="researcher"))

@kitaru.checkpoint(type="llm_call")
def run_research(topic: str) -> str:
    result = research_agent.run_sync(f"Research {topic} thoroughly")
    return result.output

@kitaru.flow
def content_pipeline(topic: str) -> str:
    notes = run_research(topic)
    return notes
```

This makes the replay boundary explicit and keeps the adapter's job mostly about visibility and convenience.

## Observability with adapters

Adapters become especially valuable when paired with OpenTelemetry-native tracing.

For example, with PydanticAI plus Logfire:

- Kitaru can emit flow/checkpoint/wait spans
- PydanticAI can emit model/tool spans
- the result can appear as one coherent trace tree

That gives users:

- durable execution semantics from Kitaru
- rich model/tool observability from the framework

## What adapters may capture

Depending on framework support, adapters may capture:

- model prompts and responses
- tool inputs and outputs
- token and cost information
- retries internal to the framework
- agent output values
- child-level metadata for dashboard visualization

These should be exposed in a way that helps inspection without confusing replay semantics.

## Future adapters

The broader platform may later support more adapters, but they should follow the same rule:

- adapt framework behavior into Kitaru's execution model
- do not introduce a second durable runtime model

Possible future adapters may include:

- other LLM or agent frameworks
- tool-runtime wrappers
- richer ecosystem integrations

But for the MVP, the focus should stay narrow.

## Rules

- adapters are convenience layers, not the primary runtime abstraction
- `@kitaru.flow` and `@kitaru.checkpoint` remain the core durable boundaries
- adapter-internal events should not muddy replay semantics
- adapters must not bypass MVP boundary restrictions (no nested checkpoints, no wait inside checkpoints)
- the MVP should favor explicit outer checkpoints around framework work

## MVP notes

For March, the PydanticAI adapter is enough to prove the shape:

- zero or low rewrite for existing agents
- useful child-level visibility
- compatibility with Kitaru flow/checkpoint orchestration
- clean coexistence with OpenTelemetry tracing
