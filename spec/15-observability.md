# 15. Observability

Kitaru does **not** own observability. It provides structured logging and metadata, and defers to external backends for deeper tracing and visualization.

## MVP: Global log store model

For the MVP, observability is centered on the **global log store** concept (see chapter 9):

- Runtime logs default to the artifact store
- Users can optionally switch the global log backend to an OTel-compatible provider (e.g. Datadog) via `kitaru log-store set`
- `kitaru.log()` provides structured metadata attachment for cost, quality, usage, and custom annotations
- The Kitaru dashboard shows execution timelines, artifacts, and metadata

This gives teams basic observability without requiring additional infrastructure setup.

## Why OTel is not MVP

Full OpenTelemetry-native tracing would require FastAPI middleware injection at the ZenML server level, which is not feasible for the MVP timeline. Rather than shipping half-baked OTel support, the MVP defers to the global log store model for basic observability needs.

## Future direction: OpenTelemetry-native tracing

In the future, Kitaru should become **OpenTelemetry-native**. That means:

- no proprietary tracing model is required to use it
- `kitaru.configure()` should not become a bag of tracing options
- Kitaru spans should fit into normal OTel pipelines
- tracing should compose well with frameworks like PydanticAI

### What Kitaru would emit (future)

When OTel support is added, Kitaru should emit spans and structured metadata around the major runtime boundaries:

- **Flow spans** — flow start, completion, failure, retry attempt
- **Checkpoint spans** — checkpoint start, completion, failure, retry attempts
- **Wait spans/events** — wait entered, execution suspended, active timeout reached, input received, execution resumed
- **LLM spans** — model request, model response, usage and cost metadata
- **Retry and resume events** — retry requested, retry attempt started, failed attempt segment, resume accepted, resumed continuation

### Example setup (future)

```python
# Logfire
import logfire
import kitaru
from kitaru import flow

logfire.configure()

@flow
def my_agent(prompt: str) -> str:
    return kitaru.llm(prompt, model="fast")
```

```python
# Generic OTel
from opentelemetry.sdk.trace.export import ConsoleSpanExporter
# ... standard OTel setup ...

import kitaru
from kitaru import flow

@flow
def my_agent(prompt: str) -> str:
    return kitaru.llm(prompt, model="fast")
```

## Dashboard vs tracing backend (future)

The Kitaru dashboard and an OTel backend serve different purposes:

- **Kitaru dashboard** — execution status, timeline of durable calls, wait states, replay and inspection, artifacts and metadata
- **OTel backend** — trace-level latency analysis, distributed system visibility, correlation with other services, long-term telemetry workflows

They complement each other rather than compete.

## Framework composition (future)

Observability becomes especially useful when Kitaru is used with framework adapters. For example, with PydanticAI:

- Kitaru can provide flow/checkpoint/wait spans
- PydanticAI can provide model/tool-level spans
- both can show up in the same trace tree if wired through the same OTel setup

## Rules

- Kitaru should not require a proprietary observability backend
- tracing config should remain outside `kitaru.configure()`
- dashboard state and observability traces should remain complementary concerns
- for MVP, rely on the global log store and structured metadata rather than full OTel span emission

## MVP notes

For the MVP, the important part is that Kitaru stores structured metadata (via `kitaru.log()`) and runtime logs (via the global log store) in a way that teams can access and inspect. Full OTel-native span emission is deferred to a post-MVP release.
