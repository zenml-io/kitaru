# 15. Observability

Kitaru does **not** own observability. It emits telemetry in a standard way so teams can use the backend they already prefer.

The intended model is:

- **Kitaru** owns durable execution semantics
- **OpenTelemetry** owns tracing and export
- your chosen backend owns storage, querying, and visualization

This keeps the runtime focused and avoids inventing a parallel observability product.

## Philosophy

Kitaru should be **OpenTelemetry-native**.

That means:

- no proprietary tracing model is required to use it
- `kitaru.configure()` should not become a bag of tracing options
- Kitaru spans should fit into normal OTel pipelines
- tracing should compose well with frameworks like PydanticAI

## What Kitaru should emit

Kitaru should emit spans and structured metadata around the major runtime boundaries.

### Flow spans

A flow should emit spans for:

- flow start
- flow completion
- flow failure
- flow retry attempt (same execution)

Useful attributes include:

- execution ID
- flow name
- status
- stack
- duration

### Checkpoint spans

A checkpoint should emit spans for:

- checkpoint start
- checkpoint completion
- checkpoint failure
- retry attempts if represented separately

Useful attributes include:

- checkpoint name
- checkpoint type
- execution ID
- attempt count
- duration
- artifact references where appropriate

### Wait spans

A wait should emit spans or events for:

- wait entered
- execution suspended
- active timeout reached / resources released
- input received
- execution resumed

Useful attributes include:

- wait name
- schema
- prompt
- waiting duration
- active timeout duration
- actor or source metadata if available

### LLM spans

`kitaru.llm()` should emit spans or child events for:

- model request
- model response
- usage and cost metadata

Useful attributes include:

- resolved model
- provider
- tokens input
- tokens output
- cost
- latency

### Retry and resume events

Because retry and resume are same-execution operations, the telemetry model should represent them within one logical execution trace:

- **retry requested** — a retry was initiated for a failed checkpoint or flow
- **retry attempt started** — the actual retry execution began
- **failed attempt segment** — marks a portion of the execution that failed before retry
- **resume accepted** — wait input was provided and the execution will continue
- **resumed continuation** — execution picked up again after suspend

These events help observability backends show the Temporal-like "one execution with a visible gap/red segment" story rather than making retries look like separate executions.

## Example setup

### Logfire

```python
import logfire
import kitaru

logfire.configure()

@kitaru.flow
def my_agent(prompt: str) -> str:
    return kitaru.llm(prompt, model="fast")
```

### Generic OTel

```python
from opentelemetry.sdk.trace.export import ConsoleSpanExporter
# ... standard OTel setup ...

import kitaru

@kitaru.flow
def my_agent(prompt: str) -> str:
    return kitaru.llm(prompt, model="fast")
```

Kitaru should work with ordinary OTel configuration rather than requiring a Kitaru-specific tracing backend.

## Dashboard vs tracing backend

The Kitaru dashboard and an OTel backend serve different purposes.

### Kitaru dashboard

Best for:

- execution status
- timeline of durable calls
- wait states
- replay and inspection
- artifacts and metadata

### OTel backend

Best for:

- trace-level latency analysis
- distributed system visibility
- correlation with other services
- long-term telemetry workflows

They complement each other rather than compete.

## Framework composition

Observability becomes especially useful when Kitaru is used with framework adapters.

For example, with PydanticAI:

- Kitaru can provide flow/checkpoint/wait spans
- PydanticAI can provide model/tool-level spans
- both can show up in the same trace tree if wired through the same OTel setup

That gives users a good combination of:

- durable execution semantics
- detailed framework-level tracing

## Rules

- Kitaru should emit OTel-compatible spans and attributes
- Kitaru should not require a proprietary observability backend
- tracing config should remain outside `kitaru.configure()`
- dashboard state and observability traces should remain complementary concerns

## MVP notes

For March, the important part is not a huge observability surface. It is that Kitaru emits clean enough spans and metadata that teams can plug it into existing OTel workflows without friction.
