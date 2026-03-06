# 8. `kitaru.llm()` — Durable LLM Call

## What it does

`kitaru.llm()` is a **thin convenience wrapper** for making a single LLM call with automatic tracking.

It is intentionally narrow.

It is **not**:

- an agent loop
- a tool execution runtime
- a conversation state manager
- a replacement for a framework like PydanticAI

It is the ergonomic path for:

- one-off model calls inside a flow
- one-off model calls inside a checkpoint
- automatic token, latency, and cost tracking
- automatic prompt/response artifact creation

## Signature

```python
response = kitaru.llm(
    prompt,                  # str or list[dict]
    model=None,              # model alias or concrete model identifier
    system=None,             # optional system prompt
    temperature=None,        # optional
    max_tokens=None,         # optional
    name=None,               # optional display name
)
```

## Inputs

`prompt` may be:

- a plain string
- a chat-style messages list such as `list[dict]`

Examples:

```python
response = kitaru.llm("Summarize this article")
```

```python
response = kitaru.llm(
    [
        {"role": "system", "content": "You are a code reviewer."},
        {"role": "user", "content": f"Review this code:\n{code}"},
    ],
    model="fast",
)
```

## Semantic contract

`kitaru.llm()` is a **durable LLM call helper**, but its exact runtime behavior depends on where it is called.

### Called directly inside a flow

If `kitaru.llm()` is called directly in a flow body, it should behave like a small synthetic durable call boundary.

That means it should:

- produce durable prompt/response artifacts
- log usage metadata
- participate in replay
- be individually visible in the execution timeline

### Called inside a checkpoint

If `kitaru.llm()` is called inside an existing checkpoint, it should **not** create a nested replay boundary in the MVP.

Instead, it should behave as a **child event** of the current checkpoint:

- prompt and response are still captured as artifacts
- tokens, cost, model, and latency are still logged
- the enclosing checkpoint remains the actual replay boundary

This avoids muddy nested checkpoint semantics while still giving visibility in the dashboard.

## Behavior

`kitaru.llm()` should:

- resolve the model through the provider abstraction layer
- make one model call
- create prompt and response artifacts
- log usage metadata such as tokens, cost, and latency
- return the model's text response

In other words, it is a tracked one-shot model call, not a workflow engine.

## Provider abstraction

Kitaru needs an abstraction layer over model providers to make `kitaru.llm()` work across different providers (OpenAI, Anthropic, etc.).

The MVP direction is likely a wrapper over an existing multi-provider SDK (e.g. a LiteLLM-like approach under the hood), but the exact backend shape is **not yet finalized**.

What is stable:

- `kitaru.llm()` is the user-facing call surface
- `model=` accepts either an alias or a concrete `provider:model` string
- cost, token, and latency tracking are core requirements

The current direction is that the provider abstraction is an **LLM model stack component** — one of the four to five core components in a Kitaru stack (see [Chapter 4](04-connection-stacks-and-configuration.md)). This means model provider credentials and configuration live in the stack, alongside the runner, artifact store, and container registry.

What is still being decided:

- the exact flavor system for different providers (OpenAI, Anthropic, etc.)
- whether to build the provider layer in-house or wrap an existing library
- the interaction between stack-level model config and call-time `model=` overrides

## Model resolution

`model=` may be:

- a configured model alias such as `"fast"` or `"smart"`
- a concrete provider/model string such as `"openai:gpt-4o"`

Model resolution should happen against the **frozen execution spec**, not ambient runtime globals.

That means:

- resume is stable
- replay provenance is clear
- the dashboard can show the actual resolved model used

## Examples

**Simple call inside a flow:**

```python
@kitaru.flow
def my_agent(topic: str) -> str:
    outline = kitaru.llm(f"Create an outline for: {topic}", model="fast")
    draft = kitaru.llm(f"Write an article from this outline:\n{outline}", model="smart")
    return draft
```

**Call inside a checkpoint:**

```python
@kitaru.checkpoint(type="llm_call")
def summarize(text: str) -> str:
    return kitaru.llm(
        f"Summarize this in 3 bullet points:\n\n{text}",
        model="fast",
        name="summary_call",
    )
```

**Chat-style messages:**

```python
response = kitaru.llm(
    [
        {"role": "system", "content": "You are a concise technical editor."},
        {"role": "user", "content": draft_text},
    ],
    model="smart",
    temperature=0.2,
)
```

## What gets recorded

A `kitaru.llm()` call should typically produce:

- a **prompt artifact**
- a **response artifact**
- metadata such as:
    - resolved model
    - tokens input
    - tokens output
    - total tokens
    - cost
    - latency

This makes LLM calls visible and auditable without users manually wiring instrumentation.

## Replay behavior

`kitaru.llm()` participates in replay according to its enclosing durable boundary.

### If called directly in a flow

The LLM call can be replayed as its own durable call.

### If called inside a checkpoint

The enclosing checkpoint outcome is what governs replay.

That means if the enclosing checkpoint is replayed from history, the inner `llm()` call is not executed again.

If the enclosing checkpoint is re-executed live, the inner `llm()` call happens live again and emits fresh child artifacts and metadata.

## When to use `kitaru.llm()` vs `@kitaru.checkpoint`

Use `kitaru.llm()` when you want:

- a simple single model call
- automatic tracking
- minimal ceremony

Use `@kitaru.checkpoint` when you want:

- a true replay boundary
- tool use
- multiple model calls grouped together
- custom control flow
- framework-based agent behavior

In practice:

- `kitaru.llm()` is good for small one-shot calls
- `@kitaru.checkpoint` is the right durable unit for larger work

## Rules

- `kitaru.llm()` is for **one call**, not an agent loop
- inside a checkpoint, it should not become a nested replay boundary in the MVP
- model resolution should work through the provider abstraction layer
- usage metadata should be recorded automatically
- prompt and response should become typed artifacts

## MVP notes

- keep the API narrow and single-call
- do not overload `kitaru.llm()` with tool loops or conversation state
- nested-boundary semantics should stay simple: standalone in flow, child event in checkpoint
- provider/config architecture should remain intentionally flexible — do not fossilize implementation details that are still being decided
