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

- resolve the model through the local model registry or as a concrete LiteLLM identifier
- normalize the prompt into LiteLLM's message format
- make one model call via `litellm.completion()`
- create prompt and response artifacts
- log usage metadata via `kitaru.log()` (tokens, cost, latency, resolved model)
- return the model's text response

In other words, it is a tracked one-shot model call, not a workflow engine.

## Backend engine: LiteLLM

`kitaru.llm()` is a thin wrapper around [LiteLLM](https://docs.litellm.ai/)'s `completion()` API. LiteLLM provides:

- a **unified chat completion API** across 100+ model providers (OpenAI, Anthropic, Cohere, Bedrock, etc.)
- **token counting and cost tracking** out of the box
- **native environment variable support** — LiteLLM reads standard provider env vars like `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, etc.

LiteLLM is the sole backend engine. There is no separate in-house provider abstraction layer.

## Local model registry

Model aliases and optional credential configuration live in **local user config**, independent of stacks. This is managed via the `kitaru model register` CLI command.

The registry conceptually stores:

```json
{
  "aliases": {
    "fast": { "model": "openai/gpt-4o-mini" },
    "smart": { "model": "anthropic/claude-sonnet-4-20250514" }
  },
  "default": "fast"
}
```

The exact on-disk schema and storage path (e.g. `~/.config/kitaru/models.json`) are not frozen yet.

**Zero-config path:** Users who already have provider environment variables set (e.g. `OPENAI_API_KEY`) can use `kitaru.llm()` without registering anything — LiteLLM reads those env vars natively. The registry adds convenience (aliases, defaults) but is not required.

**Stacks do not own model configuration.** Model aliases and credentials are user-local config, not part of a stack's component list. This keeps stacks focused on execution infrastructure (runner, artifact store, container registry).

## Model resolution

`model=` may be:

- a **locally registered alias** such as `"fast"` or `"smart"` (resolved through the local model registry)
- a concrete LiteLLM model identifier such as `"openai/gpt-4o"` or `"anthropic/claude-sonnet-4-20250514"`

Resolution logic:

1. If `model=` is provided:
   - If it matches a local alias, resolve alias to concrete LiteLLM model string
   - Otherwise treat it as a concrete LiteLLM identifier
2. If `model=` is omitted:
   - Use the locally configured default alias/model if one exists
   - Otherwise fail with a clear configuration error

Credential resolution is separate:

- Provider env vars in the execution environment (read natively by LiteLLM)
- Optional local credential/config entries from the model registry
- Future: a ZenML-backed credential source behind the same resolver

**Provenance:** The **resolved concrete model** used for each call must always be recorded as metadata (alongside the alias, if one was used). This ensures replay provenance is clear and dashboards remain auditable, even as aliases change over time.

**Remote execution note:** Model aliases and credentials are user-local. When executing on a remote stack, the remote environment needs the relevant provider credentials (typically via environment variables in the execution environment). The local registry does not automatically propagate secrets to remote runners.

## Future migration path

A future ZenML-backed `llm_model` stack component may later become an additional credential-resolution backend. This would change where credentials are resolved from, but would not change the `kitaru.llm()` API. The migration is an implementation swap behind the same interface.

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
- model resolution works through the local model registry or concrete LiteLLM identifiers
- usage metadata should be recorded automatically via `kitaru.log()`
- prompt and response should become typed artifacts
- the resolved concrete model must always be recorded as metadata for provenance

## MVP notes

- keep the API narrow and single-call
- do not overload `kitaru.llm()` with tool loops or conversation state
- nested-boundary semantics should stay simple: standalone in flow, child event in checkpoint
- LiteLLM is the sole backend engine — no in-house provider abstraction
- model aliases and credentials are user-local config, not stack-owned
- usage/cost metadata is recorded when available from LiteLLM/provider responses
