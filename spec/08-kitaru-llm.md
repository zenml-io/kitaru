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

Model aliases live in **local user config**, independent of stacks. This is managed via the `kitaru model register` CLI command.

The registry conceptually stores:

```json
{
  "aliases": {
    "fast": { "model": "openai/gpt-4o-mini", "secret": "openai-creds" },
    "smart": { "model": "anthropic/claude-sonnet-4-20250514", "secret": "anthropic-creds" }
  },
  "default": "fast"
}
```

The `secret` field is optional — it references a ZenML secret by name that holds provider credentials. When present, `kitaru.llm()` fetches the secret at runtime to obtain API keys and other credentials.

The exact on-disk schema and storage path (e.g. `~/.config/kitaru/models.json`) are not frozen yet.

**Zero-config path:** Users who already have provider environment variables set (e.g. `OPENAI_API_KEY`) can use `kitaru.llm()` without registering anything — LiteLLM reads those env vars natively. The registry adds convenience (aliases, defaults, remote credential references) but is not required.

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

## Credential resolution

Credential resolution is separate from model resolution and follows this order:

1. **Process env vars** — if the required provider env vars (e.g. `OPENAI_API_KEY`) already exist in the execution environment, use them. This works locally (user has env vars set) and remotely (env vars injected by ZenML or container config).
2. **ZenML secret** — if the resolved alias has a `secret` field, fetch the referenced ZenML secret via `Client().get_secret(...)` and temporarily inject its key-value pairs as environment variables for the LiteLLM call. This is the primary path for remote execution.
3. **No credentials found** — fail with a clear error explaining which provider credentials are needed and how to configure them.

**Secret key naming convention:** ZenML secrets used for LLM credentials should store keys using the actual environment variable names LiteLLM expects (e.g. `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`). This ensures seamless compatibility — `kitaru.llm()` fetches the secret and sets env vars that LiteLLM reads natively.

**How remote execution works:** Remote step containers receive a ZenML workload API token. This token authenticates requests to the ZenML server. When `kitaru.llm()` needs credentials, it uses this token to fetch the referenced secret from the server — the actual API keys never need to be in the container spec or pod environment.

**Provenance:** The **resolved concrete model** used for each call must always be recorded as metadata (alongside the alias, if one was used). This ensures replay provenance is clear and dashboards remain auditable, even as aliases change over time.

## Future work

- A future ZenML-backed `llm_model` stack component may later become an additional credential-resolution backend. This would not change the `kitaru.llm()` API.
- Richer model registry UX: `kitaru model show`, `kitaru model remove`, `kitaru model test`
- Import/export or team-sharing of alias configurations

## Examples

**Simple call inside a flow:**

```python
from kitaru import flow

@flow
def my_agent(topic: str) -> str:
    outline = kitaru.llm(f"Create an outline for: {topic}", model="fast")
    draft = kitaru.llm(f"Write an article from this outline:\n{outline}", model="smart")
    return draft
```

**Call inside a checkpoint:**

```python
from kitaru import checkpoint

@checkpoint(type="llm_call")
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

## When to use `kitaru.llm()` vs `@checkpoint`

Use `kitaru.llm()` when you want:

- a simple single model call
- automatic tracking
- minimal ceremony

Use `@checkpoint` when you want:

- a true replay boundary
- tool use
- multiple model calls grouped together
- custom control flow
- framework-based agent behavior

In practice:

- `kitaru.llm()` is good for small one-shot calls
- `@checkpoint` is the right durable unit for larger work

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
- model aliases are user-local config, not stack-owned
- credentials for remote execution use ZenML secrets referenced from aliases
- usage/cost metadata is recorded when available from LiteLLM/provider responses
