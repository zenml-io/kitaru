---
description: Use kitaru.llm() with model aliases, transported runtime config, and optional secret-backed credentials
icon: robot
---

# Tracked LLM Calls

`kitaru.llm()` makes a single model call and records it as a durable boundary. That capture is the enabler for replay — once a call is recorded, you can reproduce the run faithfully and replay it with one input changed (a different model, a different prompt) to diff the effect of your change. Each call captures automatically:

- prompt artifact capture
- response artifact capture
- usage/latency metadata logging
- automatic estimated-cost metadata for direct OpenAI and Anthropic calls

{% hint style="info" %}
If you want the full setup path from stored credentials to an actual flow run,
start with
[Secrets + Model Registration](secrets-and-model-registration.md).
{% endhint %}

## Model selection order

When you call `kitaru.llm()`, Kitaru resolves the model in this order:

1. the explicit `model=` argument
2. `KITARU_DEFAULT_MODEL`
3. the default alias from the effective model registry in the current environment

If `KITARU_DEFAULT_MODEL` matches a registered alias, Kitaru resolves that
alias. Otherwise it treats the value as a raw provider/model string.

When you submit or replay a flow, Kitaru automatically transports your local
model registry into the execution environment. Remote runs resolve aliases with
`kitaru.llm()` and `kitaru model list` just like local ones. If
`KITARU_MODEL_REGISTRY` is already set in the runtime environment, its aliases
and default alias take precedence over matching local entries.

This makes model selection replay-friendly. If your flow takes the model as an
input and passes it to `kitaru.llm(..., model=model)`, you can swap it on replay
with `flow.replay(exec_id, from_="...", model="other-alias")` and diff the result
against the faithful baseline — the change is the only difference. See
[Replay and overrides](replay-and-overrides.md) for the full loop.

## Register a model alias

```bash
kitaru model register fast --model openai/gpt-5-nano --secret openai-creds
```

You can also register an alias without a linked secret:

```bash
kitaru model register fast --model openai/gpt-5-nano
```

List aliases with:

```bash
kitaru model list
```

{% hint style="info" %}
`kitaru model register` writes aliases to local Kitaru config, but submitted
and replayed runs automatically receive that registry as a transported runtime
snapshot. `KITARU_MODEL_REGISTRY` is available as an advanced manual override
for adding aliases or overriding matching ones.
{% endhint %}

## Supported providers

Built-in runtime support covers:

- `openai/*` — OpenAI models (requires `kitaru[openai]`)
- `anthropic/*` — Anthropic models (requires `kitaru[anthropic]`)
- `ollama/*` — local Ollama models (requires `kitaru[openai]`, no API key needed)
- `openrouter/*` — OpenRouter meta-router (requires `kitaru[openai]`)

Ollama and OpenRouter use the OpenAI-compatible API, so they share the
`kitaru[openai]` extra — no additional packages needed.

## Usage and estimated costs

Each successful direct `kitaru.llm()` call records one `llm_usage_v1` metadata
record with token counts, latency, model information, and cost fields. For
OpenAI and Anthropic models, estimated cost tracking is on by default and uses
[`genai-prices`](https://github.com/pydantic/genai-prices).

Kitaru stores that value as `estimated_cost_usd`, not `actual_cost_usd`. If
pricing fails or the model cannot be priced, the LLM call still succeeds and the
usage record keeps the tokens plus a warning. The same setting also controls built-in `genai-prices` estimates for framework adapters. Disable automatic estimates with:

```bash
export KITARU_LLM_ESTIMATED_COSTS=off
```

or for the current Python process:

```python
kitaru.configure(llm_estimated_costs="off")
```

See [Execution Management → LLM usage and cost metadata](execution-management.md#llm-usage-and-cost-metadata)
for how checkpoint-level records roll up into execution summaries and
statistics.

## Credential resolution order

For built-in providers that require credentials (OpenAI, Anthropic,
OpenRouter), Kitaru resolves credentials in this order:

1. provider credentials already present in the environment
2. the secret linked to the resolved alias
3. otherwise, fail with a setup error

That means environment variables win over a linked secret for known providers.

Ollama does not require credentials (local server). Use `OLLAMA_HOST` to
point to a non-default server address (default: `http://localhost:11434`).

### Environment-backed setup

```bash
export OPENAI_API_KEY=sk-...
```

### Secret-backed setup

Store provider keys in a Kitaru secret:

```bash
kitaru secrets set openai-creds --OPENAI_API_KEY=sk-...
```

When an alias includes `--secret openai-creds`, `kitaru.llm()` loads that
secret at runtime if the required environment variable is not already set.

## Call `kitaru.llm()` inside a flow

```python
from kitaru import flow
import kitaru

@flow
def writer(topic: str) -> str:
    outline = kitaru.llm(
        f"Create a 3-bullet outline about {topic}.",
        model="fast",
        name="outline_call",
    )
    outline_text = outline.load()
    return kitaru.llm(
        f"Write a short paragraph using this outline:\n{outline_text}",
        model="fast",
        name="draft_call",
    )
```

Flow-body `kitaru.llm()` calls are durable call boundaries. Use `.load()` when
you need the text in flow-body Python, such as composing the next prompt. If you
pass a checkpoint or LLM output into a downstream checkpoint, keep passing the
original output handle. See [In flow bodies](artifacts.md#in-flow-bodies)
for the general pattern.

## Advanced options

`kitaru.llm()` also accepts `system=`, `temperature=`, and `max_tokens=`:

```python
reply = kitaru.llm(
    "Summarize this document in 3 bullets.",
    model="fast",
    system="You are a concise technical editor.",
    temperature=0.2,
    max_tokens=200,
    name="summary_call",
)
```

`max_tokens` is Kitaru's provider-neutral output limit. For newer OpenAI
reasoning and GPT-5-style models, Kitaru sends that value to OpenAI as
`max_completion_tokens`. For OpenAI reasoning models, that limit can include
internal reasoning tokens, so the visible response can be shorter than the
requested limit. For older OpenAI models, OpenRouter, and Ollama, Kitaru keeps
using the OpenAI-compatible `max_tokens` request field.

### Chat-style message lists

Instead of a plain string, you can pass a chat-style message list:

```python
reply = kitaru.llm(
    [
        {"role": "user", "content": "Draft a release note headline."},
        {"role": "assistant", "content": "Kitaru adds durable replay controls."},
        {"role": "user", "content": "Now make it shorter."},
    ],
    model="fast",
    name="headline_refine",
)
```

Each message must include `role` and `content` keys. If `system=` is provided
alongside a message list, Kitaru prepends a system message automatically.

## When to use `kitaru.llm()` vs your own client

`kitaru.llm()` is designed for simple text-in/text-out model calls. It handles
credential resolution, prompt/response capture, and usage tracking automatically.
Built-in runtime support covers `openai/*`, `anthropic/*`, `ollama/*`, and
`openrouter/*` models.

{% hint style="info" %}
`kitaru.llm()` requires a provider SDK to be installed. Install with
`pip install kitaru[openai]` (also covers Ollama and OpenRouter),
`pip install kitaru[anthropic]`, or `pip install kitaru[llm]` for both.
{% endhint %}

For advanced patterns — tool calling, structured outputs, streaming, vision
inputs, or multi-turn conversation management — use your provider SDK directly
inside a `@checkpoint`. You still get durable checkpointing and replay; you
just manage the model interaction yourself:

```python
from openai import OpenAI
from kitaru import checkpoint

@checkpoint
def agent_step(messages: list[dict]) -> str:
    client = OpenAI()
    resp = client.chat.completions.create(
        model="gpt-5-nano",
        messages=messages,
        tools=[...],  # tool calling, structured output, etc.
    )
    return resp.choices[0].message.content
```

For a full example of a tool-calling agent built this way, see
[`examples/end_to_end/coding_agent/`](https://github.com/zenml-io/kitaru/tree/develop/examples/end_to_end/coding_agent).

{% hint style="info" %}
Tool calling and structured output support for `kitaru.llm()` is on the
roadmap. For now, use your provider SDK directly inside checkpoints for
these patterns.
{% endhint %}

## Runtime behavior by context

- **Inside a flow (outside checkpoints):** `kitaru.llm()` runs as a synthetic durable call boundary.
- **Inside a checkpoint:** it is tracked as a child event; the enclosing checkpoint remains the replay boundary.

## What Kitaru records

Each call records prompt artifacts, response artifacts, token usage, latency,
and credential source metadata (`environment` or `secret`). This is the durable
record that replay reconstructs from.

## Example in this repository

```bash
uv sync --extra local --extra llm

# Register an alias (with or without a linked secret) before running the example.
uv run kitaru model register fast --model openai/gpt-5-nano
uv run python examples/features/llm/flow_with_llm.py
uv run pytest tests/test_phase12_llm_example.py
```

If you want the full credential-backed setup path first, start with
[Secrets + Model Registration](secrets-and-model-registration.md).

For the broader catalog, see [Examples](../getting-started/examples.md).

## Related pages

- [Examples](../getting-started/examples.md)
- [Secrets + Model Registration](secrets-and-model-registration.md)
- [Manage Secrets](secrets.md)
- [Configuration](configuration.md)
- [CLI model commands](https://sdkdocs.kitaru.ai)
