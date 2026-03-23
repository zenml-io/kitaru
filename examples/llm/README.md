# Tracked LLM example

This example shows how `kitaru.llm()` behaves inside a flow: prompt/response
artifacts are captured, and usage metadata (tokens, cost, latency) is attached
automatically.

## Getting started

```bash
cd examples/llm
uv pip install 'kitaru[local]'   # Install Kitaru with local runtime
kitaru init                      # Initialize a Kitaru project in this directory
```

Register a model alias and provide credentials:

```bash
kitaru secrets set openai-creds --OPENAI_API_KEY=sk-...
kitaru model register fast --model openai/gpt-4o-mini --secret openai-creds
```

Then run:

```bash
python flow_with_llm.py
```

For the full credential setup walkthrough, see
[Tracked LLM Calls](https://kitaru.ai/docs/getting-started/llm-calls).

## `flow_with_llm.py` — Tracked model calls with automatic cost tracking

Makes two `kitaru.llm()` calls: one at flow scope (outline generation) and
one inside a checkpoint (draft expansion). Each call automatically captures
the prompt, the response, and usage metadata (model, tokens, cost, latency)
as structured artifacts — no manual logging needed. The model alias
(`"fast"`) resolves credentials from the secret you registered above.

For the full catalog, see [../README.md](../README.md).
