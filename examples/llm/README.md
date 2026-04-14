# Tracked LLM examples

These examples show how `kitaru.llm()` behaves inside flows: request/response
artifacts are captured, usage metadata is attached automatically, and the call
returns a rich `LLMResponse` with `.content`, `.tool_calls`, `.usage`, and
`.finish_reason`.

## Getting started

```bash
cd examples/llm
uv pip install 'kitaru[local]'   # Install Kitaru with local runtime
kitaru init                      # Initialize a Kitaru project in this directory
```

For real provider calls, register a model alias and provide credentials:

```bash
kitaru secrets set openai-creds --OPENAI_API_KEY=sk-...
kitaru model register fast --model openai/gpt-5-nano --secret openai-creds
```

Then run the text example:

```bash
python flow_with_llm.py
```

The manual tool-loop example is mock-safe and does not require provider
credentials:

```bash
python manual_tool_loop.py
```

For the full credential setup walkthrough, see
[Tracked LLM Calls](https://kitaru.ai/docs/guides/llm-calls).

## `flow_with_llm.py` — Tracked model calls with rich responses

Makes two `kitaru.llm()` calls: one at flow scope (outline generation) and
one inside a checkpoint (draft expansion). The flow-scope call is passed to the
checkpoint as a durable output handle; the checkpoint receives a concrete
`LLMResponse` and uses `.content` to build the next prompt.

Each call captures the request envelope, normalized response payload, and usage
metadata as structured artifacts. The model alias (`"fast"`) resolves
credentials from the secret you registered above.

## `manual_tool_loop.py` — Manual two-turn tool calling

Demonstrates the Release 1 tool-calling pattern:

1. call `kitaru.llm(..., tools=[...])`
2. inspect `response.tool_calls`
3. execute the local tool in Python
4. append a `tool` message and call `kitaru.llm()` again

Kitaru returns tool-call intents but does not execute tools automatically. The
example uses `KITARU_LLM_MOCK_RESPONSE_JSON` internally so it can run in tests
and demos without an API key.

For the full catalog, see [../README.md](../README.md).
