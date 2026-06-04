---
description: "LLM call primitive for tracked model interactions."
---


# `kitaru.llm`

LLM call primitive for tracked model interactions.

`kitaru.llm()` wraps one provider SDK completion call with Kitaru tracking.
Built-in runtime support covers ``openai/*``, ``anthropic/*``, ``ollama/*``,
and ``openrouter/*`` models. Ollama and OpenRouter use the OpenAI-compatible
API and require the ``openai`` package (``pip install kitaru[openai]``).

## `llm`

```python
llm(prompt, *, model=None, system=None, temperature=None, max_tokens=None, name=None) -> str
```

Make a tracked LLM call.

**Parameters**

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| `prompt` | `str | list[dict[str, Any]]` |  | User prompt text or a chat-style message list. |
| `model` | `str | None` | `None` | Model alias or provider/model identifier (e.g. ``openai/gpt-5-nano``). |
| `system` | `str | None` | `None` | Optional system prompt. |
| `temperature` | `float | None` | `None` | Optional sampling temperature. |
| `max_tokens` | `int | None` | `None` | Optional maximum response tokens. |
| `name` | `str | None` | `None` | Optional display name for this call. |

**Returns:** The model response text.
