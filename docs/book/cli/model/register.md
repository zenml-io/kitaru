---
description: "Register or update a model alias used by `kitaru.llm()`."
---

# kitaru model register

Aliases are stored locally and automatically transported to submitted and replayed executions.

## Usage

```bash
kitaru model register ALIAS [OPTIONS]
```

## Parameters

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `ALIAS` | `str` | Yes |  | Model alias name (for example `fast`). |
| `--model` | `str` | Yes |  | Provider/model identifier (e.g. openai/gpt-5-nano, ollama/qwen3.5). |
| `--secret` | `str` | No | `None` | Optional secret name/ID containing provider credentials. |
| `--output`, `-o` | `str` | No | `"text"` | Output format: "text" (default) or "json". |

