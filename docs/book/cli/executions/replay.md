---
description: "Replay an execution from a checkpoint boundary."
---

# kitaru executions replay

## Usage

```bash
kitaru executions replay EXEC-ID [OPTIONS]
```

## Parameters

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `EXEC-ID` | `str` | Yes |  | Execution ID. |
| `--from` | `str` | Yes |  | Checkpoint selector (name, invocation ID, or call ID). |
| `--args` | `str` | No | `None` | Flow input overrides as a JSON object (for example '{"topic": "New topic"}'). |
| `--overrides` | `str` | No | `None` | Replay overrides as a JSON object with `checkpoint.*` keys. |
| `--output`, `-o` | `str` | No | `"text"` | Output format: "text" (default) or "json". |

