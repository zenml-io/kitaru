---
description: "Show a secret with metadata and optional raw values."
---

# kitaru secrets show

## Usage

```bash
kitaru secrets show NAME-OR-ID [OPTIONS]
```

## Parameters

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `NAME-OR-ID` | `str` | Yes |  | Secret name or ID. |
| `--show-values` | `bool` | No | `False` | Display raw secret values in command output. |
| `--output`, `-o` | `str` | No | `"text"` | Output format: "text" (default) or "json". |

