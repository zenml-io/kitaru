---
description: "Set the global runtime log-store backend override."
---

# kitaru log-store set

## Usage

```bash
kitaru log-store set BACKEND [OPTIONS]
```

## Parameters

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `BACKEND` | `str` | Yes |  | External runtime log backend name (for example datadog). |
| `--endpoint` | `str` | Yes |  | HTTP(S) endpoint for the configured log backend. |
| `--api-key` | `str` | No | `None` | Optional API key or secret placeholder. |
| `--output`, `-o` | `str` | No | `"text"` | Output format: "text" (default) or "json". |

