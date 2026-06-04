---
description: "List all secrets visible to the current user context."
---

# kitaru secrets list

## Usage

```bash
kitaru secrets list [OPTIONS]
```

## Parameters

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `--page` | `int` | No | `1` | 1-based page number to return. |
| `--size` | `int` | No | `20` | Number of items to return per page. |
| `--output`, `-o` | `str` | No | `"text"` | Output format: "text" (default) or "json". |

