---
description: "List deployment versions for one flow."
---

# kitaru flow deployments list

## Usage

```bash
kitaru flow deployments list FLOW [OPTIONS]
```

## Parameters

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `FLOW` | `str` | Yes |  | Deployment-backed flow name. |
| `--page` | `int` | No | `1` | 1-based page number to return. |
| `--size` | `int` | No | `20` | Number of items to return per page. |
| `--output`, `-o` | `str` | No | `"text"` | Output format: "text" (default) or "json". |

