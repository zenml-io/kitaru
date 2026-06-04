---
description: "Remove a public tag from one deployment version."
---

# kitaru flow untag

## Usage

```bash
kitaru flow untag FLOW TAG [OPTIONS]
```

## Parameters

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `FLOW` | `str` | Yes |  | Deployment-backed flow name. |
| `TAG` | `str` | Yes |  | Public deployment tag to remove. |
| `--version` | `int` | Yes |  | Exact deployment version to untag. |
| `--output`, `-o` | `str` | No | `"text"` | Output format: "text" (default) or "json". |

