---
description: "Delete one deployment version when no exclusive tag protects it."
---

# kitaru flow deployments delete

## Usage

```bash
kitaru flow deployments delete FLOW [OPTIONS]
```

## Parameters

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `FLOW` | `str` | Yes |  | Deployment-backed flow name. |
| `--version` | `int` | Yes |  | Exact deployment version to delete. |
| `--output`, `-o` | `str` | No | `"text"` | Output format: "text" (default) or "json". |

