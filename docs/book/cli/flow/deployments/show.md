---
description: "Show one deployment version."
---

# kitaru flow deployments show

## Usage

```bash
kitaru flow deployments show FLOW [OPTIONS]
```

## Parameters

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `FLOW` | `str` | Yes |  | Deployment-backed flow name. |
| `--version` | `int` | No | `None` | Exact deployment version. |
| `--tag` | `str` | No | `None` | Deployment tag selector. |
| `--output`, `-o` | `str` | No | `"text"` | Output format: "text" (default) or "json". |

