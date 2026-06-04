---
description: "Generate a curl command that starts a deployment execution."
---

# kitaru flow deployments curl

## Usage

```bash
kitaru flow deployments curl FLOW [OPTIONS]
```

## Parameters

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `FLOW` | `str` | Yes |  | Deployment-backed flow name. |
| `--version` | `int` | No | `None` | Exact deployment version. |
| `--tag` | `str` | No | `None` | Deployment tag selector. |
| `--input` | `str` | No | `None` | Invocation inputs as JSON or `@file`. |
| `--output`, `-o` | `str` | No | `"text"` | Output format: "text" (default) or "json". |

