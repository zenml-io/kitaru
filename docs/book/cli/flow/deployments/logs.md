---
description: "Fetch runtime logs for an execution associated with a deployment."
---

# kitaru flow deployments logs

## Usage

```bash
kitaru flow deployments logs FLOW [OPTIONS]
```

## Parameters

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `FLOW` | `str` | Yes |  | Deployment-backed flow name. |
| `--version` | `int` | No | `None` | Exact deployment version. |
| `--tag` | `str` | No | `None` | Deployment tag selector. |
| `--exec-id` | `str` | No | `None` | Explicit execution ID to read logs from. |
| `--checkpoint` | `str` | No | `None` | Optional checkpoint function name to filter by. |
| `--source` | `str` | No | `"step"` | Log source (default: "step"). |
| `--limit` | `int` | No | `None` | Maximum total log entries to return. |
| `--follow` | `bool` | No | `False` | Stream until terminal status. |
| `--interval` | `float` | No | `3.0` | Polling interval in seconds. |
| `--grouped` | `bool` | No | `False` | Group output by checkpoint. |
| `--output`, `-o` | `str` | No | `"text"` | Output format: "text" (default) or "json". |
| `--verbosity`, `-v` | `int` | No | `0` | Increase log verbosity. |

