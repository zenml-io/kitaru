---
description: "Fetch runtime log entries for an execution."
---

# kitaru executions logs

## Usage

```bash
kitaru executions logs EXEC-ID [OPTIONS]
```

## Parameters

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `EXEC-ID` | `str` | Yes |  | Execution ID. |
| `--checkpoint` | `str` | No | `None` | Optional checkpoint function name to filter by. |
| `--source` | `str` | No | `"step"` | Log source (default: "step"; use "runner" for run-level logs). |
| `--limit` | `int` | No | `None` | Maximum total log entries to return. |
| `--follow` | `bool` | No | `False` | Stream new log entries until execution reaches a terminal state. |
| `--interval` | `float` | No | `3.0` | Polling interval in seconds for `--follow`. |
| `--grouped` | `bool` | No | `False` | Group output by checkpoint with section headers. |
| `--output`, `-o` | `str` | No | `"text"` | Output format: "text" (default) or "json". |
| `--verbosity`, `-v` | `int` | No | `0` | Increase verbosity (`-v` for level+timestamp, `-vv` for module). |

