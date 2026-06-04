---
description: "List executions with optional filters."
---

# kitaru executions list

## Usage

```bash
kitaru executions list [OPTIONS]
```

## Parameters

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `--status` | `str` | No | `None` | Optional status filter (running/waiting/completed/failed/cancelled). |
| `--flow` | `str` | No | `None` | Optional flow-name filter. |
| `--limit` | `int` | No | `None` | Legacy shortcut for the first page size. Cannot be combined with `--page` or `--size`. |
| `--page` | `int` | No | `None` | 1-based page number to return. |
| `--size` | `int` | No | `None` | Number of items to return per page. |
| `--output`, `-o` | `str` | No | `"text"` | Output format: "text" (default) or "json". |

