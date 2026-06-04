---
description: "Resolve pending wait input for a non-interactive or timed-out execution."
---

# kitaru executions input

## Usage

```bash
kitaru executions input [EXEC-ID] [OPTIONS]
```

## Parameters

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `EXEC-ID` | `str` | No | `None` | Execution ID. Required in non-interactive mode. Omit with --interactive to sweep all waiting executions. |
| `--interactive`, `-i` | `bool` | No | `False` | Interactively review and resolve pending waits. |
| `--abort` | `bool` | No | `False` | Abort the pending wait instead of continuing. |
| `--value` | `str` | No | `None` | Resolved wait input as JSON (for example `true` or `{"approved": false}`). Required in non-interactive continue mode. |
| `--output`, `-o` | `str` | No | `"text"` | Output format: "text" (default) or "json". |

