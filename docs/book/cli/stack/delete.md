---
description: "Delete a stack by name or ID."
---

# kitaru stack delete

## Usage

```bash
kitaru stack delete STACK [OPTIONS]
```

## Parameters

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `STACK` | `str` | Yes |  | Stack name or ID to delete. |
| `--recursive` | `bool` | No | `False` | Delete the stack and any unshared managed components. |
| `--force` | `bool` | No | `False` | Allow deleting the active stack by falling back to the default stack. |
| `--output`, `-o` | `str` | No | `"text"` | Output format: "text" (default) or "json". |

