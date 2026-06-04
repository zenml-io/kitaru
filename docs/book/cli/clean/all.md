---
description: "Reset all local state: project + global."
---

# kitaru clean all

## Usage

```bash
kitaru clean all [OPTIONS]
```

## Parameters

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `--yes`, `-y` | `bool` | No | `False` | Skip confirmation prompt. |
| `--dry-run` | `bool` | No | `False` | Show what would be deleted without deleting. |
| `--force` | `bool` | No | `False` | Required when cleanup would destroy model registry aliases. |
| `--output`, `-o` | `str` | No | `"text"` | Output format: "text" (default) or "json". |

