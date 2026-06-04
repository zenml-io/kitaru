---
description: "List service accounts."
---

# kitaru auth service-accounts list

## Usage

```bash
kitaru auth service-accounts list [OPTIONS]
```

## Parameters

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `--active` | `bool` | No | `None` | Filter by active state. |
| `--name` | `str` | No | `None` | Filter by exact name. |
| `--page` | `int` | No | `1` | 1-based page number to return. |
| `--size` | `int` | No | `20` | Number of items to return per page. |
| `--output`, `-o` | `str` | No | `"text"` | Output format: "text" (default) or "json". |

