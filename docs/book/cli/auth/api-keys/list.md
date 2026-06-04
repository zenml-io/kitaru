---
description: "List API-key metadata for a service account."
---

# kitaru auth api-keys list

## Usage

```bash
kitaru auth api-keys list SERVICE-ACCOUNT [OPTIONS]
```

## Parameters

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `SERVICE-ACCOUNT` | `str` | Yes |  | Owning service-account name or ID. |
| `--active` | `bool` | No | `None` | Filter by active state. |
| `--name` | `str` | No | `None` | Filter by exact name. |
| `--page` | `int` | No | `1` | 1-based page number to return. |
| `--size` | `int` | No | `20` | Number of items to return per page. |
| `--output`, `-o` | `str` | No | `"text"` | Output format: "text" (default) or "json". |

