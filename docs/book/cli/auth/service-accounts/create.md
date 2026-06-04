---
description: "Create a service account."
---

# kitaru auth service-accounts create

## Usage

```bash
kitaru auth service-accounts create NAME [OPTIONS]
```

## Parameters

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `NAME` | `str` | Yes |  | Service-account name. |
| `--full-name` | `str` | No | `None` | Optional human-readable full name. |
| `--description` | `str` | No | `""` | Optional service-account description. |
| `--output`, `-o` | `str` | No | `"text"` | Output format: "text" (default) or "json". |

